import os
import smtplib
import time
import random
import logging
import logging.handlers
import json
import requests
from datetime import datetime, timedelta, timezone
import google.generativeai as genai
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart

# --- SHARED CONFIGURATION & SETUP ---

def setup_logging(log_filename: str):
    """Sets up a timed rotating logger."""
    log_handler = logging.handlers.TimedRotatingFileHandler(
        log_filename,
        when='midnight',
        interval=1,
        backupCount=400
    )
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[log_handler, logging.StreamHandler()]
    )
    logging.info(f"Logger configured for {log_filename}")

def configure_gemini():
    """Configures the Gemini API."""
    try:
        genai.configure(api_key=os.getenv('GEMINI_API_KEY'))
        logging.info("Gemini API configured successfully.")
    except Exception as e:
        logging.critical(f"Could not configure Gemini API. Is GEMINI_API_KEY set? Error: {e}", exc_info=True)
        exit()

# --- FINANCIAL DATA FUNCTION ---
def get_financial_data(ticker: str) -> dict:
    """Fetches key financial data for a given ticker from Alpha Vantage."""
    api_key = os.getenv('ALPHA_VANTAGE_API_KEY')
    if not api_key:
        logging.warning("ALPHA_VANTAGE_API_KEY not found. Skipping financial data.")
        return {}

    logging.info(f"Fetching financial data for {ticker}...")
    try:
        overview_url = f'https://www.alphavantage.co/query?function=OVERVIEW&symbol={ticker}&apikey={api_key}'
        r_overview = requests.get(overview_url)
        r_overview.raise_for_status()
        overview_data = r_overview.json()

        ts_url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={ticker}&apikey={api_key}'
        r_ts = requests.get(ts_url)
        r_ts.raise_for_status()
        ts_data = r_ts.json()

        latest_close = ts_data.get("Time Series (Daily)", {})
        price = "N/A"
        if latest_close:
            latest_date = sorted(latest_close.keys(), reverse=True)[0]
            price = latest_close[latest_date]['4. close']

        market_cap_str = overview_data.get('MarketCapitalization', "0")
        market_cap_bil = int(market_cap_str) / 1_000_000_000 if market_cap_str.isdigit() else 0

        data = {
            "Ticker": overview_data.get("Symbol", ticker),
            "Price": price,
            "MarketCap": f"${market_cap_bil:.2f}B",
            "PERatio": overview_data.get("PERatio", "N/A"),
            "52WeekHigh": overview_data.get("52WeekHigh", "N/A"),
            "52WeekLow": overview_data.get("52WeekLow", "N/A"),
        }
        logging.info(f"...Successfully fetched data for {ticker}.")
        return data
    except Exception as e:
        logging.error(f"Failed to fetch financial data for {ticker}: {e}", exc_info=True)
        return {}

# --- AI ANALYSIS FUNCTIONS ---

def analyze_content(full_article_text: str, max_retries=3) -> dict:
    """Analyzes an article for sentiment and key entities."""
    logging.info("Analyzing article for sentiment and entities...")
    model = genai.GenerativeModel('gemini-1.5-flash')
    prompt = f"""
    Analyze the following news article. Your task is to provide a structured JSON output containing two keys: "sentiment" and "entities".
    1.  **sentiment**: Classify the overall tone of the article as "Positive", "Negative", or "Neutral".
    2.  **entities**: Extract the top 3-5 most important named entities (companies, people, key topics).
    Provide your response ONLY as a valid JSON object.
    Example: {{"sentiment": "Negative", "entities": {{"companies": ["Company A"], "people": ["John Doe"], "topics": ["Inflation"]}}}}
    Here is the article text:
    ---
    {full_article_text}
    """
    attempt = 0
    while attempt < max_retries:
        try:
            response = model.generate_content(prompt)
            cleaned_text = response.text.strip().replace("```json", "").replace("```", "")
            return json.loads(cleaned_text)
        except Exception as e:
            attempt += 1
            if attempt >= max_retries:
                logging.error(f"Failed to analyze content after {max_retries} attempts: {e}", exc_info=True)
                return {"sentiment": "Unknown", "entities": {}}
            time.sleep((2 ** attempt) + random.uniform(0, 1))
    return {"sentiment": "Unknown", "entities": {}}

def generate_hedge_fund_takeaways(summarized_articles: list, time_frame: str, max_retries=3) -> str | None:
    """Generates hedge fund takeaways with a dynamic prompt."""
    logging.info(f"Generating {time_frame} actionable investor takeaways...")
    model = genai.GenerativeModel('gemini-1.5-flash')

    prompt_context = "You have been given a compilation of today's key industry news summaries."
    takeaway_title_example = "**1. Thesis Title**"
    forward_look = "that could materially impact investment decisions"
    if time_frame.lower() in ('weekly', 'custom'):
        prompt_context = "You have been given a compilation of all key industry news summaries from the specified period. Your task is to synthesize all this information and identify the most critical, actionable takeaways for your portfolio manager."
        takeaway_title_example = "**1. Thematic Shift**"
        forward_look = "in the coming weeks"

    full_context = "\n\n---\n\n".join(
        [f"Source: {a['source']}\nTitle: {a['title']}\nSummary:\n{a['summary']}" for a in summarized_articles]
    )
    prompt = f"""
    You are a senior analyst at a US-focused long-short hedge fund. Your goal is to generate alpha and achieve returns that consistently beat the S&P 500.
    {prompt_context}
    Analyze the provided news summaries below to identify overarching themes, cross-sector trends, risks, and opportunities. Distill your analysis into your top 3-5 actionable takeaways.
    For each takeaway:
    - Give it a clear, bolded title (e.g., {takeaway_title_example}).
    - Provide a 3-5 sentence analysis explaining the "so what" for an investor.
    - Mention specific sectors or companies from the articles that exemplify this trend.
    - Be concise, forward-looking, and focus on what could materially impact investment decisions {forward_look}.
    Here is the full context of the news summaries:
    ---
    {full_context}
    """
    attempt = 0
    while attempt < max_retries:
        try:
            response = model.generate_content(prompt)
            logging.info(f"...Successfully generated {time_frame} takeaways.")
            return response.text
        except Exception as e:
            attempt += 1
            if attempt >= max_retries:
                logging.error(f"Gemini API Error during takeaway generation after {max_retries} attempts: {e}", exc_info=True)
                return None
            wait_time = (2 ** attempt) + random.uniform(0, 1)
            logging.warning(f"Gemini API Error during takeaway generation. Retrying in {wait_time:.2f} seconds...")
            time.sleep(wait_time)
    return None

def get_company_selections(articles: list, max_retries=3) -> list | None:
    """Identifies companies for deep-dive analysis from news summaries."""
    logging.info("Identifying company selections for deep-dive...")
    model = genai.GenerativeModel('gemini-1.5-flash')
    full_context = "\n\n---\n\n".join([f"Title: {a['title']}\nSummary:\n{a['summary']}" for a in articles])
    prompt = f"""
    Analyze the provided news summaries to identify U.S. publicly traded companies. Your task is to provide a structured JSON output containing a single key: "companies".
    The value should be a list of JSON objects, where each object has two keys: "ticker" and "name".
    - "ticker": The stock ticker symbol (e.g., "AAPL").
    - "name": The full company name (e.g., "Apple Inc.").
    Extract the 5-7 most prominently featured companies from the text.
    **CRITICAL RULE**: Provide your response ONLY as a valid JSON object. Do not include any text before or after the JSON.
    Example format:
    {{
      "companies": [
        {{"ticker": "NVDA", "name": "NVIDIA Corporation"}},
        {{"ticker": "LLY", "name": "Eli Lilly and Company"}}
      ]
    }}
    Here is the context from the news articles:
    ---
    {full_context}
    """
    attempt = 0
    while attempt < max_retries:
        try:
            response = model.generate_content(prompt)
            cleaned_text = response.text.strip().replace("```json", "").replace("```", "")
            data = json.loads(cleaned_text)
            logging.info(f"...Successfully identified {len(data.get('companies', []))} companies.")
            return data.get('companies')
        except Exception as e:
            attempt += 1
            if attempt >= max_retries:
                logging.error(f"Failed to identify companies after {max_retries} attempts: {e}", exc_info=True)
                return None
            time.sleep((2 ** attempt) + random.uniform(0, 1))
    return None

def generate_investment_memo(articles: list, company_info: dict, financial_data: dict, max_retries=3) -> str | None:
    """Generates a detailed investment memo for a single company."""
    logging.info(f"Generating deep-dive investment memo for {company_info.get('ticker')}...")
    model = genai.GenerativeModel('gemini-1.5-flash')
    full_context = "\n\n---\n\n".join([f"Title: {a['title']}\nSummary:\n{a['summary']}" for a in articles if company_info.get('ticker') in a['summary'] or company_info.get('name') in a['summary']])
    
    prompt = f"""
    **Persona Activation & Mandate:**
    You are "GEM-PM," a world-class long-short equity investment analyst. Your investment philosophy is rooted in deep fundamental analysis and a variant perception. You are relentlessly data-driven and skeptical.

    **Core Task:**
    Generate a comprehensive, institutional-quality investment memo for **{company_info.get('name')} ({company_info.get('ticker')})**.
    Your analysis must synthesize the provided qualitative news summaries with the provided quantitative financial data.

    **Provided Quantitative Data:**
    ```json
    {json.dumps(financial_data, indent=2)}
    ```

    **Provided Qualitative News Summaries (Your "Scuttlebutt"):**
    ```
    {full_context}
    ```

    **Required Output Structure:**
    ---
    **INVESTMENT MEMO**

    **To:** Investment Committee
    **From:** GEM-PM
    **Date:** {datetime.now().strftime('%B %d, %Y')}
    **Subject:** Investment Thesis for {company_info.get('name')} ({company_info.get('ticker')})

    **1. Executive Summary & Investment Thesis:**
    * **Recommendation:** (e.g., High-Conviction Long, Tactical Long, Short, Hold/Avoid).
    * **Price Target & Time Horizon:** (e.g., Price target within 18 months, representing potential upside/downside).
    * **Thesis Summary (The Variant Perception):** In 3-5 bullet points, what is the core thesis based on the news and data? Why might the market be mispricing this security?
    * **Key Catalysts:** What specific events, implied by the news, could unlock value?
    * **Conviction Level:** (High/Medium/Low) and rationale.

    **2. Business & Competitive Landscape:**
    * **Business Model:** How does the company make money? What do the news items imply about its key segments?
    * **Industry Deep Dive:** What is the state of the industry according to the provided news?
    * **Competitive Moat:** Based on the news, is the company's competitive advantage widening or narrowing?

    **3. Synthesis of Recent Information & Scuttlebutt:**
    * **News Flow Analysis:** What are the key takeaways from the provided news summaries? What is the tone?

    **4. The Bull Case (Primary Drivers):**
    * List the 3-5 primary reasons to be long the stock, as suggested by the news.

    **5. The Bear Case (Key Risks & Mitigants):**
    * List the 3-5 primary risks to the thesis, as suggested by the news.

    **6. Valuation Analysis:**
    * Based on the news flow and the provided financial data (Price, P/E Ratio, Market Cap), provide a qualitative discussion on valuation. Is the news likely to be accretive or dilutive to its current valuation?

    **7. Capital Allocation & Management Quality:**
    * Does the news provide any insight into management's strategy or capital allocation?

    **8. Recommendation & Portfolio Implementation:**
    * Reiterate the final recommendation.
    ---
    """
    attempt = 0
    while attempt < max_retries:
        try:
            response = model.generate_content(prompt)
            logging.info(f"...Successfully generated memo for {company_info.get('ticker')}.")
            return response.text
        except Exception as e:
            attempt += 1
            if attempt >= max_retries:
                logging.error(f"Failed to generate memo for {company_info.get('ticker')} after {max_retries} attempts: {e}", exc_info=True)
                return None
            time.sleep((2 ** attempt) + random.uniform(0, 1))
    return None

# --- SHARED DATA LOADING FUNCTION ---
def load_archived_news(data_dir: str, days_to_analyze: int) -> list:
    """Loads all JSON data files from the archive directory for a given number of days."""
    logging.info(f"Searching for data files in '{data_dir}' for the last {days_to_analyze} days...")
    all_articles = []
    today = datetime.now(timezone.utc).date()
    for i in range(days_to_analyze):
        target_date = today - timedelta(days=i)
        date_str = target_date.strftime('%Y-%m-%d')
        json_filename = os.path.join(data_dir, f"news_{date_str}.json")
        if os.path.exists(json_filename):
            try:
                with open(json_filename, 'r', encoding='utf-8') as f:
                    daily_articles = json.load(f)
                    all_articles.extend(daily_articles)
                    logging.info(f"  - Loaded {len(daily_articles)} articles from {json_filename}")
            except Exception as e:
                logging.error(f"  - Could not read file {json_filename}. Error: {e}", exc_info=True)
    if not all_articles:
        logging.warning("\nNo articles found for the specified period.")
    else:
        logging.info(f"\nTotal articles loaded for analysis: {len(all_articles)}")
    return all_articles

# --- SHARED EMAIL & FILE FUNCTIONS ---
def send_email_with_attachment(recipient_email, subject, body, file_to_attach):
    sender_email = os.getenv('EMAIL_ADDRESS')
    sender_password = os.getenv('EMAIL_PASSWORD')
    if not sender_email or not sender_password:
        logging.error("Email credentials not found.")
        return False
    msg = MIMEMultipart()
    msg['From'] = f"Daily Gemini Report <{sender_email}>"
    msg['To'] = recipient_email
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))
    try:
        with open(file_to_attach, "rb") as f:
            attach = MIMEApplication(f.read(), _subtype="pdf")
        attach.add_header('Content-Disposition', 'attachment', filename=os.path.basename(file_to_attach))
        msg.attach(attach)
    except Exception as e:
        logging.error(f"Could not attach file: {e}", exc_info=True)
        return False
    try:
        logging.info(f"Connecting to SMTP server to send report to {recipient_email}...")
        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            server.starttls()
            server.login(sender_email, sender_password)
            server.send_message(msg)
        logging.info("Email with attachment sent successfully!")
        return True
    except Exception as e:
        logging.error(f"Failed to send email: {e}", exc_info=True)
        return False

def send_html_email(recipient_email, subject, html_body):
    sender_email = os.getenv('EMAIL_ADDRESS')
    sender_password = os.getenv('EMAIL_PASSWORD')
    if not sender_email or not sender_password:
        logging.error("Email credentials not found.")
        return False
    msg = MIMEMultipart()
    msg['Subject'] = subject
    msg['From'] = f"Weekly Gemini Briefing <{sender_email}>"
    msg['To'] = recipient_email
    msg.attach(MIMEText(html_body, 'html'))
    try:
        logging.info(f"Connecting to SMTP server to send weekly briefing to {recipient_email}...")
        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            server.starttls()
            server.login(sender_email, sender_password)
            server.send_message(msg)
        logging.info("Weekly HTML email report sent successfully!")
        return True
    except Exception as e:
        logging.error(f"Failed to send weekly email: {e}", exc_info=True)
        return False

def send_failure_notification(script_name: str, error_message: str):
    recipient_email = os.getenv('RECIPIENT_EMAIL', 'hcarr3000@gmail.com')
    sender_email = os.getenv('EMAIL_ADDRESS')
    sender_password = os.getenv('EMAIL_PASSWORD')
    if not sender_email or not sender_password:
        logging.error("Cannot send failure notification: Email credentials not found.")
        return
    subject = f"SCRIPT FAILURE: {script_name} has failed"
    body = f"The automated script '{script_name}' encountered a critical error.\n\nError:\n---\n{error_message}\n---\nPlease check logs."
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = f"Automated Script Alert <{sender_email}>"
    msg['To'] = recipient_email
    try:
        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            server.starttls()
            server.login(sender_email, sender_password)
            server.send_message(msg)
        logging.info("Failure notification email sent.")
    except Exception as e:
        logging.error(f"Failed to send failure notification email: {e}", exc_info=True)

def cleanup_old_files(directory: str, days_to_keep: int):
    """Removes files in a directory older than a specified number of days."""
    logging.info(f"Running cleanup of files older than {days_to_keep} days in '{directory}'...")
    if not os.path.isdir(directory):
        logging.warning(f"Directory '{directory}' not found. Skipping cleanup.")
        return
    cutoff = time.time() - (days_to_keep * 86400)
    files_deleted = 0
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        try:
            if os.path.getmtime(file_path) < cutoff:
                logging.info(f"Deleting old file: {filename}")
                os.remove(file_path)
                files_deleted += 1
        except Exception as e:
            logging.error(f"Error processing or deleting file {file_path}: {e}", exc_info=True)
    logging.info(f"Cleanup complete. Deleted {files_deleted} file(s).")

def send_dashboard_link_email(recipient_email: str, dashboard_url: str):
    """Sends an email with the public link to the Streamlit dashboard."""
    sender_email = os.getenv('EMAIL_ADDRESS')
    sender_password = os.getenv('EMAIL_PASSWORD')
    if not sender_email or not sender_password:
        logging.error("Cannot send dashboard link: Email credentials not found.")
        return False
    subject = "Your AI News Dashboard is Live!"
    body = f"""
    Hello,

    Your AI-Powered News Analysis Dashboard has been started and is now available at the following public URL:
    {dashboard_url}

    You can share this link with others. The dashboard will remain live as long as the application is running.

    - Automated System
    """
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = f"Dashboard Alert <{sender_email}>"
    msg['To'] = recipient_email
    try:
        logging.info(f"Sending dashboard link to {recipient_email}...")
        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            server.starttls()
            server.login(sender_email, sender_password)
            server.send_message(msg)
        logging.info("Dashboard link email sent successfully!")
        return True
    except Exception as e:
        logging.error(f"Failed to send dashboard link email: {e}", exc_info=True)
        return False
