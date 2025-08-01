import os
import pytz
import re
import time
import json
import random
import logging
import concurrent.futures
from datetime import datetime, timedelta, timezone
import feedparser
from dateutil import parser
from collections import defaultdict

from newspaper import Article, Config
from fpdf import FPDF
import google.generativeai as genai

# Import shared functions from our new utility module
import utility

# --- PATH SETUP (OneDrive Fix) ---
# Get the absolute path of the directory where this script is located
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# --- CONFIGURATION ---
RECIPIENT_EMAIL = "hcarr3000@gmail.com"
NEWS_SOURCES = [
    {'name': 'BioPharma Dive', 'url': 'https://www.biopharmadive.com/feeds/news/'},
    {'name': 'Automotive Dive', 'url': 'https://www.automotivedive.com/feeds/news/'},
    {'name': 'CFO Dive', 'url': 'https://www.cfodive.com/feeds/news/'},
    {'name': 'C-Store Dive', 'url': 'https://www.cstoredive.com/feeds/news/'},
    {'name': 'Banking Dive', 'url': 'https://www.bankingdive.com/feeds/news/'},
    {'name': 'CIO Dive', 'url': 'https://www.ciodive.com/feeds/news/'},
    {'name': 'CFO', 'url': 'https://www.cfo.com/feeds/news/'},
    {'name': 'Construction Dive', 'url': 'https://www.constructiondive.com/feeds/news/'},
    {'name': 'Cybersecurity Dive', 'url': 'https://www.cybersecuritydive.com/feeds/news/'},
    {'name': 'Education Dive', 'url': 'https://www.educationdive.com/feeds/news/'},
    {'name': 'Facilities Dive', 'url': 'https://www.facilitiesdive.com/feeds/news/'},
    {'name': 'Fashion Dive', 'url': 'https://www.fashiondive.com/feeds/news/'},
    {'name': 'Food Dive', 'url': 'https://www.fooddive.com/feeds/news/'},
    {'name': 'Grocery Dive', 'url': 'https://www.grocerydive.com/feeds/news/'},
    {'name': 'Healthcare Dive', 'url': 'https://www.healthcaredive.com/feeds/news/'},
    {'name': 'Hotel Dive', 'url': 'https://www.hoteldive.com/feeds/news/'},
    {'name': 'HR Dive', 'url': 'https://www.hrdive.com/feeds/news/'},
    {'name': 'K-12 Dive', 'url': 'https://www.k12dive.com/feeds/news/'},
    {'name': 'Legal Dive', 'url': 'https://www.legaldive.com/feeds/news/'},
    {'name': 'Manufacturing Dive', 'url': 'https://www.manufacturingdive.com/feeds/news/'},
    {'name': 'Marketing Dive', 'url': 'https://www.marketingdive.com/feeds/news/'},
    {'name': 'MedTech Dive', 'url': 'https://www.medtechdive.com/feeds/news/'},
    {'name': 'Multifamily Dive', 'url': 'https://www.multifamilydive.com/feeds/news/'},
    {'name': 'Packaging Dive', 'url': 'https://www.packagingdive.com/feeds/news/'},
    {'name': 'Payments Dive', 'url': 'https://www.paymentsdive.com/feeds/news/'},
    {'name': 'Restaurant Dive', 'url': 'https://www.restaurantdive.com/feeds/news/'},
    {'name': 'Retail Dive', 'url': 'https://www.retaildive.com/feeds/news/'},
    {'name': 'Smart Cities Dive', 'url': 'https://www.smartcitiesdive.com/feeds/news/'},
    {'name': 'Software Dive', 'url': 'https://www.softwaredive.com/feeds/news/'},
    {'name': 'Supply Chain Dive', 'url': 'https://www.supplychaindive.com/feeds/news/'},
    {'name': 'Pharma Voice', 'url': 'https://www.pharmavoice.com/feeds/news/'},
    {'name': 'Social Media Today', 'url': 'https://www.socialmediatoday.com/feeds/news/'},
    {'name': 'Trucking Dive', 'url': 'https://www.truckingdive.com/feeds/news/'},
    {'name': 'Transport Dive', 'url': 'https://www.transportdive.com/feeds/news/'},
    {'name': 'Utility Dive', 'url': 'https://www.utilitydive.com/feeds/news/'},
    {'name': 'Waste Dive', 'url': 'https://www.wastedive.com/feeds/news/'},
]
DATA_DIR = os.path.join(BASE_DIR, "daily_news_data")

# --- DAILY SCRIPT SPECIFIC FUNCTIONS ---

def generate_axios_summary(full_article_text: str, max_retries=3) -> str | None:
    """Generates an Axios-style summary with exponential backoff for API calls."""
    model = genai.GenerativeModel('gemini-1.5-flash')
    prompt = f"""
    Your task is to act as a highly disciplined financial news analyst. You must summarize the following news article in the "Smart Brevity" style of Axios, focusing on what matters to an investor.
    **CRITICAL RULE: YOU ARE STRICTLY FORBIDDEN FROM USING ANY INFORMATION, MAKING INFERENCES, OR ADDING EXTERNAL KNOWLEDGE NOT EXPLICITLY PRESENT IN THE PROVIDED ARTICLE TEXT. EVERY PIECE OF YOUR SUMMARY MUST BE DIRECTLY SOURCED FROM THE TEXT BELOW. If a required piece of information is not in the text, you must write "Not mentioned in the article."**
    Structure your response using ONLY the following components:
    1.  **Headline:** A short, impactful headline (8-12 words) based only on the article's main point.
    2.  **Key details:** A bolded section starting with "Key details:". In 3-5 sentences, explain the new details *as described in the article*.
    3.  **Why it matters:** A bolded section starting with "Why it matters:". In 3-5 sentences, explain the significance *as described in the article*. **Crucially, identify why this information might challenge the current consensus view on the company or industry, based only on the article's content.**
    4.  **The big picture:** A bolded section starting with "The big picture:". In 3-5 sentences, provide the background or context *as described in the article*.
    5.  **By the numbers:** A bolded section starting with "By the numbers:". Provide 4-5 bullet points (using '*') with the most important metrics, facts and figures *taken directly from the article*. If the article has fewer than 4-5 key figures, list only what is available.
    6.  **Key Players:** A bolded section starting with "Key Players:". Provide bullet points (using '*') listing companies and individuals *mentioned by name in the article*. For each, provide a 10-12 word summary of their involvement *based only on the text*.
    7.  **Looking Forward:** A bolded section starting with "Looking Forward:". In 1-2 bullet points, state the immediate next event or data point to watch for, based *only* on the information in the article (e.g., "The Q3 earnings report on October 25th," "The FDA's decision expected in December").
    8.  **The Bottom Line:** A bolded section starting with "The Bottom Line:". A single, impactful sentence (under 15 words) that summarizes the ultimate consequence or takeaway of the news.
    Here is the article text:
    ---
    {full_article_text}
    """
    attempt = 0
    while attempt < max_retries:
        try:
            response = model.generate_content(prompt)
            time.sleep(random.uniform(1, 3)) 
            return response.text
        except Exception as e:
            attempt += 1
            if attempt >= max_retries:
                logging.error(f"Gemini API Error after {max_retries} attempts: {e}", exc_info=True)
                return None
            wait_time = (2 ** attempt) + random.uniform(0, 1)
            logging.warning(f"Gemini API Error. Retrying in {wait_time:.2f} seconds...")
            time.sleep(wait_time)
    return None

def fetch_news_from_sources(sources: list, max_per_source=10, processed_urls=set()):
    logging.info("Fetching news from all sources...")
    all_articles = []
    config = Config()
    config.browser_user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'
    config.request_timeout = 15
    for source in sources:
        source_name = source['name']
        rss_url = source['url']
        logging.info(f"  -> Fetching from {source_name}...")
        feed = feedparser.parse(rss_url)
        if not feed.entries:
            logging.warning(f"       --> Could not fetch entries from {source_name}.")
            continue
        for entry in feed.entries[:max_per_source]:
            article_url = entry.get('link', '')
            if article_url in processed_urls:
                logging.info(f"       -> Skipping already processed article: {entry.get('title', 'No Title')}")
                continue
            rss_summary = entry.get('summary', '')
            text_for_summary = ""
            is_full_text = False
            if not article_url:
                continue
            try:
                logging.info(f"       -> Downloading full text for: {entry.get('title', 'No Title')}")
                article = Article(article_url, config=config)
                article.download()
                article.parse()
                if article.text and len(article.text) > len(rss_summary):
                    text_for_summary = article.text
                    is_full_text = True
                    logging.info("         ...Success, using full text.")
                else:
                    text_for_summary = rss_summary
                    logging.info("         ...Full text not substantially longer, using RSS summary instead.")
            except Exception as e:
                logging.warning(f"         ...Download failed ({e}), using RSS summary instead.")
                text_for_summary = rss_summary
            if text_for_summary and len(text_for_summary) > 100:
                all_articles.append({
                    'source': source_name,
                    'title': entry.get('title', 'No Title'),
                    'link': article_url,
                    'date': entry.get('published', ''),
                    'summary_text': text_for_summary,
                    'is_full_text': is_full_text
                })
    logging.info(f"\nSuccessfully prepared a total of {len(all_articles)} new articles for processing.")
    return all_articles

def generate_pdf(grouped_articles: dict, investment_takeaways: str, filename="news_summary.pdf"):
    pdf = FPDF()
    try:
        font_regular_path = os.path.join(BASE_DIR, 'DejaVuSans.ttf')
        font_bold_path = os.path.join(BASE_DIR, 'DejaVuSans-Bold.ttf')
        font_italic_path = os.path.join(BASE_DIR, 'DejaVuSans-Oblique.ttf')
        pdf.add_font('DejaVu', '', font_regular_path)
        pdf.add_font('DejaVu', 'B', font_bold_path)
        pdf.add_font('DejaVu', 'I', font_italic_path)
        pdf.set_font('DejaVu', '', 11)
        font_styles = {'default': ('DejaVu', '', 11), 'default_b': ('DejaVu', 'B', 11), 'title': ('DejaVu', '', 24), 'subtitle': ('DejaVu', '', 10), 'h1': ('DejaVu', 'B', 16), 'h2': ('DejaVu', 'B', 14), 'h3_b': ('DejaVu', 'B', 12), 'link': ('DejaVu', '', 11), 'italic': ('DejaVu', 'I', 9)}
    except Exception as e:
        logging.warning(f"Could not load DejaVuSans font family. Reason: {e}. PDF may have character issues. Falling back to Helvetica.")
        pdf.set_font("helvetica", size=11)
        font_styles = {'default': ('helvetica', '', 11), 'default_b': ('helvetica', 'B', 11), 'title': ('helvetica', '', 24), 'subtitle': ('helvetica', '', 10), 'h1': ('helvetica', 'B', 16), 'h2': ('helvetica', 'B', 14), 'h3_b': ('helvetica', 'B', 12), 'link': ('helvetica', '', 11), 'italic': ('helvetica', 'I', 9)}

    pdf.add_page()
    pdf.set_font(*font_styles['title'])
    pdf.multi_cell(0, 20, "Daily Industry News Summary", align='C')
    report_date = datetime.now(pytz.timezone('US/Eastern')).strftime('%B %d, %Y %I:%M %p EST')
    pdf.set_font(*font_styles['subtitle'])
    pdf.multi_cell(0, 10, f"Report generated on: {report_date}", align='C')
    pdf.ln(10)

    if investment_takeaways:
        pdf.set_font(*font_styles['h1'])
        pdf.multi_cell(0, 10, "Actionable Investor Takeaways")
        pdf.ln(2)
        for line in investment_takeaways.split('\n'):
            if line.startswith('**') and line.endswith('**'):
                pdf.set_font(*font_styles['h3_b'])
                pdf.write(6, line.strip('*'))
                pdf.ln()
            else:
                pdf.set_font(*font_styles['default'])
                pdf.write(6, line)
                pdf.ln()
        pdf.ln(8)
        pdf.line(pdf.get_x(), pdf.get_y(), pdf.get_x() + 190, pdf.get_y())
        pdf.ln(8)

    pdf.set_font(*font_styles['h1'])
    pdf.multi_cell(0, 10, "Articles in this Report:")
    pdf.ln(5)
    for source_name, articles in sorted(grouped_articles.items()):
        pdf.set_font(*font_styles['h3_b'])
        pdf.multi_cell(0, 8, f"{source_name}:")
        pdf.set_font(*font_styles['link'])
        pdf.set_text_color(40, 40, 180)
        for article in articles:
            article['pdf_link'] = pdf.add_link()
            pdf.multi_cell(0, 6, f"- {article['title']}", link=article['pdf_link'])
        pdf.ln(4)
    pdf.set_text_color(0, 0, 0)
    
    for source_name, articles in sorted(grouped_articles.items()):
        pdf.add_page()
        pdf.set_font(*font_styles['h2'])
        pdf.set_fill_color(230, 230, 230)
        pdf.multi_cell(0, 12, f" {source_name} ", fill=True)
        pdf.ln(5)
        for article in articles:
            pdf.set_link(article['pdf_link'], y=pdf.get_y())
            try:
                parsed_date = parser.parse(article['date'])
                localized_date = parsed_date.astimezone(pytz.timezone('US/Eastern'))
                safe_date = localized_date.strftime('%b %d, %Y at %I:%M %p %Z')
            except (ValueError, TypeError):
                safe_date = "Date not available"
            pdf.set_font(*font_styles['h2'])
            pdf.multi_cell(0, 8, article['title'])
            pdf.ln(1)
            pdf.set_font(*font_styles['italic'])
            pdf.set_text_color(40, 40, 180)
            pdf.multi_cell(0, 5, f"Published: {safe_date} | Link to full article", link=article['link'])
            pdf.set_text_color(0, 0, 0)
            pdf.ln(3)
            sentiment = article.get('sentiment', 'Unknown')
            sentiment_color = {'Positive': (0, 150, 0), 'Negative': (200, 0, 0), 'Neutral': (100, 100, 100)}.get(sentiment, (0, 0, 0))
            pdf.set_font(*font_styles['h3_b'])
            pdf.set_text_color(*sentiment_color)
            pdf.multi_cell(0, 6, f"Sentiment: {sentiment}")
            pdf.set_text_color(0, 0, 0)
            pdf.ln(3)
            if not article['is_full_text']:
                pdf.set_font(*font_styles['italic'])
                pdf.set_text_color(200, 0, 0)
                pdf.multi_cell(0, 5, "(Note: Summary is based on the short RSS description, not the full article.)")
                pdf.set_text_color(0, 0, 0)
                pdf.ln(3)
            for part in re.split(r'(\*\*.*?\*\*)', article['summary']):
                if part.startswith('**') and part.endswith('**'):
                    pdf.set_font(*font_styles['default_b'])
                    pdf.write(6, part.strip('*'))
                else:
                    pdf.set_font(*font_styles['default'])
                    pdf.write(6, part)
            pdf.ln(10)
            pdf.line(pdf.get_x(), pdf.get_y(), pdf.get_x() + 190, pdf.get_y())
            pdf.ln(6)

    output_path = os.path.join(BASE_DIR, filename)
    pdf.output(output_path)
    logging.info(f"Successfully generated PDF: {output_path}")
    return output_path

def process_single_article(article_details):
    """Helper function to summarize and analyze a single article for concurrent processing."""
    title = article_details['title']
    logging.info(f"  -> Processing: '{title}'")
    try:
        ai_summary = generate_axios_summary(article_details['summary_text'])
        if ai_summary:
            logging.info(f"    ... Summarized: '{title}'")
            analysis_data = utility.analyze_content(ai_summary)
            article_data = {
                'source': article_details['source'], 'date': article_details['date'],
                'title': title, 'summary': ai_summary,
                'link': article_details['link'], 'is_full_text': article_details['is_full_text'],
                'sentiment': analysis_data.get('sentiment', 'Unknown'),
                'entities': analysis_data.get('entities', {})
            }
            return article_data
        else:
            logging.warning(f"  -> Failed to summarize: '{title}' (AI summary was empty).")
            return None
    except Exception as exc:
        logging.error(f"'{title}' generated an exception during processing: {exc}", exc_info=True)
        return None

def run_news_report():
    logging.info("="*60)
    logging.info("Starting new daily news report run.")
    logging.info("="*60)
    
    PROCESSED_URLS_FILE = os.path.join(BASE_DIR, "processed_urls.json")
    RETENTION_DAYS = 14
    processed_articles_history = {}

    try:
        with open(PROCESSED_URLS_FILE, 'r') as f:
            processed_articles_history = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        processed_articles_history = {}

    cutoff_date = datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)
    urls_to_keep = {url: timestamp for url, timestamp in processed_articles_history.items() if datetime.fromisoformat(timestamp) > cutoff_date}
    
    purged_count = len(processed_articles_history) - len(urls_to_keep)
    if purged_count > 0:
        logging.info(f"Purged {purged_count} old URL(s) from tracking file.")
    
    processed_urls_set = set(urls_to_keep.keys())
    all_raw_articles = fetch_news_from_sources(NEWS_SOURCES, max_per_source=10, processed_urls=processed_urls_set)
    if not all_raw_articles:
        logging.info("No new articles fetched. Exiting.")
        with open(PROCESSED_URLS_FILE, 'w') as f:
            json.dump(urls_to_keep, f, indent=4)
        logging.info(f"Updated tracking file with {len(urls_to_keep)} URLs.")
        return
    
    summarized_articles = []
    logging.info("-" * 50)
    logging.info(f"Attempting to summarize and analyze {len(all_raw_articles)} articles concurrently...")

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        future_to_article = {executor.submit(process_single_article, article): article for article in all_raw_articles}
        for future in concurrent.futures.as_completed(future_to_article):
            result = future.result()
            if result:
                summarized_articles.append(result)
                urls_to_keep[result['link']] = datetime.now(timezone.utc).isoformat()

    if not summarized_articles:
        logging.warning("No articles were successfully summarized to generate a report.")
        return

    investment_takeaways = utility.generate_hedge_fund_takeaways(summarized_articles, time_frame="daily")
    grouped_articles = defaultdict(list)
    for article in summarized_articles:
        grouped_articles[article['source']].append(article)
    
    pdf_filename = generate_pdf(grouped_articles, investment_takeaways or "Takeaway generation failed.")
    
    eastern_tz = pytz.timezone('US/Eastern')
    email_subject = f"Your Gemini Industry News Summary - {datetime.now(eastern_tz).strftime('%B %d, %Y')}"
    email_body = f"Attached is your AI-powered industry news summary, containing {len(summarized_articles)} successfully summarized new articles."
    
    email_sent = utility.send_email_with_attachment(RECIPIENT_EMAIL, email_subject, email_body, pdf_filename)

    if email_sent and os.path.exists(pdf_filename):
        os.remove(pdf_filename)
        logging.info(f"Cleaned up temporary file: {pdf_filename}")

    with open(PROCESSED_URLS_FILE, 'w') as f:
        json.dump(urls_to_keep, f, indent=4)
    logging.info(f"Saved/updated tracking file with {len(urls_to_keep)} URLs for next run.")
    
    os.makedirs(DATA_DIR, exist_ok=True)
    today_str = datetime.now(eastern_tz).strftime('%Y-%m-%d')
    json_filename = os.path.join(DATA_DIR, f"news_{today_str}.json")
    with open(json_filename, 'w', encoding='utf-8') as f:
        json.dump(summarized_articles, f, indent=4)
    logging.info(f"Saved daily summaries to: {json_filename}")

    utility.cleanup_old_files(DATA_DIR, days_to_keep=400)
    
    logging.info("="*60)
    logging.info("Daily news report run finished.")
    logging.info("="*60 + "\n")

if __name__ == "__main__":
    log_file_path = os.path.join(BASE_DIR, 'daily_report.log')
    utility.setup_logging(log_file_path)
    utility.configure_gemini()
    try:
        run_news_report()
    except Exception as e:
        logging.critical("The daily_report.py script failed unexpectedly.", exc_info=True)
        utility.send_failure_notification("daily_report.py", str(e))
