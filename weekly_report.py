import os
import json
import logging
import pytz
from datetime import datetime
from collections import Counter

# Import shared functions from our utility module
import utility

# --- PATH SETUP (OneDrive Fix) ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# --- CONFIGURATION ---
RECIPIENT_EMAIL = "hcarr3000@gmail.com"
DATA_DIR = os.path.join(BASE_DIR, "daily_news_data")
DAYS_TO_ANALYZE = 7

def analyze_weekly_trends(articles: list) -> dict:
    """Analyzes sentiment and entity trends from a list of articles."""
    logging.info("Analyzing weekly sentiment and entity trends...")
    sentiments = [article.get('sentiment', 'Unknown') for article in articles]
    sentiment_counts = Counter(sentiments)
    total_sentiments = sum(sentiment_counts.values())
    sentiment_summary = {
        "Positive": f"{sentiment_counts.get('Positive', 0) / total_sentiments:.1%}" if total_sentiments else "0%",
        "Negative": f"{sentiment_counts.get('Negative', 0) / total_sentiments:.1%}" if total_sentiments else "0%",
        "Neutral": f"{sentiment_counts.get('Neutral', 0) / total_sentiments:.1%}" if total_sentiments else "0%",
    }
    all_entities = {"companies": [], "people": [], "topics": []}
    for article in articles:
        entities = article.get('entities', {})
        for category, items in entities.items():
            if category in all_entities and isinstance(items, list):
                all_entities[category].extend(items)
    top_entities = {
        "Top Companies": [item for item, count in Counter(all_entities["companies"]).most_common(5)],
        "Top People": [item for item, count in Counter(all_entities["people"]).most_common(5)],
        "Top Topics": [item for item, count in Counter(all_entities["topics"]).most_common(5)],
    }
    logging.info("...Trend analysis complete.")
    return {"sentiment_summary": sentiment_summary, "top_entities": top_entities}

def generate_html_email_body(takeaways: str, trends: dict, days: int) -> str:
    """Generates a rich HTML body for the weekly email report."""
    def format_list_as_html(items: list) -> str:
        if not items:
            return "<li>None mentioned</li>"
        return "".join([f"<li>{item}</li>" for item in items])
    takeaways_html = takeaways.replace('**', '<b>').replace('\n', '<br>')
    sentiment_summary = trends.get("sentiment_summary", {})
    top_entities = trends.get("top_entities", {})
    html = f"""
    <html><head><style>
        body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
        .container {{ max-width: 700px; margin: 20px auto; padding: 20px; border: 1px solid #ddd; border-radius: 8px; }}
        h1 {{ color: #2c3e50; }} h2 {{ color: #34495e; border-bottom: 2px solid #ecf0f1; padding-bottom: 5px; }}
        .summary-box {{ background-color: #f9f9f9; padding: 15px; border-radius: 5px; margin-bottom: 20px; }}
        .sentiment-positive {{ color: #27ae60; font-weight: bold; }} .sentiment-negative {{ color: #c0392b; font-weight: bold; }}
        .sentiment-neutral {{ color: #7f8c8d; font-weight: bold; }} ul {{ padding-left: 20px; }}
    </style></head><body><div class="container">
        <h1>Weekly Investor Briefing</h1><p>Here are the top insights based on news from the past {days} days.</p>
        <h2>Weekly Trend Analysis</h2><div class="summary-box"><b>Overall News Sentiment:</b><ul>
            <li>Positive: <span class="sentiment-positive">{sentiment_summary.get('Positive', '0%')}</span></li>
            <li>Negative: <span class="sentiment-negative">{sentiment_summary.get('Negative', '0%')}</span></li>
            <li>Neutral: <span class="sentiment-neutral">{sentiment_summary.get('Neutral', '0%')}</span></li></ul>
            <b>Most Mentioned This Week:</b><ul>
            <li><b>Companies:</b><ul>{format_list_as_html(top_entities.get("Top Companies"))}</ul></li>
            <li><b>People:</b><ul>{format_list_as_html(top_entities.get("Top People"))}</ul></li>
            <li><b>Topics:</b><ul>{format_list_as_html(top_entities.get("Top Topics"))}</ul></li></ul></div>
        <h2>Actionable Takeaways</h2><p>{takeaways_html}</p>
    </div></body></html>
    """
    return html

def run_weekly_analysis():
    """Main function to load data and generate the weekly analysis."""
    logging.info("="*60)
    logging.info("Starting new weekly news analysis run.")
    logging.info("="*60)
    all_articles_this_week = utility.load_archived_news(DATA_DIR, DAYS_TO_ANALYZE)
    if not all_articles_this_week:
        logging.info("Cannot run analysis as no articles were loaded.")
        return
    weekly_takeaways = utility.generate_hedge_fund_takeaways(all_articles_this_week, time_frame="weekly")
    if weekly_takeaways:
        weekly_trends = analyze_weekly_trends(all_articles_this_week)
        html_body = generate_html_email_body(weekly_takeaways, weekly_trends, DAYS_TO_ANALYZE)
        eastern_tz = pytz.timezone('US/Eastern')
        week_end_date = datetime.now(eastern_tz).strftime('%B %d, %Y')
        email_subject = f"Your Weekly Gemini Investor Briefing for {week_end_date}"
        utility.send_html_email(RECIPIENT_EMAIL, email_subject, html_body)
    else:
        logging.error("\nFailed to generate weekly takeaways. No report will be sent.")
    logging.info("="*60)
    logging.info("Weekly news analysis run finished.")
    logging.info("="*60 + "\n")

if __name__ == "__main__":
    log_file_path = os.path.join(BASE_DIR, 'weekly_report.log')
    utility.setup_logging(log_file_path)
    utility.configure_gemini()
    try:
        run_weekly_analysis()
    except Exception as e:
        logging.critical("The weekly_report.py script failed unexpectedly.", exc_info=True)
        utility.send_failure_notification("weekly_report.py", str(e))
