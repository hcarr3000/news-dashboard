import os
import json
import logging
import pytz
from datetime import datetime
from fpdf import FPDF

# Import shared functions from our utility module
import utility

# --- PATH SETUP (OneDrive Fix) ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# --- CONFIGURATION ---
RECIPIENT_EMAIL = "hcarr3000@gmail.com"
DATA_DIR = os.path.join(BASE_DIR, "daily_news_data")
DAYS_TO_ANALYZE = 7

def generate_memo_pdf(memo_text: str, filename: str):
    pdf = FPDF()
    pdf.add_page()
    try:
        font_regular_path = os.path.join(BASE_DIR, 'DejaVuSans.ttf')
        font_bold_path = os.path.join(BASE_DIR, 'DejaVuSans-Bold.ttf')
        font_italic_path = os.path.join(BASE_DIR, 'DejaVuSans-Oblique.ttf')
        pdf.add_font('DejaVu', '', font_regular_path)
        pdf.add_font('DejaVu', 'B', font_bold_path)
        pdf.add_font('DejaVu', 'I', font_italic_path)
        pdf.set_font('DejaVu', '', 10)
    except Exception as e:
        logging.warning(f"Could not load DejaVuSans font family. Reason: {e}. Falling back to basic font.")
        pdf.set_font('Arial', '', 10)
    for line in memo_text.split('\n'):
        line = line.strip()
        if line.startswith('**') and line.endswith('**'):
            pdf.set_font(family=pdf.font_family, style='B', size=12)
            pdf.multi_cell(0, 10, line.strip('*'))
            pdf.set_font(family=pdf.font_family, style='', size=10)
        elif line.startswith('*'):
            pdf.multi_cell(0, 5, f"  • {line.strip('* ')}")
        elif line.lower().startswith('to:') or line.lower().startswith('from:') or line.lower().startswith('date:') or line.lower().startswith('subject:'):
            pdf.set_font(family=pdf.font_family, style='I', size=9)
            pdf.multi_cell(0, 5, line)
            pdf.set_font(family=pdf.font_family, style='', size=10)
        elif line == '---':
             pdf.ln(5)
             pdf.line(pdf.get_x(), pdf.get_y(), pdf.get_x() + 190, pdf.get_y())
             pdf.ln(5)
        else:
            pdf.multi_cell(0, 5, line)
    output_path = os.path.join(BASE_DIR, filename)
    pdf.output(output_path)
    logging.info(f"Successfully generated investment memo PDF: {output_path}")
    return output_path

def run_deep_dive_analysis():
    """Main function to load data and generate the deep-dive memo."""
    logging.info("="*60)
    logging.info("Starting new Deep-Dive Investment Analysis run.")
    logging.info("="*60)
    all_articles = utility.load_archived_news(DATA_DIR, DAYS_TO_ANALYZE)
    if not all_articles:
        return
    company_selections = utility.get_company_selections(all_articles)
    if not company_selections:
        logging.error("Could not identify any companies for deep-dive analysis.")
        return
    all_memos = []
    for company_info in company_selections:
        ticker = company_info['ticker']
        logging.info(f"--- Analyzing {ticker} ---")
        financial_data = utility.get_financial_data(ticker)
        memo = utility.generate_investment_memo(
            articles=all_articles, 
            company_info=company_info, 
            financial_data=financial_data
        )
        if memo:
            all_memos.append(memo)
    if all_memos:
        full_memo_text = "\n\n---\n\n".join(all_memos)
        eastern_tz = pytz.timezone('US/Eastern')
        report_date_str = datetime.now(eastern_tz).strftime('%Y-%m-%d')
        pdf_filename = f"GEM-PM_Deep_Dive_{report_date_str}.pdf"
        generate_memo_pdf(full_memo_text, pdf_filename)
        email_subject = f"GEM-PM Deep-Dive Analysis (with Financial Data) for {report_date_str}"
        email_body = "Attached is your deep-dive investment memo, now enriched with quantitative financial data."
        email_sent = utility.send_email_with_attachment(RECIPIENT_EMAIL, email_subject, email_body, pdf_filename)
        if email_sent and os.path.exists(pdf_filename):
            os.remove(pdf_filename)
            logging.info(f"Cleaned up temporary file: {pdf_filename}")
    else:
        logging.error("\nFailed to generate any investment memos. No report will be sent.")
    logging.info("="*60)
    logging.info("Deep-Dive Investment Analysis run finished.")
    logging.info("="*60 + "\n")

if __name__ == "__main__":
    log_file_path = os.path.join(BASE_DIR, 'deep_dive_report.log')
    utility.setup_logging(log_file_path)
    utility.configure_gemini()
    try:
        run_deep_dive_analysis()
    except Exception as e:
        logging.critical("The deep_dive_report.py script failed unexpectedly.", exc_info=True)
        utility.send_failure_notification("deep_dive_report.py", str(e))
