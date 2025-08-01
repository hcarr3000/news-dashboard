import streamlit as st
import pandas as pd
from collections import Counter
import os
import json
import logging
import utility # Your shared utility module

# --- PATH SETUP (OneDrive Fix) ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# --- CONFIGURATION ---
DATA_DIR = os.path.join(BASE_DIR, "daily_news_data")
RECIPIENT_EMAIL = "hcarr3000@gmail.com"

# --- INITIAL SETUP ---
st.set_page_config(layout="wide", page_title="News Analysis Dashboard")
# Configure Gemini API (will only run once)
utility.configure_gemini()

st.title("📰 AI-Powered News Analysis Dashboard")

@st.cache_data
def load_all_data():
    """Loads and caches all news data from the daily_news_data directory."""
    all_articles = []
    if not os.path.isdir(DATA_DIR):
        st.error(f"Data directory not found: {DATA_DIR}")
        return pd.DataFrame()
        
    for filename in os.listdir(DATA_DIR):
        if filename.endswith('.json'):
            file_path = os.path.join(DATA_DIR, filename)
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    daily_articles = json.load(f)
                    all_articles.extend(daily_articles)
            except Exception as e:
                st.error(f"Could not read or parse file {filename}: {e}")

    if not all_articles:
        return pd.DataFrame()
    
    df = pd.DataFrame(all_articles)
    df['date_parsed'] = pd.to_datetime(df['date'], utc=True, errors='coerce')
    df.dropna(subset=['date_parsed'], inplace=True)
    return df

@st.cache_data
def get_cached_takeaways(articles_json_tuple):
    """Caches the generated takeaways to avoid repeated API calls."""
    if not articles_json_tuple:
        return "No articles selected to generate takeaways."
    
    articles_list = [json.loads(s) for s in articles_json_tuple]
    
    return utility.generate_hedge_fund_takeaways(articles_list, time_frame="custom")

# --- Send Startup Email ---
if 'startup_email_sent' not in st.session_state:
    public_url = os.getenv('STREAMLIT_PUBLIC_URL')
    if public_url:
        utility.send_dashboard_link_email(RECIPIENT_EMAIL, public_url)
    st.session_state['startup_email_sent'] = True

# --- Load Data ---
df = load_all_data()

if df.empty:
    st.warning("No news data found. Please run the daily_report.py script first.")
else:
    # --- Sidebar ---
    st.sidebar.header("Controls & Filters")
    view_selection = st.sidebar.radio(
        "Select View:",
        ("News Article Summaries", "Investor Takeaways"),
        help="Switch between viewing individual article summaries or a generated analysis of the filtered articles."
    )
    st.sidebar.markdown("---")
    sources = sorted(df['source'].unique())
    selected_sources = st.sidebar.multiselect("Filter by Source:", sources, default=sources)
    sentiments = sorted(df['sentiment'].unique())
    selected_sentiments = st.sidebar.multiselect("Filter by Sentiment:", sentiments, default=sentiments)
    all_companies = set()
    for entities_dict in df['entities']:
        if isinstance(entities_dict, dict) and 'companies' in entities_dict:
            for company in entities_dict['companies']:
                all_companies.add(company)
    sorted_companies = sorted(list(all_companies))
    selected_companies = st.sidebar.multiselect("Filter by Company:", sorted_companies, default=[])
    min_date = df['date_parsed'].min().date()
    max_date = df['date_parsed'].max().date()
    date_range = st.sidebar.date_input("Filter by Date Range:", [min_date, max_date])

    # Apply filters
    filtered_df = df[(df['source'].isin(selected_sources)) & (df['sentiment'].isin(selected_sentiments))]
    if len(date_range) == 2:
        filtered_df = filtered_df[(filtered_df['date_parsed'].dt.date >= date_range[0]) & (filtered_df['date_parsed'].dt.date <= date_range[1])]
    if selected_companies:
        filtered_df = filtered_df[filtered_df['entities'].apply(lambda entities: isinstance(entities, dict) and any(comp in entities.get('companies', []) for comp in selected_companies))]

    st.sidebar.info(f"Displaying **{len(filtered_df)}** of **{len(df)}** articles.")

    # --- Main Dashboard ---
    col1, col2, col3 = st.columns(3)
    col1.metric("Filtered Articles", len(filtered_df))
    sentiment_counts = filtered_df['sentiment'].value_counts()
    col2.metric("Most Common Sentiment", sentiment_counts.index[0] if not sentiment_counts.empty else "N/A")
    source_counts = filtered_df['source'].value_counts()
    col3.metric("Most Active Source", source_counts.index[0] if not source_counts.empty else "N/A")
    st.markdown("---")

    if view_selection == "News Article Summaries":
        st.subheader("Filtered News Articles")
        for index, row in filtered_df.iterrows():
            with st.expander(f"**{row['title']}** ({row['source']})"):
                st.markdown(f"**Sentiment:** {row['sentiment']}")
                st.markdown(f"**Published:** {row['date_parsed'].strftime('%Y-%m-%d %H:%M')}")
                st.markdown(f"**Link:** [Read Full Article]({row['link']})")
                st.markdown("---")
                st.markdown(row['summary'].replace('**', '<h5>').replace(':', '</h5>', 1), unsafe_allow_html=True)
    
    elif view_selection == "Investor Takeaways":
        st.subheader("Generated Investor Takeaways")
        st.info("This analysis is generated by AI based on the articles matching your current filters.")
        if not filtered_df.empty:
            with st.spinner("Generating AI takeaways for the selected articles..."):
                articles_for_takeaways = tuple(filtered_df.to_json(orient='records', lines=True).splitlines())
                takeaways = get_cached_takeaways(articles_for_takeaways)
                if takeaways:
                    st.markdown(takeaways.replace('**', '<h4>').replace('\n', '<br>'), unsafe_allow_html=True)
                else:
                    st.error("Could not generate takeaways for the selected articles.")
        else:
            st.warning("No articles match the current filters. Please adjust your filters to generate takeaways.")
