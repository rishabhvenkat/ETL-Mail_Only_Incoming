import streamlit as st
import pandas as pd
from db_utils import get_folders, get_email_data, get_response_data

st.title("📬 Email Analytics Dashboard")

# Sidebar
folders = get_folders()
selected_folder = st.selectbox("Select Folder", folders)

# Fetch Data
email_df = get_email_data(selected_folder)
response_df = get_response_data()

# Convert date/time
email_df['received_date'] = pd.to_datetime(email_df['received_date'])
response_df['response_date'] = pd.to_datetime(response_df['response_date'])

# Count by Date
count_by_date = email_df.groupby(email_df['received_date'].dt.date).size().reset_index(name='Email Count')
st.subheader(f"📅 Email Count by Date - {selected_folder}")
st.bar_chart(count_by_date.set_index('received_date'))

# Count per folder by date
st.subheader("🗂 Email Count per Folder by Date")
folder_counts = email_df.groupby(['folder', email_df['received_date'].dt.date]).size().unstack().fillna(0)
st.line_chart(folder_counts.T)

# Merge for Response Analytics
merged = pd.merge(email_df, response_df, on='thread_id', suffixes=('', '_resp'))
merged['response_delay'] = (merged['response_date'] - merged['received_date']).dt.total_seconds() / 60

# Response Rate
response_rate = len(merged['email_id'].unique()) / len(email_df['email_id'].unique()) * 100
st.metric(label="📬 Response Rate (%)", value=f"{response_rate:.2f}")

# Avg Response Time
avg_resp_time = merged['response_delay'].mean()
st.metric(label="⏱ Avg. Response Time (minutes)", value=f"{avg_resp_time:.1f}")

# % Email response by folder
st.subheader("📈 Response Rate by Folder")
response_rate_by_folder = merged.groupby('folder').size() / email_df.groupby('folder').size() * 100
st.dataframe(response_rate_by_folder.reset_index(name="Response Rate (%)"))

