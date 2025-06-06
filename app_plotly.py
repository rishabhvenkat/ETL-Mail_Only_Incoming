import streamlit as st
import pandas as pd
import plotly.express as px
from db_utils import get_folders, get_email_data, get_response_data

st.title("Email Analytics Dashboard")

# Sidebar
folders = get_folders()
selected_folder = st.selectbox("Select Folder", folders)

# Fetch Data
email_df = get_email_data(selected_folder)
response_df = get_response_data()

# Convert date/time
email_df['received_date'] = pd.to_datetime(email_df['received_date'])

# Date Range Filter
st.subheader("Filter by Date Range")
min_date = email_df['received_date'].min().date()
max_date = email_df['received_date'].max().date()

start_date, end_date = st.date_input(
    "Select date range:",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date
)

# Filter email data by selected date range
email_df = email_df[(email_df['received_date'].dt.date >= start_date) & (email_df['received_date'].dt.date <= end_date)]

# Folder comparison multiselect
st.subheader("Compare Email Volume by Folder")

selected_folders_to_compare = st.multiselect(
    "Select folders to compare", folders, default=[selected_folder] if selected_folder else None
)

if selected_folders_to_compare:
    email_filtered = email_df[email_df['folder'].isin(selected_folders_to_compare)]
    email_filtered['date'] = email_filtered['received_date'].dt.date

    volume_comparison = (
        email_filtered.groupby(['date', 'folder'])
        .size()
        .reset_index(name='Email Count')
    )

    fig = px.line(
        volume_comparison,
        x='date',
        y='Email Count',
        color='folder',
        markers=True,
        title="Email Volume Comparison by Folder Over Time"
    )
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("Select at least one folder to visualize comparison.")

# Count by Date
count_by_date = email_df.groupby(email_df['received_date'].dt.date).size().reset_index(name='Email Count')
st.subheader(f"Email Count by Date - {selected_folder}")
st.bar_chart(count_by_date.set_index('received_date'))

# Count per folder by date
st.subheader("Email Count per Folder by Date")
folder_counts = email_df.groupby(['folder', email_df['received_date'].dt.date]).size().unstack().fillna(0)
st.line_chart(folder_counts.T)

# Merge for Response Analytics
merged = pd.merge(email_df, response_df, on='thread_id', suffixes=('', '_resp'))
merged['response_delay'] = merged['response_time_seconds'] / 60

# Response Rate
if not email_df.empty:
    response_rate = len(merged['email_id'].unique()) / len(email_df['email_id'].unique()) * 100
    st.metric(label="Response Rate (%)", value=f"{response_rate:.2f}")
else:
    st.warning("No emails in selected date range for response rate calculation.")

# Avg Response Time
if not merged.empty:
    avg_resp_time = merged['response_delay'].mean()
    st.metric(label="Avg. Response Time (minutes)", value=f"{avg_resp_time:.1f}")
else:
    st.warning("No responded emails in selected range for average response time.")

# % Email response by folder
st.subheader("Response Rate by Folder")
response_rate_by_folder = merged.groupby('folder').size() / email_df.groupby('folder').size() * 100
st.dataframe(response_rate_by_folder.reset_index(name="Response Rate (%)"))

# Response Rate by Folder by Date
st.subheader("Response Rate by Folder by Date")

email_df['date'] = email_df['received_date'].dt.date
merged['date'] = merged['received_date'].dt.date

# Total emails per folder per date
total_emails_by_folder_date = email_df.groupby(['folder', 'date']).size().rename("Total Emails")

# Responded emails per folder per date
responded_emails_by_folder_date = merged.groupby(['folder', 'date']).size().rename("Responded Emails")

# Combine and calculate response rate
response_rate_by_folder_date = pd.concat([total_emails_by_folder_date, responded_emails_by_folder_date], axis=1).fillna(0)
response_rate_by_folder_date['Response Rate (%)'] = (
    response_rate_by_folder_date['Responded Emails'] / response_rate_by_folder_date['Total Emails'] * 100
)

# Display
st.dataframe(response_rate_by_folder_date.reset_index())

# Avg. Response Time by Folder by Date
st.subheader("Avg. Response Time (minutes) by Folder by Date")

avg_response_time_by_folder_date = merged.groupby(['folder', 'date'])['response_delay'].mean().reset_index()
avg_response_time_by_folder_date.rename(columns={'response_delay': 'Avg. Response Time (min)'}, inplace=True)

st.dataframe(avg_response_time_by_folder_date)

