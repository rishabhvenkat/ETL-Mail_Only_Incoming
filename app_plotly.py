import streamlit as st
import pandas as pd
import plotly.express as px
from db_utils import get_folders, get_email_data, get_response_data

st.title("Email Analytics Dashboard")

# Load folders and all email data
folders = get_folders()
all_email_df = get_email_data(None)  # Fetch all folders' emails
response_df = get_response_data()

# Convert to datetime
all_email_df['received_date'] = pd.to_datetime(all_email_df['received_date'])

# Sidebar folder selection
selected_folder = st.selectbox("Select Folder", folders)

# Date range filtering
st.subheader("Filter by Date Range")
min_date = all_email_df['received_date'].min().date()
max_date = all_email_df['received_date'].max().date()

start_date, end_date = st.date_input(
    "Select date range:",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date
)

# Apply date filter globally
all_email_df = all_email_df[
    (all_email_df['received_date'].dt.date >= start_date) &
    (all_email_df['received_date'].dt.date <= end_date)
]

# Filter for selected folder
email_df = all_email_df[all_email_df['folder'] == selected_folder]

# Compare Email Volume by Folder (Bar Chart)
st.subheader("Compare Email Volume by Folder")

selected_folders_to_compare = st.multiselect(
    "Select folders to compare", folders, default=[selected_folder]
)

if selected_folders_to_compare:
    comparison_df = all_email_df[all_email_df['folder'].isin(selected_folders_to_compare)]
    comparison_df['date'] = comparison_df['received_date'].dt.date

    volume_comparison = (
        comparison_df.groupby(['date', 'folder'])
        .size()
        .reset_index(name='Email Count')
    )

    fig = px.bar(
        volume_comparison,
        x='date',
        y='Email Count',
        color='folder',
        barmode='relative',
        title="Email Volume Comparison by Folder Over Time"
    )
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("Select at least one folder to visualize comparison.")

# Email Count by Date (Selected Folder)
count_by_date = email_df.groupby(email_df['received_date'].dt.date).size().reset_index(name='Email Count')
st.subheader(f"Email Count by Date - {selected_folder}")
fig_date = px.bar(count_by_date, x='received_date', y='Email Count', title=f"Email Count by Date - {selected_folder}")
st.plotly_chart(fig_date, use_container_width=True)

# Email Count per Folder by Date
st.subheader("Email Count per Folder by Date - All Folders")
folder_counts = (
    all_email_df.groupby([all_email_df['received_date'].dt.date, 'folder'])
    .size()
    .reset_index(name='Email Count')
)
fig_folder = px.bar(
    folder_counts,
    x='received_date',
    y='Email Count',
    color='folder',
    barmode='relative',
    title="Email Count per Folder by Date (Comparison)"
)
st.plotly_chart(fig_folder, use_container_width=True)

# Merge for Response Analytics
merged = pd.merge(email_df, response_df, on='thread_id', suffixes=('', '_resp'))
merged['response_delay'] = merged['response_time_seconds'] / 60
merged['date'] = merged['received_date'].dt.date
email_df['date'] = email_df['received_date'].dt.date

# Response Rate Metric
if not email_df.empty:
    response_rate = len(merged['email_id'].unique()) / len(email_df['email_id'].unique()) * 100
    st.metric(label="Response Rate (%)", value=f"{response_rate:.2f}")
else:
    st.warning("No emails in selected date range for response rate calculation.")

# Average Response Time Metric
if not merged.empty:
    avg_resp_time = merged['response_delay'].mean()
    st.metric(label="Avg. Response Time (minutes)", value=f"{avg_resp_time:.1f}")
else:
    st.warning("No responded emails in selected range for average response time.")

# Response Rate by Folder
st.subheader(" Avg. Response Rate by Folder")
response_rate_by_folder = merged.groupby('folder').size() / all_email_df.groupby('folder').size() * 100
response_rate_by_folder = response_rate_by_folder.reset_index(name="Response Rate (%)")
fig_response_folder = px.bar(
    response_rate_by_folder,
    x='folder',
    y='Response Rate (%)',
    title=" Avg. Response Rate by Folder (Bar plot)"
)
st.plotly_chart(fig_response_folder, use_container_width=True)

# Response Rate by Folder by Date
st.subheader("Avg. Response Rate by Folder by Date")
total_emails = email_df.groupby(['folder', 'date']).size().rename("Total Emails")
responded_emails = merged.groupby(['folder', 'date']).size().rename("Responded Emails")

response_rate_by_folder_date = pd.concat([total_emails, responded_emails], axis=1).fillna(0)
response_rate_by_folder_date['Response Rate (%)'] = (
    response_rate_by_folder_date['Responded Emails'] / response_rate_by_folder_date['Total Emails'] * 100
)
response_rate_by_folder_date = response_rate_by_folder_date.reset_index()

fig_response_trend = px.bar(
    response_rate_by_folder_date,
    x='date',
    y='Response Rate (%)',
    color='folder',
    barmode='relative',
    title="Avg. Response Rate by Folder by Date (Bar plot)"
)
st.plotly_chart(fig_response_trend, use_container_width=True)

# Avg. Response Time by Folder by Date
st.subheader("Avg. Response Time (minutes) by Folder by Date")
avg_response_time = merged.groupby(['folder', 'date'])['response_delay'].mean().reset_index()
avg_response_time.rename(columns={'response_delay': 'Avg. Response Time (min)'}, inplace=True)

fig_avg_resp = px.bar(
    avg_response_time,
    x='date',
    y='Avg. Response Time (min)',
    color='folder',
    barmode='relative',
    title="Avg. Response Time by Folder by Date (Bar plot)"
)
st.plotly_chart(fig_avg_resp, use_container_width=True)

# Responded  Emails Count by Folder by Date
st.subheader("Responded Emails per Folder by Date")

responded_counts = (
    merged.groupby(['date', 'folder'])
    .size()
    .reset_index(name='Responded Emails')
)

fig_responded_counts = px.bar(
    responded_counts,
    x = 'date',
    y = 'Responded Emails',
    color = 'folder',
    barmode = 'relative',
    title = 'Responded Emails per Folder by Date (Bar plot)'
)

st.plotly_chart(fig_responded_counts, use_container_width = True)
