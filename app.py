from google.cloud import bigquery
import pandas as pd
import streamlit as st
from src.CallDetail import CallDetail
from src.utils import (
    parse_phone_number,
    parse_iso_datetime,
    parse_time_duration,
    parse_call_memo,
    classify_number,
    call_hash,
    format_datetime_as_human_readable,
    format_timedelta,
    format_username,
)

client = bigquery.Client()

st.title("Call Charge Dashboard")

tenant_name = st.text_input("Tenant Name (e.g., tenant-id)")
col1, col2 = st.columns(2)
with col1:
    start_date = st.date_input("Start Date")
with col2:
    end_date = st.date_input("End Date")

st.subheader("Rate Configuration")
default_rate = st.number_input("Default Rate", min_value=0.0)
other_rate = st.number_input("Other Rate", min_value=0.0)

calculate = st.button("Calculate")

if calculate:
    # quick validation
    check_sql = """
        SELECT 1
        FROM `revcomm-data-warehouse.CallCharge_local_v_daily_call_charges_jakarta_joined`
        WHERE tenant_name = @tenant_name
        LIMIT 1
    """
    job_cfg = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("tenant_name", "STRING", tenant_name)]
    )
    exists = client.query(check_sql, job_config=job_cfg).to_dataframe()

    if exists.empty:
        st.warning("Tenant name not found. Please check spelling.")
    else:
        main_sql = """
            SELECT *
            FROM `revcomm-data-warehouse.CallCharge_local_v_daily_call_charges_jakarta_joined`
            WHERE tenant_name = @tenant_name
            AND DATE(dial_starts_at) BETWEEN @start AND @end
        """
        params = [
            bigquery.ScalarQueryParameter("tenant_name", "STRING", tenant_name),
            bigquery.ScalarQueryParameter("start", "DATE", start_date),
            bigquery.ScalarQueryParameter("end", "DATE", end_date),
        ]
        df = client.query(main_sql, job_config=bigquery.QueryJobConfig(query_parameters=params)).to_dataframe()
        st.success(f"Fetched {len(df)} rows.")

processed = []
for _, row in df.iterrows():
    detail = CallDetail(row.to_dict())
    # use your helper functions to compute charges etc.
    processed.append(detail.calculate())  # or however your class outputs data

result_df = pd.DataFrame(processed)
st.dataframe(result_df)
st.download_button("Download CSV", result_df.to_csv(index=False), "results.csv")

