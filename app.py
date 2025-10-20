import streamlit as st
import pandas as pd
from google.cloud import bigquery
import os, json

if "SERVICE_ACCOUNT_KEY" in os.environ:
    key_path = "/tmp/service_account.json"
    with open(key_path, "w") as f:
        f.write(os.environ["SERVICE_ACCOUNT_KEY"])
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path

# Optional: import your calculation modules
# from src.CallDetail import CallDetail
# from src.utils import some_helper_function

st.set_page_config(page_title="Call Charge Dashboard", layout="wide")

st.title("📞 Call Charge Dashboard (Jakarta Region)")

# --- Inputs ---
tenant_name = st.text_input("Tenant Name (e.g., tenant-id)").strip()
col1, col2 = st.columns(2)
with col1:
    start_date = st.date_input("Start Date")
with col2:
    end_date = st.date_input("End Date")

st.subheader("Rate Configuration")
default_rate = st.number_input("Default Rate", min_value=0.0, step=0.01)
other_rate = st.number_input("Other Rate", min_value=0.0, step=0.01)

calculate = st.button("Calculate")

# --- BigQuery Client ---
@st.cache_resource
def get_bq_client():
    return bigquery.Client()

client = get_bq_client()

# --- Main Action ---
if calculate:
    if not tenant_name:
        st.warning("Please enter a tenant name.")
        st.stop()

    st.info("Checking tenant...")

    check_sql = """
        SELECT 1
        FROM `revcomm-data-warehouse.CallCharge_local.v_daily_call_charges_jakarta_joined`
        WHERE tenant_name = @tenant_name
        LIMIT 1
    """
    job_cfg = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("tenant_name", "STRING", tenant_name)
        ]
    )
    exists = client.query(check_sql, job_config=job_cfg).to_dataframe()

    if exists.empty:
        st.error(f"Tenant '{tenant_name}' not found. Please check the name.")
        st.stop()

    st.success("Tenant verified. Fetching call data...")

    query = """
        SELECT *
        FROM `revcomm-data-warehouse.CallCharge_local.v_daily_call_charges_jakarta_joined`
        WHERE tenant_name = @tenant_name
          AND DATE(dial_starts_at) BETWEEN @start AND @end
    """
    params = [
        bigquery.ScalarQueryParameter("tenant_name", "STRING", tenant_name),
        bigquery.ScalarQueryParameter("start", "DATE", start_date),
        bigquery.ScalarQueryParameter("end", "DATE", end_date),
    ]

    df = client.query(query, job_config=bigquery.QueryJobConfig(query_parameters=params)).to_dataframe()

    if df.empty:
        st.warning("No call data found for that period.")
        st.stop()

    st.success(f"Loaded {len(df)} rows from BigQuery.")
    st.dataframe(df.head())

    # --- Processing placeholder ---
    st.subheader("Processing Call Charges...")

    # Example placeholder for your custom logic
    # processed = []
    # for _, row in df.iterrows():
    #     detail = CallDetail(row.to_dict())
    #     result = detail.calculate(default_rate, other_rate)
    #     processed.append(result)

    # processed_df = pd.DataFrame(processed)
    # st.dataframe(processed_df)

    # For now, just show the raw data until calculation logic is plugged in
    st.download_button("Download Raw Data CSV", df.to_csv(index=False), "call_data.csv")

st.caption("Powered by BigQuery • Streamlit • RevComm Data Warehouse")
