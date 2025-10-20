import streamlit as st
import pandas as pd
import json
from google.oauth2 import service_account
from google.cloud import bigquery
import streamlit as st

creds_dict = {
    "type": st.secrets["GCP_TYPE"],
    "project_id": st.secrets["GCP_PROJECT_ID"],
    "private_key_id": st.secrets["GCP_PRIVATE_KEY_ID"],
    "private_key": st.secrets["GCP_PRIVATE_KEY"],
    "client_email": st.secrets["GCP_CLIENT_EMAIL"],
    "client_id": st.secrets["GCP_CLIENT_ID"],
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": f"https://www.googleapis.com/robot/v1/metadata/x509/{st.secrets['GCP_CLIENT_EMAIL']}"
}

st.write("creds_dict project_id:", creds_dict["project_id"])

credentials = service_account.Credentials.from_service_account_info(creds_dict)
st.write("credentials.project_id:", credentials.project_id)

client = bigquery.Client(
    credentials=credentials,
    project=creds_dict["project_id"]
)

datasets = list(client.list_datasets())
st.write("datasets:", [d.dataset_id for d in datasets])

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
    creds_dict = {
        "type": st.secrets["GCP_TYPE"],
        "project_id": st.secrets["GCP_PROJECT_ID"],
        "private_key_id": st.secrets["GCP_PRIVATE_KEY_ID"],
        "private_key": st.secrets["GCP_PRIVATE_KEY"],
        "client_email": st.secrets["GCP_CLIENT_EMAIL"],
        "client_id": st.secrets["GCP_CLIENT_ID"],
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": f"https://www.googleapis.com/robot/v1/metadata/x509/{st.secrets['GCP_CLIENT_EMAIL']}"
    }

    credentials = service_account.Credentials.from_service_account_info(creds_dict)
    project_id = st.secrets["GCP_PROJECT_ID"]
    return bigquery.Client(credentials=credentials, project=project_id)

client = get_bq_client()
st.write("✅ Connected to BigQuery project:", client.project)

# --- Main Action ---
if calculate:
    if not tenant_name:
        st.warning("Please enter a tenant name.")
        st.stop()

    st.info("Checking tenant...")

    # Parameterized tenant check
    check_sql = """
        SELECT 1
        FROM `ultra-concord-475707-a7.CallCharge_local_v_daily_call_charges_jakarta_joined.CallCharge_local`
        WHERE name = @tenant_name
        LIMIT 1
    """
    tenant_params = [
        bigquery.ScalarQueryParameter("tenant_name", "STRING", tenant_name)
    ]
    exists = client.query(
        check_sql,
        job_config=bigquery.QueryJobConfig(query_parameters=tenant_params)
    ).to_dataframe()
    
    if exists.empty:
        st.error(f"Tenant '{tenant_name}' not found. Please check the name.")
        st.stop()
    
    st.success("Tenant verified. Fetching call data...")

    # Parameterized call data query
    region = "jkt"  # you can later make this dynamic if needed
    query = """
        SELECT
          tenant_id,
          name AS tenant_name,
          call_type,
          duration_of_call_sec,
          duration_of_call_sec_str,
          all_duration_of_call_sec_str,
          dial_starts_at,
          dial_answered_at,
          dial_ends_at,
          call_to,
          call_from,
          number_type,
          call_id
        FROM `ultra-concord-475707-a7.CallCharge_local_v_daily_call_charges_jakarta_joined.CallCharge_local`
        WHERE pbx_region = jkt
    """
    query_params = [
        bigquery.ScalarQueryParameter("region", "STRING", region)
    ]
    job_cfg = bigquery.QueryJobConfig(query_parameters=query_params)

    df = client.query(query, job_config=job_cfg).to_dataframe()

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
