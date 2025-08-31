import os
import flask
from google.ads.googleads.client import GoogleAdsClient
from google.cloud import bigquery
import yaml

# --- Configuration ---
# In Cloud Run, these will be set as environment variables.
# For local testing, you can uncomment and set them here.
# os.environ["GCP_PROJECT"] = "your-gcp-project-id"
# os.environ["BQ_DATASET"] = "google_ads_data"
# os.environ["BQ_TABLE"] = "p_CampaignStats"
# os.environ["ADS_CUSTOMER_ID"] = "customer-id-to-pull-data-from"
# os.environ["ADS_CONFIG_STRING"] = yaml.dump({ ... your config ...}) # See deployment step

GCP_PROJECT = os.environ.get("GCP_PROJECT")
BQ_DATASET = os.environ.get("BQ_DATASET")
BQ_TABLE = os.environ.get("BQ_TABLE")
ADS_CUSTOMER_ID = os.environ.get("ADS_CUSTOMER_ID") # The account you want data from
ADS_CONFIG_STRING = os.environ.get("ADS_CONFIG_STRING")

# Initialize the BigQuery Client
bq_client = bigquery.Client(project=GCP_PROJECT)
app = flask.Flask(__name__)

@app.route("/run", methods=["POST"])
def run_job():
    """Main function triggered by a POST request."""
    request_json = flask.request.get_json(silent=True)

    if not request_json or 'start_date' not in request_json or 'end_date' not in request_json:
        return ("Invalid request: JSON payload with 'start_date' and 'end_date' is required.", 400)

    start_date = request_json['start_date']
    end_date = request_json['end_date']
    
    print(f"Starting job for customer {ADS_CUSTOMER_ID} from {start_date} to {end_date}")

    try:
        # 1. Fetch data from Google Ads API
        ads_data = fetch_google_ads_data(start_date, end_date)
        if not ads_data:
            print("No data returned from Google Ads.")
            return ("Successfully completed. No data to load.", 200)

        # 2. Load data into BigQuery
        load_data_to_bigquery(ads_data)
        
        print(f"Successfully loaded {len(ads_data)} rows into BigQuery.")
        return (f"Success: Loaded {len(ads_data)} rows.", 200)

    except Exception as e:
        print(f"Error during job execution: {e}")
        return (f"An error occurred: {e}", 500)

def fetch_google_ads_data(start_date, end_date):
    """Queries the Google Ads API for campaign stats."""
    
    # Load configuration from the environment variable string
    config = yaml.safe_load(ADS_CONFIG_STRING)
    google_ads_client = GoogleAdsClient.load_from_dict(config)
    
    ga_service = google_ads_client.get_service("GoogleAdsService")

    # This is the Google Ads Query Language (GAQL) query
    query = f"""
        SELECT
            campaign.id,
            segments.date,
            metrics.cost_micros,
            segments.device,
            segments.ad_network_type,
            customer.id
        FROM campaign
        WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
    """

    stream = ga_service.search_stream(customer_id=ADS_CUSTOMER_ID, query=query)
    
    rows_to_insert = []
    for batch in stream:
        for row in batch.results:
            # IMPORTANT: Cost is in micros. Convert it to standard currency units.
            cost = row.metrics.cost_micros / 1_000_000
            
            # Format the data for BigQuery
            rows_to_insert.append({
                "event_date": row.segments.date,
                "external_customer_id": row.customer.id,
                "campaign_id": row.campaign.id,
                "ad_network_type": row.segments.ad_network_type.name,
                "device": row.segments.device.name,
                "cost": cost
            })
    return rows_to_insert


def load_data_to_bigquery(rows):
    """Loads a list of dictionaries into a BigQuery table."""
    table_id = f"{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    
    errors = bq_client.insert_rows_json(table_id, rows)
    if errors:
        print(f"Encountered errors while inserting rows: {errors}")
        raise Exception(f"BigQuery load job failed: {errors}")


if __name__ == "__main__":
    # Used for local testing. In Cloud Run, a production server like Gunicorn is used.
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
