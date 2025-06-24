import os
import glob
from google.oauth2 import service_account     # ← add this
from google.cloud import bigquery

# point to your service account key
KEY_PATH = "credentials/gcp-project.json"
# folder containing your three .sql files
SQL_FOLDER = "sql/trusted"
# your project id
PROJECT_ID = "gcp-project-463802"

def run_sql_scripts(project_id: str, sql_folder: str):
    # load credentials
    creds = service_account.Credentials.from_service_account_file(KEY_PATH)
    client = bigquery.Client(project=project_id, credentials=creds)

    sql_paths = sorted(glob.glob(os.path.join(sql_folder, "*.sql")))
    for path in sql_paths:
        table_name = os.path.splitext(os.path.basename(path))[0]
        print(f"▶ Running `{path}` → rebuilding `{project_id}.trusted.{table_name}`")
        with open(path, "r") as f:
            sql = f.read()
        job = client.query(sql)
        job.result()
        print(f"✔ `{table_name}` complete\n")

if __name__ == "__main__":
    run_sql_scripts(PROJECT_ID, SQL_FOLDER)
