"""
Script processes the data into aggregated datasets and uploads them to S3.
"""
import os

import duckdb
import boto3.session


TASKS = {
    "legislativas2022": {
        "path": "processed/legislativas2022/v1/all.json",
        "sql": "etl/legislativas2022.sql",
    },
    "legislativas2025": {
        "path": "processed/legislativas2025/v1/all.json",
        "sql": "etl/legislativas2025.sql",
    },
    "autarquicas2025": {
        "path": "processed/autarquicas2025/v1/all.json",
        "sql": "etl/autarquicas2025.sql",
    },
    "all": {
        "path": "processed/all/v1/all.csv",
        "sql": "etl/all.sql",
    }
}
DEPENDENCIES = {
    "all": {"legislativas2025", "autarquicas2025"},
}


def _execute(s3_client, task: str):
    print(f"Executing task: {task}")
    for dependency in DEPENDENCIES.get(task, set()):
        _execute(s3_client, dependency)

    path = TASKS[task]["path"]
    os.makedirs(os.path.dirname(f"database/{path}"), exist_ok=True)
    with open(TASKS[task]["sql"]) as f:
        sql = f.read()
    sql = f"""
SET s3_endpoint='fsn1.your-objectstorage.com';
SET s3_region='fsn1';

COPY ({sql}) TO 'database/{path}';
"""
    duckdb.sql(sql)
    s3_client.upload_file(f"database/{path}", "pt-elections", path, ExtraArgs={'ACL':'public-read', 'ContentType': 'text/csv'})


if __name__ == "__main__":
    s3_client = boto3.session.Session().client(
        service_name="s3",
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name="fsn1",
        endpoint_url="https://fsn1.your-objectstorage.com",
    )

    _execute(s3_client, "all")
