import sys
import os

import duckdb

with open(sys.argv[1]) as f:
    sql = f.read()

sql = f"""
CREATE OR REPLACE SECRET secret (
    TYPE s3,
    PROVIDER config,
    ENDPOINT 'fsn1.your-objectstorage.com',
    KEY_ID '{os.environ["AWS_ACCESS_KEY_ID"]}',
    SECRET '{os.environ["AWS_SECRET_ACCESS_KEY"]}',
    REGION 'fsn1'
);

{sql}
"""

print(duckdb.sql(sql))
