import io, os
from urllib.parse import urlparse
from minio import Minio
import pandas as pd

S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL", "http://minio.mlops-lab.svc.cluster.local:9000")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "minio")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minio123")

def _client() -> Minio:
    u = urlparse(S3_ENDPOINT_URL)
    return Minio(
        u.netloc,
        access_key=AWS_ACCESS_KEY_ID,
        secret_key=AWS_SECRET_ACCESS_KEY,
        secure=(u.scheme == "https"),
    )

def ensure_bucket(bucket: str):
    c = _client()
    if not c.bucket_exists(bucket):
        c.make_bucket(bucket)

def put_bytes(bucket: str, key: str, data: bytes, content_type="application/octet-stream"):
    ensure_bucket(bucket)
    _client().put_object(bucket, key, io.BytesIO(data), len(data), content_type=content_type)

def put_df(bucket: str, key: str, df: pd.DataFrame):
    ensure_bucket(bucket)
    buf = io.BytesIO()
    if key.endswith(".parquet"):
        df.to_parquet(buf, index=False)
        ct = "application/octet-stream"
    else:
        df.to_csv(buf, index=False)
        ct = "text/csv"
    put_bytes(bucket, key, buf.getvalue(), ct)

def get_df(s3_uri: str) -> pd.DataFrame:
    u = urlparse(s3_uri)
    bucket, key = u.netloc, u.path.lstrip("/")
    resp = _client().get_object(bucket, key)
    data = resp.read()
    resp.close(); resp.release_conn()
    if key.endswith(".parquet"):
        return pd.read_parquet(io.BytesIO(data))
    return pd.read_csv(io.BytesIO(data))

def parse_s3(uri: str):
    u = urlparse(uri)
    return u.netloc, u.path.lstrip("/")
