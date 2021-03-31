from typing import Optional
from google.cloud.storage import Client  # type: ignore


class GcsClient:
    def __init__(self, bucket=None):
        self.client = Client()
        self.bucket = bucket and self.client.get_bucket(bucket) or None

    def upload_json(self, json: str, path: str) -> Optional[str]:

        if self.bucket is None:
            raise IOError("GCS bucket not available, skipping")

        blob = self.bucket.blob(path)
        return blob.upload_from_string(json, content_type="application/json")
