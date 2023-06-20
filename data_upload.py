from google.cloud import storage

def upload_to_bucket(bucket_name: str, local_fp: str, destination_fp: str):
    client = storage.Client()

    bucket = client.get_bucket(bucket_name)

    blob = bucket.blob(destination_fp)

    blob.upload_from_filename(local_fp)

