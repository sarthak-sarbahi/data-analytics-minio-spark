# !pip install minio
# !pip install s3fs

from minio import Minio
import pandas as pd
import s3fs

minioClient = Minio('minio1:9000',
                    access_key='minio',
                    secret_key='minio123',
                    secure=False)  # Use secure=True for HTTPS

buckets = minioClient.list_buckets()
for bucket in buckets:
    print(bucket.name)

objects = minioClient.list_objects('mybucket')
for obj in objects:
    print(obj.object_name)

objects = minioClient.list_objects('mybucket')
data = [{'Name': obj.object_name, 'Size': obj.size, 'Last Modified': obj.last_modified} for obj in objects]
df = pd.DataFrame(data)
df.head()

fs = s3fs.S3FileSystem(
    client_kwargs={'endpoint_url': 'http://minio1:9000'},
    key='minio',
    secret='minio123',
    use_ssl=False  # Set to True if MinIO is set up with SSL
)
with fs.open('mybucket/data_pd.csv', 'w') as f:
    df.to_csv(f, index=False)
