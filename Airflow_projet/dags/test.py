from airflow.providers.amazon.aws.hooks.s3 import S3Hook

hook = S3Hook(aws_conn_id='minio_s3')
hook.load_file("/tmp/produits_20260123_115522.csv", bucket_name="datalake-bronze", key="produits/test.csv", replace=True)
print(hook.get_bucket_names())
