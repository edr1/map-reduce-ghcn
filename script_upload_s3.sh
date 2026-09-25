# 1. Explicitly create the target folder structure inside your S3 bucket
hdfs dfs -mkdir -p s3://dataset-ghcn/input/

# 2. Run the upload command again now that the path exists
hdfs dfs -put /home/ec2-user/datasets/urls_01/*.csv s3://dataset-ghcn/input/
