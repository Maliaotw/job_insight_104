import boto3
from botocore.exceptions import ClientError
import os
from config.settings import BASE_DIR, DATABASE_PROCESSED_DATA_PATH

access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
session_token = os.getenv('AWS_SESSION_TOKEN')
region = os.getenv('AWS_DEFAULT_REGION')

# 创建S3客户端
s3_client = boto3.client(
    's3',
    aws_access_key_id=access_key_id,
    aws_secret_access_key=secret_access_key,
    region_name=region
)

import boto3
from botocore.exceptions import ClientError


# 方法1：简单列出所有桶
def list_buckets():
    try:
        response = s3_client.list_buckets()
        buckets = response['Buckets']

        print("您的S3桶列表：")
        print("-" * 50)
        for bucket in buckets:
            print(f"桶名: {bucket['Name']}")
            print(f"创建时间: {bucket['CreationDate']}")
            print("-" * 50)

        return buckets
    except ClientError as e:
        print(f"列出桶时发生错误: {e}")
        return None



def get_bucket_details():
    try:
        response = s3_client.list_buckets()
        buckets = response['Buckets']

        print(f"账户下共有 {len(buckets)} 个S3桶：")
        print("=" * 80)

        for i, bucket in enumerate(buckets, 1):
            bucket_name = bucket['Name']
            creation_date = bucket['CreationDate']

            print(f"\n{i}. 桶名称: {bucket_name}")
            print(f"   创建时间: {creation_date}")

            # 获取桶的区域
            try:
                location_response = s3_client.get_bucket_location(Bucket=bucket_name)
                region = location_response['LocationConstraint']
                if region is None:
                    region = 'us-east-1'  # 默认区域
                print(f"   区域: {region}")
            except ClientError as e:
                print(f"   区域: 无法获取 ({e.response['Error']['Code']})")

            # 获取桶中对象数量（前1000个）
            try:
                objects_response = s3_client.list_objects_v2(Bucket=bucket_name, MaxKeys=1000)
                object_count = objects_response.get('KeyCount', 0)
                print(f"   对象数量: {object_count} (显示前1000个)")
            except ClientError as e:
                print(f"   对象数量: 无法获取 ({e.response['Error']['Code']})")

        print("=" * 80)

    except ClientError as e:
        print(f"获取桶详情时发生错误: {e}")




# 上传文件
def upload_file_to_s3(local_file_path, bucket_name, s3_key):
    """
    上传文件到S3

    Args:
        local_file_path: 本地文件路径
        bucket_name: S3桶名称
        s3_key: S3中的文件路径/键名
    """
    try:
        print(local_file_path)
        s3_client.upload_file(local_file_path, bucket_name, s3_key)
        print(f"文件 {local_file_path} 已成功上传到 s3://{bucket_name}/{s3_key}")
        return True
    except ClientError as e:
        print(f"上传失败: {e}")
        return False



if __name__ == '__main__':
    # 执行列出桶
    buckets = list_buckets()
    # # 执行获取详细信息
    # get_bucket_details()
    #
    # # 使用示例
    # upload_file_to_s3(
    #     local_file_path=DATABASE_PROCESSED_DATA_PATH,
    #     bucket_name='job-insight-104',
    #     s3_key='processed_job_data.duckdb'
    # )