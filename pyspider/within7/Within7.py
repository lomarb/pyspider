import json
import boto3
import gzip

with open('/opt/pyspider/access_key') as f:
    access_key = f.read()

with open('/opt/pyspider/secret_key') as f:
    secret_key = f.read()


class Within7ResultWorker:
    bucket_name = 'aws-glue-assets-080794739569-us-east-2'  # S3 桶的名称
    object_name = 'Test_ypp_20230621/Test_S3_V2/Test_Pyspider'  # 存储在 S3 中的对象名称（通常以 .json 结尾）

    s3_client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)

    def upload_data_to_s3(self, task, json_data):
        json_string = json.dumps(json_data)

        # 使用 gzip 进行压缩
        compressed_data = gzip.compress(json_string.encode('utf-8'))

        project = task['project']
        taskid = task['taskid']
        # 将压缩后的数据存储到 S3
        object_name = f"{self.object_name}/{project}/{taskid}.json.gz"
        try:
            response = self.s3_client.put_object(Body=compressed_data, Bucket=self.bucket_name, Key=object_name)
            print(response, type(response))
            print(f"压缩后的 JSON 数据已成功存储到 S3 桶 {self.bucket_name} 中，保存为对象 {object_name}")
        except Exception as e:
            print(f"存储压缩后的 JSON 数据时出现错误：{e}")
