import json
import boto3
import urllib

s3 = boto3.resource('s3')
s3_client = boto3.client('s3')


def lambda_handler(event, context):
    # docstring for lambda_handler"event, context
    bucket_list = []
    print(event)
    bucket_details = event['Records'][0]['s3']
    bucket_name = bucket_details['bucket']['name']
    key = urllib.parse.unquote_plus(bucket_details['object']['key'], encoding='utf-8')
    print("File name : {} \nBucket name : {}".format(key, bucket_name))
    # print("Bucket name {}".format(bucket_name))
    response = s3_client.get_object(Bucket=bucket_name, Key=key)
    print("Response : {}".format(response))
    content = response['Body'].read().decode()
    file_size = response['ContentLength']
    contents = json.loads(content)
    print("File name : {}\nFile size : {}".format(key, file_size))
    for bucket in s3.buckets.all():
        print(bucket.name)
        bucket_list.append(bucket.name)

    return {
        "status_code": 200,
        "body": bucket_list
    }