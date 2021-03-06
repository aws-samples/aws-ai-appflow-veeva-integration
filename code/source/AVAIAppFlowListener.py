# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#   Licensed under the Apache License, Version 2.0 (the "License").
#   You may not use this file except in compliance with the License.
#   A copy of the License is located at
#       http://www.apache.org/licenses/LICENSE-2.0
#   or in the "license" file accompanying this file. This file is distributed 
#   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either 
#   express or implied. See the License for the specific language governing 
#   permissions and limitations under the License.

import os
import json
import uuid
from datetime import datetime
from urllib.parse import unquote_plus
import boto3

#read the environment variables
queueName = unquote_plus(os.environ['QUEUE_NAME'])

s3 = boto3.client('s3')
sqs = boto3.resource('sqs')
queue = sqs.get_queue_by_name(QueueName=queueName)

SUCCESFUL_STATUS = "Execution Successful"
ACCEPTED_FORMATS = ['image/jpeg', 'image/png', 'application/pdf', 'audio/mp3']

def lambda_handler(event, context):

    print(event)

    status = event['detail']['status']

    if status==SUCCESFUL_STATUS:
        bucket = event['detail']['destination-object'].split('//')[1].split('/')[0]
        
        flow_name = event['detail']['flow-name']
        prefix_part = event['detail']['destination-object'].split('//')[1].split('/')[1]
        start_time= datetime.strptime(event['detail']['start-time'].split('.')[0],"%Y-%m-%dT%H:%M:%S")
        execution_id = event['detail']['execution-id']
        prefix = prefix_part + '/' + flow_name + '/' + str(start_time.year) + '/' + str(start_time.month).zfill(2) + '/' + str(start_time.day).zfill(2) + '/' + str(start_time.hour).zfill(2) + '/' + execution_id

        print(prefix)

        s3_object_keys = get_all_keys(bucket, prefix)

        s3_meta_key = s3_object_keys.pop(0)

        print(s3_meta_key)

        meta_file_content = s3.get_object(Bucket=bucket, Key=s3_meta_key)['Body'].read().decode('utf-8')
        meta_file_json_content = json.loads(meta_file_content)['data']
        for document in meta_file_json_content:
            push_to_queue(bucket, s3_object_keys, document)
    else:
        print("AppFlow Run not succesful. Skipping.")

    return 1

def get_all_keys(bucket, prefix):
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

    all_keys = []
    for page in pages:
        for obj in page['Contents']:
            all_keys.append(obj['Key'])

    return all_keys


def push_to_queue(bucket, keys, document):
    if document['format__v'] in ACCEPTED_FORMATS:
        document_key = list(filter(lambda x: partial_document_prefix(document) in x, keys)).pop()
        print(f"Pushing document ID {document['id']} with filename {document['filename__v']} to SQS")
        message = {}
        message['documentId'] = document['id']
        message['fileType'] = 'png'
        message['bucketName'] = bucket
        message['keyName'] = document_key

        queue.send_message(MessageBody= json.dumps(message), MessageGroupId='messageGroup1', MessageDeduplicationId = str(uuid.uuid4()))
    else:
        print ("Unsupported format:" + document['format__v'] + ". Skipping.")
def partial_document_prefix(document):
    return str(document['id']) + '/' + str(document['major_version_number__v'])+'_'+str(document['minor_version_number__v']) + '/' + document['filename__v']