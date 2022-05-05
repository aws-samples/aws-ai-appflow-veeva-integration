# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#   Licensed under the Apache License, Version 2.0 (the "License").
#   You may not use this file except in compliance with the License.
#   A copy of the License is located at
#       http://www.apache.org/licenses/LICENSE-2.0
#   or in the "license" file accompanying this file. This file is distributed 
#   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either 
#   express or implied. See the License for the specific language governing 
#   permissions and limitations under the License.

import sys
import os
import json
from urllib.parse import unquote_plus
import datetime
import decimal
import boto3
import requests

sys.path.insert(0, '/opt')
sm_client = boto3.client('secretsmanager')

#read the environment variables
VEEVA_DOMAIN_NAME = sm_client.get_secret_value(SecretId = unquote_plus(os.environ['VEEVA_DOMAIN_NAME_SECRET']))['SecretString']
VEEVA_USERNAME = sm_client.get_secret_value(SecretId = unquote_plus(os.environ['VEEVA_DOMAIN_USERNAME_SECRET']))['SecretString']
VEEVA_PASSWORD = sm_client.get_secret_value(SecretId = unquote_plus(os.environ['VEEVA_DOMAIN_PASSWORD_SECRET']))['SecretString']
CUSTOM_PROPERTY_LABEL = sm_client.get_secret_value(SecretId = unquote_plus(os.environ['VEEVA_CUSTOM_FIELD_NAME_SECRET']))['SecretString']

VERSION = 'v20.1'

# Veeva URL formats.
auth_url = f'https://{VEEVA_DOMAIN_NAME}.veevavault.com/api/{VERSION}/auth'
data_url = f'https://{VEEVA_DOMAIN_NAME}.veevavault.com/api/{VERSION}/'
document_properties_url = f'https://{VEEVA_DOMAIN_NAME}.veevavault.com/api/{VERSION}/metadata/objects/documents/properties'
document_url = f'https://{VEEVA_DOMAIN_NAME}.veevavault.com/api/{VERSION}/objects/documents/'

s3 = boto3.client('s3')
sqs = boto3.resource('sqs')

# specify the runDate global variable so it is initialized when the Lambda environment is initialized.
# you can also use a dynamodb table to keep track of this date.
# we use this date to get all the changes for the first run and then just the delta.
runDate = datetime.datetime(1900, 1, 1) 

def lambda_handler(event, context):
    # attempt authentication with Veeva
    # https://developer.veevavault.com/api/20.1/#authentication
    response = requests.post(auth_url,  data = {'username':VEEVA_USERNAME, 'password': VEEVA_PASSWORD})
    response = response.json()

    if response['responseStatus'] == 'SUCCESS':
        print ('Authentication Successful.')
        session_id = response['sessionId']

        #authHeader would be needed for subsequent calls. 
        auth_header = {'Authorization': session_id}

        if custom_property_exists(CUSTOM_PROPERTY_LABEL, auth_header):
            push_tags(event, CUSTOM_PROPERTY_LABEL, auth_header)
        else:
            print (f'Custom field {CUSTOM_PROPERTY_LABEL} does not exist. Skipping.')

    else:
        print ('Authentication NOT Successful.')
        print (json.dumps(response))

    return 1

def push_tags(event, label, auth_header):

    tag_dictionary = {}
    count = 0

    for record in event['Records']:
        print(record)
        # Get the primary key for use as the Elasticsearch ID
        if record['eventName'] != 'REMOVE':
            document_id = record['dynamodb']['NewImage']['DocumentId']['N']
            if document_id not in tag_dictionary:
                tag_dictionary[document_id] = set()
            tag = record['dynamodb']['NewImage']['Tag']['S']
            confidence = decimal.Decimal(record['dynamodb']['NewImage']['Confidence']['N'])
            if confidence > 85:
                if 'Value' in record['dynamodb']['NewImage'].keys() :
                    value = record['dynamodb']['NewImage']['Value']['S']
                    if value != 'False':
                        tag_dictionary[document_id].add(tag + ':' + value)
                else:
                    tag_dictionary[document_id].add(tag)
        count += 1

    print(tag_dictionary)

    for (document_id, old_tags) in tag_dictionary.items():
        document = get_document(document_id, auth_header)
        custom_field_name = get_custom_field_name_based_on_label(label,auth_header)
        if custom_field_name in document:
            current_tags = set(document[custom_field_name].split(','))
        else:
            current_tags = set()
        new_tags = old_tags.union(current_tags)
        update_document(document_id, label, ','.join(new_tags), auth_header)

    print(str(count) + ' records processed.')

def custom_property_exists(label, auth_header):
    labels = list(map(lambda x:x['label'],get_properties(auth_header)))
    if label in labels:
        return True
    return False

def get_custom_field_name_based_on_label(label, auth_header):
    filtered_labels = list(filter(lambda x: 'label' in x.keys() and x['label']==label, get_properties(auth_header)))
    if len(filtered_labels)!=0:
        return filtered_labels[0]['name']
    else: 
        raise Exception('Custom label is not present.')

def get_properties(auth_header):
    veeva_document_properties = requests.get(document_properties_url, headers=auth_header)
    veeva_document_properties = veeva_document_properties.json()
    if veeva_document_properties['responseStatus'] == 'SUCCESS':
        return list(filter(lambda x: 'label' in x.keys(), veeva_document_properties['properties']))
    return []

def get_document(document_id, auth_header):
    veeva_document_response = requests.get(document_url + document_id, headers=auth_header)
    veeva_document_response = veeva_document_response.json()
    if veeva_document_response['responseStatus'] == 'SUCCESS':
        return veeva_document_response['document']

def update_document(document_id, field_label, field_value, auth_header):
    field_name = get_custom_field_name_based_on_label(field_label, auth_header)
    data = { field_name: field_value}
    veeva_document_update_response = requests.put(document_url + document_id, data= data, headers=auth_header)
    veeva_document_update_response = veeva_document_update_response.json()
    return veeva_document_update_response
