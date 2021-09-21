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
sys.path.insert(0, '/opt')
import boto3
import requests
from requests.auth import HTTPBasicAuth
from urllib.parse import unquote_plus
import urllib
import json
import os
import uuid
import datetime
import decimal

#read the environment variables
veevaDomainName = unquote_plus(os.environ['VEEVA_DOMAIN_NAME']) 


veevaUserName = unquote_plus(os.environ['VEEVA_DOMAIN_USERNAME']) 


veevaPassword = unquote_plus(os.environ['VEEVA_DOMAIN_PASSWORD']) 

customPropertyLabel = unquote_plus(os.environ['VEEVA_CUSTOM_FIELD_NAME']) 


version = 'v20.1'


# Veeva URL formats.
authUrl = f'https://{veevaDomainName}.veevavault.com/api/{version}/auth'
dataUrl = f'https://{veevaDomainName}.veevavault.com/api/{version}/'
documentPropertiesUrl = f'https://{veevaDomainName}.veevavault.com/api/{version}/metadata/objects/documents/properties'
documentUrl = f'https://{veevaDomainName}.veevavault.com/api/{version}/objects/documents/'

s3 = boto3.client('s3')
sqs = boto3.resource('sqs')

# specify the runDate global variable so it is initialized when the Lambda environment is initialized.
# you can also use a dynamodb table to keep track of this date.
# we use this date to get all the changes for the first run and then just the delta.
runDate = datetime.datetime(1900, 1, 1) 

def lambda_handler(event, context):
    # attempt authentication with Veeva
    # https://developer.veevavault.com/api/20.1/#authentication
    response = requests.post(authUrl,  data = {'username':veevaUserName, 'password': veevaPassword})
    response = response.json()

    if(response['responseStatus'] == 'SUCCESS'):
        print ('Authentication Successful.')
        sessionId = response['sessionId']
        
        #authHeader would be needed for subsequent calls. 
        authHeader = {'Authorization': sessionId}

        if custom_property_exists(customPropertyLabel, authHeader):
            push_tags(event, customPropertyLabel, authHeader)
        else:
            print (f'Custom field {customPropertyLabel} does not exist. Skipping.')

    else:
        print ('Authentication NOT Successful.')
        print (json.dumps(response))

    return 1

def push_tags(event, label, authHeader):
 
    tagDictionary = {}
    count = 0

    for record in event['Records']:
        print(record)
        # Get the primary key for use as the Elasticsearch ID
        if record['eventName'] != 'REMOVE':
            documentId = record['dynamodb']['NewImage']['DocumentId']['N']
            if documentId not in tagDictionary.keys():
                tagDictionary[documentId] = set()
            tag = record['dynamodb']['NewImage']['Tag']['S']
            confidence = decimal.Decimal(record['dynamodb']['NewImage']['Confidence']['N'])
            if confidence > 0.9:
                if 'Value' in record['dynamodb']['NewImage'].keys():
                    value = record['dynamodb']['NewImage']['Value']['S']
                    tagDictionary[documentId].add(tag + ':' + value)
                else:
                    tagDictionary[documentId].add(tag)
        count += 1
    
    for documentId in tagDictionary.keys():
        document = get_document(documentId, authHeader)
        custom_field_name = get_custom_field_name_based_on_label(label,authHeader)
        if custom_field_name in document.keys():
            current_tags = set(document[custom_field_name].split(','))
        else:
            current_tags = set()
        new_tags = tagDictionary[documentId].union(current_tags)
        update_document(documentId, label, ','.join(new_tags), authHeader)

    print(str(count) + ' records processed.')

def custom_property_exists(label, authHeader):    
    labels = list(map(lambda x:x['label'],get_properties(authHeader)))
    if label in labels:
        return True
    return False

def get_custom_field_name_based_on_label(label, authHeader):
    filtered_labels = list(filter(lambda x: 'label' in x.keys() and x['label']==label, get_properties(authHeader)))
    if (len(filtered_labels)!=0):
        return filtered_labels[0]['name']
    else: 
        raise Exception('Custom label is not present.')

def get_properties(authHeader):
    veevaDocumentProperties = requests.get(documentPropertiesUrl, headers=authHeader)
    veevaDocumentProperties = veevaDocumentProperties.json()
    if (veevaDocumentProperties['responseStatus'] == 'SUCCESS'):
        return list(filter(lambda x: 'label' in x.keys(), veevaDocumentProperties['properties']))
    return []

def get_document(documentId, authHeader):
    veevaDocumentResponse = requests.get(documentUrl + documentId, headers=authHeader)
    veevaDocumentResponse = veevaDocumentResponse.json()
    if (veevaDocumentResponse['responseStatus'] == 'SUCCESS'):
        return veevaDocumentResponse['document']

def update_document(documentId, fieldLabel, fieldValue, authHeader):
    fieldName = get_custom_field_name_based_on_label(fieldLabel, authHeader)
    data = { fieldName: fieldValue}
    veevaDocumentUpdateResponse = requests.put(documentUrl + documentId, data= data, headers=authHeader)
    veevaDocumentUpdateResponse = veevaDocumentUpdateResponse.json()
    return veevaDocumentUpdateResponse