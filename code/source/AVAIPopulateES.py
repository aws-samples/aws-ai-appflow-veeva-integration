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
from urllib.parse import unquote_plus
import boto3
import requests
from requests_aws4auth import AWS4Auth

sys.path.insert(0, '/opt')

SERVICE = 'es'
HOST =  f"https://{unquote_plus(os.environ['ES_DOMAIN'])}"
INDEX = 'avai_index'
TYPE = '_doc'
DOC_URL = HOST + '/' + INDEX + '/' + TYPE + '/'
INDEX_URL = HOST+ '/' + INDEX
HEADERS = { "Content-Type": "application/json" }

# variables that will be used in the code
my_session = boto3.session.Session()
region = my_session.region_name
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, SERVICE, session_token=credentials.token)

INDEX_BODY = {
    "mappings": {
      "properties": {
        "ROWID": {
            "type": "keyword"
        },
        "Location": {
            "type": "keyword"
        },
        "AssetType": {
            "type": "keyword"
        },
        "Operation": {
            "type": "keyword"
        },
        "Tag": {
            "type": "keyword"
        },
        "Confidence": {
            "type": "float"
        },
        "Face_Id": {
            "type": "integer"
        },
        "Value": {
            "type": "keyword"
        },
        "TimeStamp": {
            "type": "date"
        }
      }
    }
  }

def lambda_handler(event, context):

    # Check if index exists
    response = requests.get(INDEX_URL, auth=awsauth, headers=HEADERS)
    if not response.ok:
        # create index
        response = requests.put(INDEX_URL, auth=awsauth, json=INDEX_BODY, headers=HEADERS)

    count = 0
    for record in event['Records']:
        # Get the primary key for use as the Elasticsearch ID
        es_id = record['dynamodb']['Keys']['ROWID']['S']

        if record['eventName'] == 'REMOVE':
            requests.delete(DOC_URL + es_id, auth=awsauth)
        else:
            document = record['dynamodb']['NewImage']
            # create index document
            item = {}
            item['AssetType'] = document['AssetType']['S']
            item['Confidence'] = float(document['Confidence']['N'])
            item['Operation'] = document['Operation']['S']
            item['Tag'] = document['Tag']['S']
            item['ROWID'] = document['ROWID']['S']
            item['TimeStamp'] = int(document['TimeStamp']['N'])
            if 'Face_Id' in document:
                item['Face_Id'] = int(document['Face_Id']['N'])
            if 'Value' in document:
                item['Value'] = document['Value']['S']
            item['Location'] = document['Location']['S']
            # print(json.dumps(item))
            requests.put(DOC_URL + es_id, auth=awsauth, json=item, headers=HEADERS)
        count += 1
        print(str(count) + ' records processed.')
    return str(count) + ' records processed.'
