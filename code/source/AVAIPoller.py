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

#read the environment variables
veevaDomainName = unquote_plus(os.environ['VEEVA_DOMAIN_NAME']) 


veevaUserName = unquote_plus(os.environ['VEEVA_DOMAIN_USERNAME']) 


veevaPassword = unquote_plus(os.environ['VEEVA_DOMAIN_PASSWORD']) 


bucketName = unquote_plus(os.environ['BUCKETNAME']) 


queueName = unquote_plus(os.environ['QUEUE_NAME']) 


# Veeva URL formats.
authUrl = 'https://{0}.veevavault.com/api/v20.1/auth'.format(veevaDomainName)
dataUrl = 'https://{0}.veevavault.com/api/v20.1/'.format(veevaDomainName)

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
    # print(response)
    response = response.json()

    global runDate

    # Get the queue
    queue = sqs.get_queue_by_name(QueueName=queueName)
    keyName = ''

    if(response['responseStatus'] == 'SUCCESS'):
        print ('Authentication Successful.')
        sessionId = response['sessionId']
        
        #authHeader would be needed for subsequent calls. 
        authHeader = {'Authorization': sessionId}
        
        
        print('Querying Veeva for changes after {0}.'.format(str(runDate)))

        query = 'SELECT id, format__v, filename__v, major_version_number__v, minor_version_number__v, version_modified_date__v, version_creation_date__v from documents'
        query = "{0} where version_creation_date__v >= '{1}'".format(query, runDate.strftime("%Y-%m-%dT%H:%M:%S.000Z")) 
        
        #update runDate so that next time we just get the deltas from the last run.
        runDate = datetime.datetime.utcnow()

        payload = {'q': query}

        # post the query
        # https://developer.veevavault.com/api/20.1/#vault-query-language-vql
        veeva_Docs = requests.post(dataUrl+'query', headers=authHeader, data = payload)
        veeva_Docs = veeva_Docs.json()
        if (veeva_Docs['responseStatus'] == 'SUCCESS'):
            for document in veeva_Docs['data']:
                if (document['format__v'] == 'image/jpeg' or document['format__v'] == 'image/png' or  document['format__v'] == 'application/pdf' or  document['format__v'] == 'audio/mp3'):
                    filename = document['filename__v']
                    print(('Downloading document ID {0} with filename {1}').format(document['id'], filename))
                    docImageUrl = ('objects/documents/{0}/versions/{1}/{2}/file').format(document['id'],document['major_version_number__v'],document['minor_version_number__v'])
                    veeva_Doc = requests.get(dataUrl+docImageUrl, headers=authHeader)
                    if (veeva_Doc.headers['Content-Type'] == 'application/octet-stream;charset=UTF-8'):
                        # copy image to S3
                        keyName = 'input/' + filename
                        response = s3.put_object(Bucket = bucketName, Key = keyName, Body = veeva_Doc.content)
                        # put a message in SQS
                        # Create a new message
                        message = {}
                        message['documentId'] = document['id']
                        message['fileType'] = 'png'  # can be png, jpg, pdf, mp3
                        message['bucketName'] = bucketName
                        message['keyName'] = keyName

                        print('sending message to queue ' + queueName)
                        response = queue.send_message(MessageBody= json.dumps(message), MessageGroupId='messageGroup1', MessageDeduplicationId = str(uuid.uuid4()))
                    else:
                        print(veeva_Doc.json()['errors'][0]['message'])
    else:
        print ('Authentication NOT Successful.')
        print (json.dumps(response))

    return 1