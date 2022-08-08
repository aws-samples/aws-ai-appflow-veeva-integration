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
import json
import uuid
import decimal
import time
import os
from urllib.parse import unquote_plus
import boto3

sys.path.insert(0, '/opt')

# read the environment variables
DDB_TABLE = unquote_plus(os.environ['DDB_TABLE'])

current_region = boto3.session.Session().region_name
dynamoDBResource = boto3.resource('dynamodb', region_name = current_region)

sqs = boto3.client('sqs')
rekognition = boto3.client('rekognition', region_name = current_region)
hera  = boto3.client(service_name='comprehendmedical', use_ssl=True, region_name = current_region)
textract = boto3.client('textract',region_name= current_region)
transcribe = boto3.client('transcribe',region_name=current_region)
s3 = boto3.resource('s3')
s3_client = boto3.client('s3')

table = dynamoDBResource.Table(DDB_TABLE)

def lambda_handler(event, context):

    print(event)

    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']

        print(bucket)
        print(key)

        metadata = s3_client.head_object(Bucket=bucket, Key=key)
        
        print(metadata)
        
        document_type = metadata['ContentType']
        
        print(document_type)

        message_body = {
            'bucketName': bucket,
            'keyName': key
        }

        print(message_body)

        try:
            if (message_body['keyName'].lower().endswith('.jpg') 
                    or message_body['keyName'].lower().endswith('.jpeg') 
                    or message_body['keyName'].lower().endswith('.png')):
                # Process the image.
                process_image(message_body)

            if (message_body['keyName'].lower().endswith('.txt')):
                print(f"Processing Document: {message_body['bucketName']}/{message_body['keyName']}")
                #get the S3 object
                bucket = s3.Bucket(message_body['bucketName'])
                file_text = bucket.Object(message_body['keyName']).get()['Body'].read().decode("utf-8", 'ignore')
                # Process the text document.
                process_document(message_body, file_text, 'Text-file')

            if (message_body['keyName'].lower().endswith('.pdf')):
                # process PDF
                process_pdf(message_body)

            if (message_body['keyName'].lower().endswith('.mp3') 
                or message_body['keyName'].lower().endswith('.mp4') 
                or message_body['keyName'].lower().endswith('.flac') 
                or message_body['keyName'].lower().endswith('.wav')
                or message_body['keyName'].lower().endswith('.ogg')
                or message_body['keyName'].lower().endswith('.webm')
                or message_body['keyName'].lower().endswith('.amr')
                ):
                # process Audio
                process_audio(message_body)

        except Exception as ex:
            print("Something went wrong processing " + str(message_body['keyName']))
            print(ex)


    return 1


def process_audio(message_body):
    if message_body is not None:

        bucket_name = message_body['bucketName']
        key_name = unquote_plus(message_body['keyName'])

        print(f"Processing audio file: {bucket_name}/{key_name}")

        # call start_transcription_job
        print('Calling start_transcription_job')

        media_format = key_name[key_name.rindex('.')+1:len(key_name)]
        transcription_job_name = str(uuid.uuid4())
        # start a async batch job for transcription
        transcribe.start_transcription_job(
                    TranscriptionJobName = transcription_job_name,
                    LanguageCode = 'en-US',
                    MediaFormat = media_format,
                    Media={
                                'MediaFileUri': f's3://{bucket_name}/{key_name}'
                            },
                    OutputBucketName = bucket_name
                    )

        transcribe_response = None
        # Check the response in a loop to see if the job is done.
        while True:
            print('Calling get_transcription_job...')
            transcribe_response = transcribe.get_transcription_job(
                                TranscriptionJobName=transcription_job_name
                        )
            print(transcribe_response['TranscriptionJob']['TranscriptionJobStatus'] )
            if (transcribe_response['TranscriptionJob']['TranscriptionJobStatus'] != 'IN_PROGRESS' and
                transcribe_response['TranscriptionJob']['TranscriptionJobStatus'] != 'QUEUED'):
                break
            time.sleep(3)

        # we have a status
        if transcribe_response is not None:
            if transcribe_response['TranscriptionJob']['TranscriptionJobStatus'] == 'COMPLETED':
                print('Success')
                # extract the KeyName from the TranscriptFileUri
                s3_location = transcribe_response['TranscriptionJob']['Transcript']['TranscriptFileUri']
                print(s3_location)
                s3_location = s3_location.replace('https://','')
                print(s3_location)
                print ('Text extracted from audio. Proceeding to extract clinical entities from the text...')
                target_key_name = s3_location.split('/')[2]
                print(target_key_name)
                # get the text
                bucket = s3.Bucket(bucket_name)
                file_text = bucket.Object(target_key_name).get()['Body'].read().decode("utf-8", 'ignore')
                # delete the transcribe output
                print ('Deleting transcribe output')
                bucket.Object(target_key_name).delete()
                # Use the extracted file text and process it using Comprehend Medical
                print ('Pushing transcript to Comprehend medical...')
                if len(file_text) > 0 :
                    process_document(message_body, file_text, 'Audio-file')
                else:
                    print('Warning: transcript is empty. Skipping file.')
            else:
                print('Failure')
        else:
            print('Failure')


def process_pdf(message_body):
    if message_body is not None:

        bucket_name = message_body['bucketName']
        key_name = unquote_plus(message_body['keyName'])

        print(f"Processing document: {bucket_name}/{key_name}")

        # call detect_document_text
        print('Calling detect_document_text')

        # start an async batch job to extract text from PDF
        response = textract.start_document_text_detection(
                    DocumentLocation={
                        'S3Object': {
                            'Bucket': bucket_name,
                            'Name': key_name
                        }
                    })

        textract_response = None
        # Check the response in a loop to see if the job is done.
        while True:
            print('Calling get_document_text_detection...')
            textract_response = textract.get_document_text_detection(
                            JobId=response['JobId'],
                            MaxResults=1000
                        )
            # print(textractResponse['JobStatus'] )
            if textract_response['JobStatus'] != 'IN_PROGRESS':
                break
            time.sleep(2)

        if textract_response is not None:
            if textract_response['JobStatus'] == 'SUCCEEDED':
                print('Success')
                textract_output = ''
                # contactanate all the text blocks
                for blocks in textract_response["Blocks"]:
                    if blocks['BlockType']=='LINE':
                        line = blocks['Text']
                        textract_output = textract_output + line +'\n'
                print ('Text extracted from image. Proceeding to extract clinical entities from the text...')

                # Use the extracted file text and process it using Comprehend Medical
                process_document(message_body, textract_output, 'PDF-file')
            else:
                print('Failure')
        else:
            print('Failure')

def process_document(message_body, file_text, asset_type):
    if file_text != '':

        if asset_type == '':
            asset_type = 'Text-file'

        # comprehend medical has a input size limit of 20,000 characters.
        # ideally, you should break it down in chunks of 20K characters and call them in a loop
        # For this PoC, we will just consider the first 20K characters.
        if len(file_text) > 20000:
            file_text = file_text[0:20000]

         # call detect_entities
        print('Calling detect_entities')

        # Call the detect_entities API to extract the entities
        test_result = hera.detect_entities(Text = file_text)

         # Create a list of entities
        test_entities = test_result['Entities']

        trait_list = []
        attribute_list = []

        # batch writer for dyanmodb is efficient way to write multiple items.
        with table.batch_writer() as batch:
            # Create a loop to iterate through the individual entities
            for row in test_entities:
                # Remove PHI from the extracted entites
                if row['Category'] != "PERSONAL_IDENTIFIABLE_INFORMATION":

                    # Create a loop to iterate through each key in a row 
                    for key in row:

                        # Create a list of traits
                        if key == 'Traits':
                            if len(row[key])>0:
                                trait_list = []
                                for r in row[key]:
                                    trait_list.append(r['Name'])

                        # Create a list of Attributes
                        elif key == 'Attributes':
                            attribute_list = []
                            for r in row[key]:
                                attribute_list.append(r['Type']+':'+r['Text'])

                item = generate_base_item(message_body, asset_type = asset_type, operation='DETECT_ENTITIES')
                item['Confidence'] = decimal.Decimal(row['Score']) * 100
                item['Tag'] = row['Text']
                item['Detect_Entities_Type']= row['Type']
                item['Detect_Entities_Category'] = row['Category']
                item['Detect_Entities_Trait_List']= str(trait_list)
                item['Detect_Entities_Attribute_List']=str(attribute_list)
                batch.put_item(Item=item)
        print('Tags inserted in DynamoDB.')

def process_image(message_body):
    if message_body is not None:
        print(f"Processing Image: {message_body['bucketName']}/{message_body['keyName']}")
        # call detect_labels
        print('Calling detect_labels')
        response = rekognition.detect_labels(
                            Image={
                                'S3Object': {
                                    'Bucket': message_body['bucketName'],
                                    'Name': message_body['keyName']
                                }
                            }
                        )

        if_person = False

        # batch writer for dyanmodb is efficient way to write multiple items.
        with table.batch_writer() as batch:
            for label in response['Labels']:
                item = generate_base_item(message_body, asset_type = 'Image', operation='DETECT_LABEL')
                item['Confidence'] = decimal.Decimal(label['Confidence'])                
                item['Tag'] = label['Name']
                batch.put_item(Item=item)

                if (label['Name'] == 'Human' or label['Name'] == 'Person') and (float(label['Confidence']) > 80):
                    if_person = True

            if if_person: # person detected, call detect faces
                # call detect_faces
                print('Calling detect_faces')
                response = rekognition.detect_faces(
                            Image={
                                'S3Object': {
                                    'Bucket': message_body['bucketName'],
                                    'Name': message_body['keyName']
                                }
                            },
                            Attributes=['ALL']
                        )
                # print(json.dumps(response))
                # faceDetails = response['FaceDetails']
                index = 1
                for face_detail in response['FaceDetails']:
                    del face_detail['BoundingBox']
                    del face_detail['Landmarks']
                    del face_detail['Pose']
                    del face_detail['Quality']
                    face_detail_confidence = face_detail['Confidence']
                    del face_detail['Confidence']

                    for (key,value) in face_detail.items():
                        if(key == 'Emotions'):
                            for emotion in value:
                                item = generate_base_item(message_body, asset_type = 'Image', operation='DETECT_FACE')
                                item['Face_Id']= index
                                item['Confidence'] = decimal.Decimal(emotion['Confidence'])
                                item['Tag'] = emotion['Type']
                                batch.put_item(Item=item)
                            continue

                        if key == 'AgeRange':
                            item = generate_base_item(message_body, asset_type = 'Image', operation='DETECT_FACE')
                            item['Face_Id']= index
                            item['Confidence'] = decimal.Decimal(face_detail_confidence)
                            item['Tag'] = key + '_Low'
                            item['Value'] = str(value['Low'])
                            batch.put_item(Item=item)

                            item = generate_base_item(message_body, asset_type = 'Image', operation='DETECT_FACE')
                            item['Face_Id']= index
                            item['Confidence'] = decimal.Decimal(face_detail_confidence)
                            item['Tag'] = key + '_High'
                            item['Value'] = str(value['High'])
                            batch.put_item(Item=item)
                            continue

                        item = generate_base_item(message_body, asset_type = 'Image', operation='DETECT_FACE')
                        item['Face_Id']= index
                        item['Confidence'] = decimal.Decimal(value['Confidence'])
                        item['Tag'] = key
                        item['Value'] = str(value['Value'])
                        batch.put_item(Item=item)
                    index+=1

            # call detect_text
            print('Calling detect_text')
            response = rekognition.detect_text(
                                    Image={
                                        'S3Object': {
                                            'Bucket': message_body['bucketName'],
                                            'Name': message_body['keyName']
                                        }
                                    }
                                )
            # create data structure and insert in DDB
            for text in response['TextDetections']:
                if text['Type'] == 'LINE':
                    item = generate_base_item(message_body, asset_type = 'Image', operation='DETECT_TEXT')
                    item['Confidence'] = decimal.Decimal(text['Confidence'])
                    item['Tag'] = text['DetectedText']
                    batch.put_item(Item=item)
        print('Tags inserted in DynamoDB.')
        return 1

def generate_base_item(message_body, asset_type = None, operation = None):
    # time in milliseconds
    timestamp = int(round(time.time() * 1000))

    return {
            'ROWID': str(uuid.uuid4()),
            'Location': message_body['bucketName'] + '/' + message_body['keyName'],
            'AssetType': asset_type,
            'Operation': operation,
            'TimeStamp': timestamp,
            'DocumentId': message_body['bucketName'] + '/' + message_body['keyName']
    }
