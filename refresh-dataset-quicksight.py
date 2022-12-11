from asyncio import sleep
import boto3
import time
import sys

datasetid = #define dataset id
AwsAccountId = #aws account id


time = str(int(time.time()))
client = boto3.client('quicksight')
response = client.create_ingestion(DataSetId=datasetid,IngestionId=time,AwsAccountId=AwsAccountId)
while True:
    response = client.describe_ingestion(DataSetId=datasetid,IngestionId=time,AwsAccountId=AwsAccountId)
    if response['Ingestion']['IngestionStatus'] in ('INITIALIZED', 'QUEUED', 'RUNNING'):
        sleep(20) #change the value according to the expected time it may take to update the dataset
    elif response['Ingestion']['IngestionStatus'] == 'COMPLETED':
        print("refresh completed")
        break
    else:
        print("refresh failed! - status ".format(response['Ingestion']['IngestionStatus']))
        sys.exit(1)
