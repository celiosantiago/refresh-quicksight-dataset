from asyncio import sleep
import boto3
import time
import sys

time = str(int(time.time()))
client = boto3.client('quicksight')
response = client.create_ingestion(DataSetId='6b2db34d-7dc8-40f9-9111-f07d2573508d',IngestionId=time,AwsAccountId='431591413306')
while True:
    response = client.describe_ingestion(DataSetId='6b2db34d-7dc8-40f9-9111-f07d2573508d',IngestionId=time,AwsAccountId='431591413306')
    if response['Ingestion']['IngestionStatus'] in ('INITIALIZED', 'QUEUED', 'RUNNING'):
        sleep(20) #mudar o valor, de acordo com o tempo esperado que pode levar para atualizar o dataset
    elif response['Ingestion']['IngestionStatus'] == 'COMPLETED':
        print("refresh completed")
        break
    else:
        print("refresh failed! - status ".format(response['Ingestion']['IngestionStatus']))
        sys.exit(1)




#import boto3
#
#client = boto3.client('quicksight')
#
#response = client.list_data_sets(
#    AwsAccountId='431591413306'
#)
#
#print(response)