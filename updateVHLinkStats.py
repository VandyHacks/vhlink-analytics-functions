import boto3
import json
from json.decoder import JSONDecodeError
import time



def lambda_handler(event, context):
    if event['requestContext']['http']['method'] != 'POST':
        return {
            'statusCode': 400,
            'body': 'Only POST allowed',
        }
    if 'body' not in event or 'content-type' not in event['headers'] or event['headers']['content-type'] != 'application/json':
        return {
            'statusCode': 400,
            'body': 'Requires body of type JSON',
        }
    try:
        body = json.loads(event['body'])
    except JSONDecodeError as e:
        return {
            'statusCode': 400,
            'body': 'Body is invalid JSON',
        }
    
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('VHLink_stats')
    
    
    response = table.update_item(
        Key={ 'path': body['path'] },
        UpdateExpression='ADD hits :one, times :time',
        ExpressionAttributeValues={
			':one': 1,
			':time': {int(time.time())},
		},
    )
    
    return {
        'statusCode': 200,
        'headers': {'Content-Type': 'application/json'},
        'body': json.dumps(response),
    }
