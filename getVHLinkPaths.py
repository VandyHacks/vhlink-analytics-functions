import boto3
from collections import defaultdict
from decimal import Decimal
import json
from json.decoder import JSONDecodeError


def lambda_handler(event, context):
    if event['requestContext']['http']['method'] != 'GET':
        return {
            'statusCode': 400,
            'body': 'Only GET allowed',
        }
    
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('vhlink_stats')
    paths = []
    done = False
    start_key = None
    scan_kwargs = {
        'ProjectionExpression': '#p',
        'ExpressionAttributeNames': {'#p': 'path'},
        'Select': 'SPECIFIC_ATTRIBUTES',
    }
    while not done:
        if start_key:
            scan_kwargs['ExclusiveStartKey'] = start_key
        response = table.scan(**scan_kwargs)
        pathsSub = map(lambda x: x['path'], response.get('Items', []))
        paths.extend(pathsSub)
        start_key = response.get('LastEvaluatedKey', None)
        done = start_key is None
    
    return {
        'statusCode': 200,
        'headers': {'Content-Type': 'application/json'},
        'body': json.dumps(paths),
    }
