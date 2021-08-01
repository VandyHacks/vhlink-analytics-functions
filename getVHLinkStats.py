import boto3
from collections import defaultdict
from decimal import Decimal
import json
from json.decoder import JSONDecodeError


def make_viz(times):
    timesViz = defaultdict(int)
    for t in times:
        timesViz[int(t)] += 1
    return timesViz
    


def lambda_handler(event, context):
    if event['requestContext']['http']['method'] != 'GET':
        return {
            'statusCode': 400,
            'body': 'Only GET allowed',
        }
    
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('vhlink_stats')
    path = event['pathParameters']['path']
    if path == '':
        stats = []
        done = False
        start_key = None
        scan_kwargs = {}
        while not done:
            if start_key:
                scan_kwargs['ExclusiveStartKey'] = start_key
            response = table.scan(**scan_kwargs)
            stats.extend(response.get('Items', []))
            start_key = response.get('LastEvaluatedKey', None)
            done = start_key is None
        for stat in stats:
            print(stat)
            stat['timesViz'] = make_viz(stat['times'])
    else:
        item = table.get_item(Key={
            'path':  path,
        })
        if 'Item' in item:
            stats = item['Item']
            stats['timesViz'] = make_viz(stats['times'])
        else:
            stats = {
                'path': path,
                'times': [],
                'hits': 0,
            }
    
    def set_default(obj):
        if isinstance(obj, set):
            return list(obj)
        if isinstance(obj, Decimal):
            return int(obj)
        print(type(obj))
        raise TypeError()
    
    return {
        'statusCode': 200,
        'headers': {'Content-Type': 'application/json'},
        'body': json.dumps(stats, default=set_default),
    }
