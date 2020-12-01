import os
import json

os.environ['AWS_PROFILE']= 'jobnoo'

import boto3
client = boto3.client('sqs', region_name='us-east-1')


if __name__ == "__main__":
    symbols=['btcusdt']
    for symbol in symbols:
        client.send_message(
            QueueUrl='https://sqs.us-east-1.amazonaws.com/418448067427/trades-split-work-queue',
            MessageBody=json.dumps({
                "symbol": symbol
            }))

