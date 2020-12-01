import json
from binance.client import Client
import os
import pandas as pd
import boto3
client = Client()


batch_size = 100000
sqs = boto3.client('sqs')
s3 = boto3.client('s3')


def split_work(event, context):
    message = json.loads(event['Records'][0]['body'])
    last_trade_id = client.get_aggregate_trades(
        symbol=message['symbol'].upper(), limit=1)[-1]['a']

    last_processed_trade_id = 0  # TODO get from dynamodb
    while(last_processed_trade_id < last_trade_id):
        last_processed = min(last_trade_id, last_processed_trade_id+batch_size)
        queueTask(last_processed_trade_id, last_processed, message)
        last_processed_trade_id = last_processed


def download(event, context):
    message = json.loads(event['Records'][0]['body'])

    stop_id = message['stop_id']
    trades = []
    symbol = message['symbol']
    start_id = message['start_id']
    for trade in client.aggregate_trade_iter(symbol=symbol, last_id=start_id):
        trades.append(trade)
        if trade['a'] == stop_id:
            break

    df = create_dataframe(trades)
    write_to_s3(df, symbol, start_id, stop_id)


def create_dataframe(trades):
    df = pd.DataFrame.from_records(trades)
    df.set_index('a', inplace=True)
    df.sort_index(inplace=True)
    df = df[~df.index.duplicated(keep='first')]
    return df


def write_to_s3(df, symbol, start_id, stop_id):
    file_name = f'/tmp/trades-{start_id}-{stop_id}.parquet'
    df.to_parquet(file_name)
    s3.upload_file(file_name, 'np-trades-2020',
                   f'{symbol}/trades-{start_id}-{stop_id}.parquet')


def queueTask(last_processed_trade_id, temp, message):
    sqs.send_message(
        QueueUrl=os.environ['WORK_QUEUE'],
        MessageBody=json.dumps({"start_id": last_processed_trade_id,
                                "stop_id": temp-1,
                                "symbol": message['symbol'].upper()
                                }))
