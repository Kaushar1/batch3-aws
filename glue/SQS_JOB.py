import sys
import sys
import json
import boto3
import urllib.parse
from awsglue.utils import getResolvedOptions

# ---- Job parameters (pass these from Glue job config) ----
args = getResolvedOptions(sys.argv, [
    'QUEUE_URL',
    'DEST_BUCKET'
])

QUEUE_URL = args['QUEUE_URL']
DEST_BUCKET = args['DEST_BUCKET']
REGION = 'ap-south-1'

sqs = boto3.client('sqs', region_name=REGION)
s3 = boto3.client('s3', region_name=REGION)


def process_message(message):
    """Parse S3 event from SQS message body and copy the object."""
    body = json.loads(message['Body'])

    if 'Records' not in body:
        print(f"Skipping non-S3-event message: {message['MessageId']}")
        return

    for record in body['Records']:
        event_name = record.get('eventName', '')
        if not event_name.startswith('ObjectCreated'):
            print(f"Skipping non-create event: {event_name}")
            continue

        src_bucket = record['s3']['bucket']['name']
        # S3 keys can be URL-encoded (spaces as '+', etc.) — decode them
        src_key = record['s3']['object']['key'].replace('+', ' ')
        src_key = urllib.parse.unquote(src_key)

        print(f"Copying s3://{src_bucket}/{src_key} -> s3://{DEST_BUCKET}/{src_key}")

        copy_source = {'Bucket': src_bucket, 'Key': src_key}
        s3.copy_object(
            CopySource=copy_source,
            Bucket=DEST_BUCKET,
            Key=src_key
        )
        print(f"Copied successfully: {src_key}")


def main():
    total_processed = 0

    while True:
        response = sqs.receive_message(
            QueueUrl=QUEUE_URL,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=10,      # long polling
            VisibilityTimeout=60     # give enough time to process before it's redelivered
        )

        messages = response.get('Messages', [])
        if not messages:
            print("No more messages in queue. Exiting.")
            break

        for message in messages:
            try:
                process_message(message)

                # delete only after successful processing
                sqs.delete_message(
                    QueueUrl=QUEUE_URL,
                    ReceiptHandle=message['ReceiptHandle']
                )
                total_processed += 1

            except Exception as e:
                print(f"Error processing message {message['MessageId']}: {e}")
                # message NOT deleted -> will reappear after visibility timeout

    print(f"Job complete. Total messages processed: {total_processed}")


if __name__ == '__main__':
    main()