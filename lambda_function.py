import boto3
import os
import json
import logging

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize the DynamoDB client
dynamodb = boto3.resource('dynamodb')

def lambda_handler(event, context):
    tbl_name = os.environ['TABLE_NAME']
    websites_tbl = dynamodb.Table(tbl_name)

    #Initialize to SQS queue
    sqs = boto3.resource('sqs')
    queue_name = os.environ['SQS_QUEUE_NAME']
    logger.info(f"Opening queue {queue_name}")
    queue = sqs.get_queue_by_name(QueueName=queue_name)
    max_entries = int(os.environ['MAX_ENTRIES_PER_REQUEST'])

    try:
        # Load websites from dynamodb
        websites = websites_tbl.scan()
        items = websites['Items']

        request_urls = []
        for item in items:
            url = item['url']
            logger.info(f"Posting URL to SQS Queue: '{url}'")
            request_urls.append(url)
            if len(request_urls) >= max_entries:
                # Publish URL to queue
                try:
                    urls_json = json.dumps({'urls': request_urls})
                    queue.send_message(MessageBody=urls_json)
                    logger.info(f"Message Sent for urls {urls_json}")
                    request_urls.clear()
                except Exception as e:
                    logger.error(f"Unable to initiate website check for {request_urls}:{str(e)}")
        
        # pick up urls from the end of the list    
        try:
            if len(request_urls):
                urls_json = json.dumps({'urls': request_urls})
                queue.send_message(MessageBody=urls_json)
                logger.info(f"Message Sent for urls {urls_json}")
        except Exception as e:
            logger.error(f"Unable to initiate website check for {request_urls}:{str(e)}")
            
    except Exception as e:
        logger.error(f"Error scanning DynamoDB: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps("Error scanning DynamoDB")
        }

    return {
        'statusCode': 200,
        'body': json.dumps("Website processed")
    }
