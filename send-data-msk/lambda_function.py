import boto3
import json
import os
from confluent_kafka import Producer
from botocore.exceptions import ClientError
from urllib.parse import unquote_plus

# Get the secret for kafka credentials
def get_kafka_credentials():
    secret_name = os.environ['sasl_credentials_secret']
    region_name = os.environ['region']

    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise Exception("Couldn't retrieve Kafka credentials from Secrets Manager") from e
    else:
        if 'SecretString' in get_secret_value_response:
            return json.loads(get_secret_value_response['SecretString'])
        else:
            raise Exception("Kafka credentials are not in SecretString format")
            
# Retrieve Kafka credentials from AWS Secrets Manager
kafka_credentials = get_kafka_credentials()

# Initialize Kafka producer
conf = {
    'bootstrap.servers': os.environ['bootstrap_servers'],
    'security.protocol': os.environ['security_protocol'],
    'sasl.mechanism': os.environ['sasl_mechanism'],
    'compression.type': os.environ['producer_compression'],
    'sasl.username': kafka_credentials['username'],
    'sasl.password': kafka_credentials['password']
}
producer = Producer(conf)
topic_name = os.environ['topic']



def lambda_handler(event, context):
    # Iterate over each SQS message in the batch
    num = 1
    for record in event['Records']:
        print(f'the record event is: {record}')
        sqs_body = json.loads(record['body'])
        
        # If the notification comes from SQS, by default we have directly the S3 event in there
        s3_event = sqs_body
        #If the notification comes from SNS it is needed to get the S3 event from the Message key
        if 'Message' in sqs_body:
            s3_event = json.loads(sqs_body['Message'])
            print(f'The s3 event is from a sqs and is:{s3_event}')
                
        if 'Records' in s3_event and len(s3_event['Records']) != 0:
            for s3_record in s3_event['Records']:
                print(f'the record s3 event is: {s3_record}')
                # Parse SQS message to get S3 bucket and object key
                s3_bucket = s3_record['s3']['bucket']['name']
                s3_object_key = unquote_plus(s3_record['s3']['object']['key'])
        
                # Initialize S3 client
                s3 = boto3.client('s3')
                
                # Get the object from S3
                s3_object = s3.get_object(Bucket=s3_bucket, Key=s3_object_key)
                s3_object_content = s3_object['Body'].read().decode('utf-8')
                
                #Send the data to MSK Kafka
                for s3_object_line in s3_object_content.splitlines():
                    try:
                        complete_message = json.loads(s3_object_line)
                    except json.JSONDecodeError as ex:
                        print(f"Invalid JSON")
                        continue
                    
                    key = complete_message['app']['context']['haId']
                    log_type = complete_message['app']['cdlConfig']['type']
                    region = complete_message['app']['cdlConfig']['region']
                    topic = f'{topic_name}_{log_type}_{region}'
                    
                    producer.produce(topic=topic, key=bytes(key, 'utf-8'), value=bytes(s3_object_line, 'utf-8'))

        print(f'Ending iteration number {num} - sending data to topic {topic}')
        num += 1
        
    print("Finish all elements in the batch")    
    # Ensure all messages are sent
    producer.flush()

    return {
        'statusCode': 200,
        'body': json.dumps('Data sent to MSK Kafka successfully!')
    }