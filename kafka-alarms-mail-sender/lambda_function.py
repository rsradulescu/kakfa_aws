import boto3
import os

def getClusterTags(clusterName):
    """
    This function returns tags of cluster

    Args:
        clusterName (str): Name of MSK Cluster
    
    Return Value:
        tags (dict): All tags of MSK cluster.
    """
    client = boto3.client('kafka')
    response = client.list_clusters(ClusterNameFilter=clusterName)
    return (response['ClusterInfoList'][0]['Tags'])


def generate_rotation_notice_html(clusterName,metricName):
    html_content = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta http-equiv="X-UA-Compatible" content="IE=edge">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>CDL MSK Cluster Alarm</title>
    </head>
    <body>
        <p>Hello,</p>
        <p>An alarm has been triggered for Msk kafka cluster: {clusterName}</p>
        <p>Metric Name: {metricName}</p>
        <p>Please contact XXX@gmail.com or create a ticket to the team.</p>
        <p>Best Regards,</p>
        <p>RR</p>
        </body>
    </html>
    """
    return html_content

def mail_sender(Sender,Responsible, Developer,Content):
    CHARSET = 'UTF-8'
    client = boto3.client('ses')
    
    response  = client.send_email(
        Destination = {
            'ToAddresses': [
                Responsible,
                Developer            ],
        },
        Message = {
            'Body': {
                'Html': {
                    'Charset': CHARSET,
                    'Data': Content,
                },
            },
            'Subject': {
                'Charset': CHARSET,
                'Data': 'MSK Cluster Alarms',
            }
        },
        Source=Sender,
    )


def lambda_handler(event,context):
    print(event)
    # Get cluster and tags 
    clusterName = event['alarmData']['configuration']['metrics'][0]['metricStat']['metric']['dimensions']['Cluster Name']
    metric=event['alarmData']['configuration']['metrics'][0]['metricStat']['metric']['name']
    print("Cluster Name: ", clusterName)
    print("Metric: ", metric)
    tags = getClusterTags(clusterName)
    print(tags)
    responsible = tags['Developer']
    developer = tags['Responsible']
    sender  = os.environ['sender']
    
    # Generate alert email 
    content = generate_rotation_notice_html(clusterName,metric)
    mail_sender(sender,responsible,developer,content)
    print('Finish successfully')
    
