import json
import boto3
import datetime

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('enquiry_form')  

def sns_notification(sns_arn, sns_message):
    client = boto3.client('sns')
    response = client.publish(TopicArn=sns_arn, Message=sns_message)
    print('response:', response)
    return response

def lambda_handler(event, context):
    try:
        print('Received event:', json.dumps(event))
        current_date = datetime.datetime.now().strftime("%Y-%m-%d")
        name = event['name']
        email = event['email']
        message = event['message']
        item ={
            'date':current_date,
            'name':name,
            'email':email,
            'message':message
        }

        table.put_item(Item=item)
        response = {
            "statusCode": 200,
            "data": json.dumps({"message": "Data is added successfully"})
        }
        #sns implementation
        # arn = "arn:aws:sns:us-east-1:612362567483:serverless-sns-topic"
        # message=f"New data stored"
        # resource = sns_notification(arn,message)
    except Exception as e:
        print("Error:", e)
        response = {
            "statusCode": 500,
            "data": json.dumps({"message": "Internal Server Error"})
        }
    return response
