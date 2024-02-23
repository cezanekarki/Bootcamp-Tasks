#Imports
import json
import boto3
import datetime

#Initialize DB and Table
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('Serverless-One_table')  

#SNS NOT REQUIRED in task. Commented for FUTURE REFRENCE
"""def sns_notification(sns_arn, sns_message):
    client = boto3.client('sns')
    response = client.publish(TopicArn=sns_arn, Message=sns_message)
    print('response:', response)
    return response
"""

#For POST
def msg_read(event, context):
    try:
        print('Received event:', json.dumps(event))
        current_date = datetime.datetime.now().strftime("%Y-%m-%d")
        name = event['name']
        email = event['email']
        message = event['textbox']
        item ={
            'date':current_date,
            'name':name,
            'textbox':email,
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
        
    #Exception Handler    
    except Exception as e:
        print("Error:", e)
        response = {
            "statusCode": 500,
            "data": json.dumps({"message": "Internal Server Error"})
        }
    return response