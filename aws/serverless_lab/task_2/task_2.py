import json
import boto3
from botocore.exceptions import ClientError

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('todo')  

def lambda_handler(event, context):
    http_method = event['httpMethod']
    if http_method == "POST":
        return post_request(event)
    elif http_method =="GET":
        return get_request(event)
    elif http_method == "DELETE":
        return delete_request(event)
    elif http_method == "PUT":
        return put_request(event)
    else:
        return {
            "statusCode": 405,
            "body": json.dumps({"error": "Method not allowed"})
        }
    
def post_request(event):
    try:
        item={
            'id': event['id'],
            'task':event['task'],
            'status':event['status']
        }
        table.put_item(Item=item)
        response= {
            "statusCode":200,
            "message":"todo added"
        }
    except ClientError as e:
        response= {
            "statusCode":500,
            "message":"Something went wrong",
            "data":str(e)
        } 
    return response
    
def get_request(event):
    try:
        data = table.scan()
        items = data.get("Items", [])
        print(items)
        response= {
            "statusCode":200,
            "message":"Sucess",
            "data":items
        }
    except ClientError as e:
        response={
            "statusCode":500,
            "message":"Something went wrong",
            "data":str(e)
        } 
    return response

def delete_request(event):
    try:
        id_value = event['id']   # Get id from event
        if id_value is None:
            raise ValueError("ID is missing in the event")

        response = table.delete_item(Key={'id': id_value})
        return {
            "statusCode": 200,
            "message": "Data is deleted successfully",
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "message": "An error occurred while deleting data",
            "error": str(e)
        }


def put_request(event):
    try:
        id = event['id']
        update_key = event['key']
        update_value = event['new_value']
        
        table.update_item(
            Key={'id': id},
            UpdateExpression=f'SET #attr = :value',  
            ExpressionAttributeNames={'#attr': update_key}, 
            ExpressionAttributeValues={':value': update_value},
            ReturnValues='UPDATED_NEW'
        )
        
        response= {
            "statusCode":200,
            "message":"Task Updated"
        }
    except ClientError as e:
        response= {
            "statusCode":500,
            "message":"Something went wrong",
            "data":str(e)
        } 
    return response
