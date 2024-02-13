import json
import boto3
from botocore.exceptions import ClientError

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('user_db')  

def lambda_handler(event, context):
    http_method = event.get("httpMethod").upper()
    
    if http_method == "POST":
        return handle_post_request(event)
    elif http_method == "GET":
        return handle_get_request(event)
    elif http_method == "DELETE":
        return handle_delete_request(event)
    elif http_method == "PUT":
        return handle_put_request(event)
    else:
        return {
            "statusCode": 405,
            "body": json.dumps({"error": "Method not allowed"})
        }

def build_response(code, message, data=None):
    response_data = {
        "Code": code,
        "Message": message,
        "Data": data
    }
    return {
        "statusCode": code,
        "body": json.dumps(response_data)
    }

def handle_post_request(event):
    try:
        userid = event.get('userid')
        username = event.get('username')
        todos = event.get('todos')
        task_status = event.get('task_status')
        item = {
            'userid': userid,
            'username': username,
            'todos':todos,
            'task_status':task_status
        }

        # Put item into DynamoDB
        table.put_item(Item=item)

        return build_response(200, "Success", {"message": "The data is inserted into the table."})
    except ClientError as e:
        return build_response(500, "Internal Server Error", str(e))

def handle_get_request(event):
    try:
        result = table.scan()
        items = result.get("Items",[])
        return build_response(200, "Success", items)
    except ClientError as e:
        return build_response(500, "Internal Server Error", str(e))

def handle_delete_request(event):
    try:
        userid = event.get('userid')
        table.delete_item(Key={'userid': userid})
        return build_response(200, "Success", {"message": "User deleted successfully."})
    except ClientError as e:
        return build_response(500, "Internal Server Error", str(e))

def handle_put_request(event):
    try:
        userid = event.get('userid')
        update_key = event.get('update_key')
        update_value = event.get('update_value')

        table.update_item(
            Key={'userid': userid},
            UpdateExpression=f'SET {update_key} = :value',
            ExpressionAttributeValues={':value': update_value},
            ReturnValues='UPDATED_NEW'
        )
        return build_response(200, "Success", {"message": "User updated successfully."})
    except ClientError as e:
        return build_response(500, "Internal Server Error", str(e))
