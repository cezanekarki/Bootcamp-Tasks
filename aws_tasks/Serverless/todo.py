# Import required libraries
import json
import uuid
import boto3

# Initialize DynamoDB resource and table
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('mydatabase.db')

# Constants for common responses
RESOURCE_NOT_FOUND = {'statusCode': 404, 'body': json.dumps({'message': 'Resource not found'})}
METHOD_NOT_ALLOWED = {'statusCode': 405, 'body': json.dumps({'message': 'Method not allowed'})}
ACCESS_CONTROL_HEADERS = {"Access-Control-Allow-Origin": "*", "Access-Control-Allow-Headers": "Content-Type"}

# Function to generate CORS headers
def generate_cors_headers(method):
    headers = ACCESS_CONTROL_HEADERS.copy()
    headers["Access-Control-Allow-Methods"] = method
    return headers

# Function to add CORS headers to the response
def add_cors_headers(response, method):
    response['headers'] = generate_cors_headers(method)
    return response

# Function to create a response with CORS headers
def create_response(status_code, message, data=None):
    response = {'statusCode': status_code, 'body': json.dumps({'message': message, 'data': data} if data else {'message': message})}
    return add_cors_headers(response, '*')

# Decorator to handle exceptions in Lambda function
def handle_exceptions(fn):
    def wrapper(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            return create_response(500, str(e))
    return wrapper

# Function to handle Lambda event
@handle_exceptions
def lambda_handler(event, context):
    # Extract resource and HTTP method from the event
    resource = event['resource']
    http_method = event['httpMethod']

    # Route requests based on resource and method
    if resource == '/todos':
        if http_method == 'GET':
            return get_all_tasks(event)
        else:
            return METHOD_NOT_ALLOWED
    elif resource == '/todo':
        if http_method == 'GET':
            return get_task(event)
        elif http_method == 'POST':
            return create_task(event)
        elif http_method == 'PUT':
            return update_task(event)
        elif http_method == 'DELETE':
            return delete_task(event)
        else:
            return METHOD_NOT_ALLOWED
    else:
        return RESOURCE_NOT_FOUND

# Function to get all tasks
@handle_exceptions
def get_all_tasks(event):
    response = table.scan()
    return create_response(200, 'Tasks retrieved successfully', response['Items'])

# Function to get a specific task
@handle_exceptions
def get_task(event):
    task_id = event['queryStringParameters']['id']
    response = table.get_item(Key={'id': task_id})
    if 'Item' in response:
        return create_response(200, 'Task retrieved successfully', response['Item'])
    else:
        return create_response(404, 'Task not found')

# Function to create a new task
@handle_exceptions
def create_task(event):
    task_id = str(uuid.uuid4())
    request_body = json.loads(event['body'])
    task_data = {'id': task_id, 'name': request_body.get('name', ''), 'details': request_body.get('details', '')}
    table.put_item(Item=task_data)
    return create_response(201, 'Task created successfully', {'task': task_data})

# Function to update an existing task
@handle_exceptions
def update_task(event):
    try:
        task_id = event['queryStringParameters']['id']
        update_data = json.loads(event['body'])

        # Build the update expression dynamically
        update_expression = 'SET ' + ', '.join([f'#{k} = :{k}' for k in update_data.keys()])
        
        # Build the expression attribute names and values
        expression_attribute_names = {f'#{k}': k for k in update_data.keys()}
        expression_attribute_values = {f':{k}': update_data[k] for k in update_data.keys()}

        # Update the task in the DynamoDB table
        table.update_item(
            Key={'id': task_id},
            UpdateExpression=update_expression,
            ExpressionAttributeNames=expression_attribute_names,
            ExpressionAttributeValues=expression_attribute_values
        )

        return create_response(200, 'Task updated successfully')
    except Exception as e:
        return create_response(500, str(e))

# Function to delete a task
@handle_exceptions
def delete_task(event):
    task_id = event['queryStringParameters']['id']
    table.delete_item(Key={'id': task_id})
    return create_response(200, 'Task deleted successfully')
