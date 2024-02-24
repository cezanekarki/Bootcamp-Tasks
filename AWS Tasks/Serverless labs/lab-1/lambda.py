import json
import boto3

def lambda_handler(event, context):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('Contact-form')
    try:
        http_method = event['httpMethod']
        if http_method == "POST":
            request_body = json.loads(event['body'])
            first_name = request_body.get('fname', '')
            last_name = request_body.get('lname', '')
            email = request_body.get('email', '')
            message = request_body.get('subject', '')
            item = {
                'first_name': first_name,
                'last_name': last_name,
                'email': email,
                'message': message
            }
            table.put_item(Item=item)
            response = {
                "statusCode": 200,
                "body": json.dumps({"message": "Request processed successfully"})
            }
    except Exception as e:
        # Handle any exceptions
        print("Error:", e)
        # Return error response
        response = {
            "statusCode": 500,
            "body": json.dumps({"message": "Internal Server Error"})
        }

    return response
