import json
import boto3



def lambda_handler(event, context):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('ContactForm')  
    
    try:
        http_method = event['httpMethod']
        
        if http_method == "POST":
            request_body = json.loads(event['body'])
            name = request_body.get('name', '')
            email = request_body.get('email', '')
            message = request_body.get('message', '')
            
            item ={
                'name':name,
                'email':email,
                'message':message
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
    
    
    
    
  


