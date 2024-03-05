import json
import boto3

def lambda_handler(event, context):
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table("my-old-table")
   
    http_method = event.get("httpMethod").upper()
   
    if http_method == "POST":
        id = event.get("id")
        name = event.get("name")
        todo = event.get("todo")
        todo_status = event.get("todo_status")
       
        item = {
            "id": id,
            "name": name,
            "todo": todo,
            "todo_status": todo_status
        }
       
        table.put_item(Item=item)
       
        return {
            'statusCode': 200,
            'body': json.dumps('Successfully saved to database!')
        }
       
    elif http_method == "GET":
        response = table.scan()
        items = response["Items"]
       
        return {
                "message": "Data from DynamoDB",
                "data": items
            }
       
    elif http_method == 'DELETE':
        id = event.get('id')
        table.delete_item(Key={'id': id})
       
        return {
                "message": f"Data with id {id} is deleted from DynamoDB"
            }
    elif http_method == "PUT":
        id = event.get('id')
        update_key = event.get('update_key')
        update_value = event.get('update_value')

        table.update_item(
            Key={'id': id},
            UpdateExpression=f'SET {update_key} = :value',
            ExpressionAttributeValues={':value': update_value},
            ReturnValues='UPDATED_NEW'
        )