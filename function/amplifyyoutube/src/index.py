import json
import os
from PIL import Image
import base64
from io import BytesIO
import boto3
import hashlib


s3Bucket = "dog-recognition-app-us-east-1"
s3FileKey = "image.jpeg"
dynamoDBTable = "DogGoneGPT"
fileName = "image.jpeg"
PATH = "/tmp/"


def hash_base64_string(base64_string):
    sha256_hash = hashlib.sha256()
    sha256_hash.update(base64_string.encode())
    return sha256_hash.hexdigest()


def put_hashed_data_into_dynamodb(table_name, hashed_string, coordinates):
    dynamodb = boto3.client("dynamodb")
    
    table_params = {
        "TableName": table_name,
        "Item": {
            "ImageID": {"S": hashed_string},  # Make sure hashed_string is a string
            "GPS": {"S": coordinates},  # Replace with the JSON string coordinates
        }
    }

    try:
        response = dynamodb.put_item(**table_params)
        print("Inserted data into DynamoDB")
        return response
    except Exception as e:
        print(f"Error putting data into DynamoDB: {str(e)}")
        return None




def detect_labels_with_rekognition(s3_bucket=None, s3_file_key=None):
    rekognition_client = boto3.client("rekognition")
    rekognition_params = {}

    if s3_bucket and s3_file_key:
        rekognition_params = {
            "S3Object": {
                "Bucket": s3_bucket,
                "Name": s3_file_key,
            }
        }

    try:
        rekognition_response = rekognition_client.detect_labels(Image=rekognition_params)
        return rekognition_response
    except Exception as e:
        print(f"Error detecting labels with Rekognition: {str(e)}")
        return []

def uploadFileToS3(bucket_name, file_name, file_bytes):
    s3 = boto3.client("s3")
    try:
        s3.upload_fileobj(BytesIO(file_bytes), bucket_name, file_name)
        print(f"File '{file_name}' uploaded to S3 bucket '{bucket_name}'")
    except Exception as e:
        print(f"Error uploading file to S3: {str(e)}")

def handler(event, context):
    try:
        data = json.loads(event.get("body"))
        parsed_data = json.loads(data)

        base64_string = parsed_data["base64Data"]

        hashed_base64 =  hash_base64_string(base64_string)
        print(hashed_base64)
        print(type(hashed_base64))
      


        coordinates = json.loads(parsed_data["coordinates"])

        latitude = coordinates["latitude"]
        longitude = coordinates["longitude"]

        image_bytes = base64.b64decode(base64_string.split(",")[1])
        im = Image.open(BytesIO(image_bytes))
        im.save(PATH + fileName, "JPEG")

        uploadFileToS3(s3Bucket, fileName, image_bytes)
        labels = detect_labels_with_rekognition(s3_bucket=s3Bucket, s3_file_key=fileName)

        put_hashed_data_into_dynamodb(dynamoDBTable, str(hashed_base64), str(coordinates))

        response = {
            "statusCode": 200,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "*",
                "Content-Type": "application/json",
            },
            "body": json.dumps({"message": "Hello from your new Amplify Python lambda!", "labels": labels}),
        }

        # Clean up temporary file
        os.remove(PATH + fileName)
    except Exception as e:
        response = {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }

    return response
