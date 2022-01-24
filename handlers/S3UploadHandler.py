import json
import boto3
import os
import uuid


def labelOnS3Upload(event, context):
    bucket = os.environ['SERVERLESS_IMAGE_LABELLING_BUCKET']
    region_name = os.environ['REGION_NAME']
    dynamoDb = boto3.resource('dynamodb', region_name=region_name)
    fileUploaded = event['Records']

    for file in fileUploaded:
        fileName = file['s3']['object']['Key']
        rekognitionClient = boto3.client('rekognition', region_name=region_name)
        response = rekognitionClient.detect_labels(Image={'S3Object': {'Bucket': bucket, 'Name': fileName}},
                                                   MaxLabels=5)
        print(response)

        imageLabels = []

        print(f"Detected labels for {fileName}: ")

        for label in response['Labels']:
            print(f"Label: {label['Name']}")
            imageLabels.append(label['Name'].lower())

        imageId = str(uuid.uuid1())

        addDataToTableResponse = addImageDataToMasterTable(dynamoDb, imageId, fileName, imageLabels)
        print(json.dumps(addDataToTableResponse))
        addToLabelMappingTableResponse = addToLabelMappingTable(dynamodb=dynamoDb, imageID=imageId,
                                                                imageLabels=imageLabels)

        print(json.dumps(addToLabelMappingTableResponse))

        s3HandlerResponseBody = {
            "addImageDataToMasterTableResponse": addToLabelMappingTableResponse,
            "addToLabelMappingTableResponse": addToLabelMappingTableResponse
        }

        finalResponse = {
            "statusCode": 200,
            "body": json.dumps(s3HandlerResponseBody)
        }
        print(finalResponse)
        return finalResponse


def addImageDataToMasterTable(dynamodb, imageID, fileName, labels):
    masterImageTable = dynamodb.Table(os.environ['MASTER_IMAGE_TABLE'])
    item = {
        'imageID': str(imageID),
        'fileName': fileName,
        'labels': labels

    }


def addToLabelMappingTable(dynamodb, imageID, imageLabels):
    labelToS3MappingTable = dynamodb.Table(os.environ['LABEL_TO_S3_MAPPING_TABLE'])
    labelResponses = []
    imageIDSet = set()
    imageIDSet.add(imageID)

    for label in imageLabels:
        addLabelResponse = labelToS3MappingTable.update_item(
            Key={'label': label},
            UpdateExpression="ADD imageIDs :imageID",
            ExpressionAttributeValues={":imageID": imageIDSet}
        )
        print(json.dumps(addLabelResponse))
        labelResponses.append(addLabelResponse)

    labelToS3MappingTableResponse = {
        "labelResponses": labelResponses
    }

    return labelToS3MappingTableResponse
