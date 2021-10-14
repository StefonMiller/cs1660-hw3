import boto3
import csv

s3 = boto3.resource('s3',
    aws_access_key_id='aws_access_key',
    aws_secret_access_key='aws_secret_key'
)

try:
    s3.create_bucket(Bucket='cs1660-hw3', CreateBucketConfiguration={
    'LocationConstraint': 'us-east-2'})
except Exception as e:  
    pass

bucket = s3.Bucket("cs1660-hw3")
bucket.Acl().put(ACL='public-read')

body = open('NoSQL/exp1.csv', 'rb')

o = s3.Object('cs1660-hw3', 'test').put(Body=body)

s3.Object('cs1660-hw3', 'test').Acl().put(ACL='public-read')

dyndb = boto3.resource('dynamodb',
    region_name='us-east-2',
    aws_access_key_id='aws_access_key',
    aws_secret_access_key='aws_secret_key')
try:
    table = dyndb.create_table(
    TableName='DataTable',
    KeySchema=[{
        'AttributeName': 'PartitionKey',
        'KeyType': 'HASH'
    },
    {
        'AttributeName': 'RowKey',
        'KeyType': 'RANGE'
    }],
    AttributeDefinitions=[{
        'AttributeName': 'PartitionKey',
        'AttributeType': 'S'
    },
    {
        'AttributeName': 'RowKey',
        'AttributeType': 'S'
    }],
    ProvisionedThroughput={
        'ReadCapacityUnits': 5,
        'WriteCapacityUnits': 5
    })
except Exception as e:
    pass
    #if there is an exception, the table may already exist. if so...
    table = dyndb.Table("DataTable")

#wait for the table to be created
table.meta.client.get_waiter('table_exists').wait(TableName='DataTable')

with open('NoSQL/experiments.csv', 'r') as csvfile:
    csvf = csv.reader(csvfile, delimiter=',', quotechar='|')
    next(csvf)
    
    for item in csvf:
        body = open('NoSQL/' + item[4], 'rb')
        s3.Object('cs1660-hw3', item[4]).put(Body=body )
        md = s3.Object('cs1660-hw3', item[4]).Acl().put(ACL='public-read')

        url = " https://s3-us-east-2.amazonaws.com/cs1660-hw3/"+item[4]
        metadata_item = {'PartitionKey': item[4], 'RowKey': item[0],
            'url':url}
        try:
            table.put_item(Item=metadata_item)
        except:
            pass

response = table.get_item(
    Key={
        'PartitionKey': 'exp1.csv',
        'RowKey': '1'
    }
)
item = response['Item']
print(item)
