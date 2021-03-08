import boto3


s3 = boto3.resource('s3', 
	aws_access_key_id='SECRET', 
	aws_secret_access_key='SECRET'
)


try:
	s3.create_bucket(Bucket='datacont-test', 
		CreateBucketConfiguration={
		'LocationConstraint': 'us-east-2'}) 
except:
	print("this may already exist")



bucket = s3.Bucket("datacont-test")
bucket.Acl().put(ACL='public-read')



#new object
body = open(r'C:/Users/Derek Stathis/Desktop/cs1660/nosql/exp22.csv', 'rb')
o = s3.Object('datacont-test', 'exp22').put(Body=body )
s3.Object('datacont-test', 'exp22').Acl().put(ACL='public-read')




#the database connection
dyndb = boto3.resource('dynamodb', 
	region_name='us-east-2', 
	aws_access_key_id='SECRET', 
	aws_secret_access_key='SECRET'
)

#the database schema
try:
	table = dyndb.create_table( TableName='DataTable', KeySchema=[
		{
			'AttributeName': 'PartitionKey', 
			'KeyType': 'HASH'
		}, 
		{
			'AttributeName': 'RowKey',
			'KeyType': 'RANGE' 
		}
	], 
	AttributeDefinitions=[
		{
			'AttributeName': 'PartitionKey', 
			'AttributeType': 'S'
		}, 
		{
			'AttributeName': 'RowKey',
			'AttributeType': 'S' 
		},
	], 
	ProvisionedThroughput={
		'ReadCapacityUnits': 5,
		'WriteCapacityUnits': 5 
	}
) 
except:
	#if there is an exception, the table may already exist. if so...
	table = dyndb.Table("DataTable")

#wait for the table to be created. 
table.meta.client.get_waiter('table_exists').wait(TableName='DataTable')

print(table.item_count)


import csv
import os

with open(r'C:/Users/Derek Stathis/Desktop/cs1660/nosql/Master.csv', 'rt', encoding ="utf8") as csvfile: 
	csvf = csv.reader(csvfile, delimiter=',', quotechar='|')
	next(csvf)
	for item in csvf:
		print(item)
		body = open(os.path.join(r"C:/Users/Derek Stathis/Desktop/cs1660/nosql",item[4]), 'rb')
		s3.Object('datacont-test', item[4]).put(Body=body)
		md = s3.Object('datacont-test', item[4]).Acl().put(ACL='public-read')
		url = "https://s3-us-east-2.amazonaws.com/datacont-test/"+item[4] 
		metadata_item = {'PartitionKey': item[0], 'RowKey': item[1],
			'description' : item[3], 'date' : item[2], 'url':url}
		try: 
			table.put_item(Item=metadata_item)
		except:
			print("item may already be there or another failure")



response = table.get_item( 
	Key={
		'PartitionKey': 'experiment2',
		'RowKey': 'data2' 
	}
)

item = response['Item'] 
print(item)

response