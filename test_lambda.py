import boto3
import pprint as pp
import json

client = boto3.client('lambda')
url = "https://www.furusato-tax.jp/search?sort=11&page=1"
payload = {
    "url": url,
}
response = client.invoke(
    FunctionName='products_with_review_count',
    InvocationType='RequestResponse',
    Payload=json.dumps(payload).encode('utf-8'),
)
decoded = response['Payload'].read().decode('utf-8')
data = json.loads(decoded)
body = json.loads(data['body'])

pp.pprint(body)
