import json
import boto3
import botocore
from io import StringIO
import pandas as pd


#SETUP LOGGING
import logging
from pythonjsonlogger import jsonlogger

LOG = logging.getLogger()
LOG.setLevel(logging.DEBUG)
logHandler = logging.StreamHandler()
formatter = jsonlogger.JsonFormatter()
logHandler.setFormatter(formatter)
LOG.addHandler(logHandler)

# Setup Twitter Developer Account
import tweepy
from tweepy import OAuthHandler

API_KEY = "f9WiRMTrqXXDfKrxl0W5lC8wX"
API_SECRET = "glEbTlZl6C4NtITaTxdCIpZYTNfXO4LFW8bf2h5QMHMo9Nprs0"
# Replace the API_KEY and API_SECRET with your application's key and secret.
auth = OAuthHandler(API_KEY, API_SECRET)
api = tweepy.API(auth, wait_on_rate_limit=True,wait_on_rate_limit_notify=True)
if (not api):
    LOG.info("Can't Authenticate Twitter Account")
    
def get_tweets(query):
    '''Return the latest 30 tweets and most popular 30 tweets'''
    res_latest = api.search(query, result_type='recent', lang='en', count=30)
    res_popular = api.search(query, result_type='popular', lang='en', count=30)
    res_latest = [s.text for s in res_latest]
    res_popular = [s.text for s in res_popular]
    return res_latest + res_popular
    
    
# Setup Coinmarketcap Account
from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects

url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
parameters = {
  'limit':'1',
  'convert':'USD'
}
headers = {
  'Accepts': 'application/json',
  'X-CMC_PRO_API_KEY': '1f514daf-ff15-4b45-870e-12d08ee560b5',
}

# Configurations
crypto_dict = {
    'BTC': 1,
    'ETH': 2
}
price_thred = {
    'BTC': 8000,
    'ETH': 200
}


def get_price(name):
    session = Session()
    session.headers.update(headers)
    parameters['start'] = crypto_dict[name]
    try:
        response = session.get(url, params=parameters)
        data = json.loads(response.text)
        return data['data'][0]['quote']['USD']
    except (ConnectionError, Timeout, TooManyRedirects) as e:
        return None



#S3 BUCKET
REGION = "us-east-1"

### SQS Utils###
def sqs_queue_resource(queue_name):
    """Returns an SQS queue resource connection
    Usage example:
    In [2]: queue = sqs_queue_resource("dev-job-24910")
    In [4]: queue.attributes
    Out[4]:
    {'ApproximateNumberOfMessages': '0',
     'ApproximateNumberOfMessagesDelayed': '0',
     'ApproximateNumberOfMessagesNotVisible': '0',
     'CreatedTimestamp': '1476240132',
     'DelaySeconds': '0',
     'LastModifiedTimestamp': '1476240132',
     'MaximumMessageSize': '262144',
     'MessageRetentionPeriod': '345600',
     'QueueArn': 'arn:aws:sqs:us-west-2:414930948375:dev-job-24910',
     'ReceiveMessageWaitTimeSeconds': '0',
     'VisibilityTimeout': '120'}
    """

    sqs_resource = boto3.resource('sqs', region_name=REGION)
    log_sqs_resource_msg = "Creating SQS resource conn with qname: [%s] in region: [%s]" %\
     (queue_name, REGION)
    LOG.info(log_sqs_resource_msg)
    queue = sqs_resource.get_queue_by_name(QueueName=queue_name)
    return queue

def sqs_connection():
    """Creates an SQS Connection which defaults to global var REGION"""

    sqs_client = boto3.client("sqs", region_name=REGION)
    log_sqs_client_msg = "Creating SQS connection in Region: [%s]" % REGION
    LOG.info(log_sqs_client_msg)
    return sqs_client

def sqs_approximate_count(queue_name):
    """Return an approximate count of messages left in queue"""

    queue = sqs_queue_resource(queue_name)
    attr = queue.attributes
    num_message = int(attr['ApproximateNumberOfMessages'])
    num_message_not_visible = int(attr['ApproximateNumberOfMessagesNotVisible'])
    queue_value = sum([num_message, num_message_not_visible])
    sum_msg = """'ApproximateNumberOfMessages' and 'ApproximateNumberOfMessagesNotVisible' = *** [%s] *** for QUEUE NAME: [%s]""" %\
         (queue_value, queue_name)
    LOG.info(sum_msg)
    return queue_value

def delete_sqs_msg(queue_name, receipt_handle):

    sqs_client = sqs_connection()
    try:
        queue_url = sqs_client.get_queue_url(QueueName=queue_name)["QueueUrl"]
        delete_log_msg = "Deleting msg with ReceiptHandle %s" % receipt_handle
        LOG.info(delete_log_msg)
        response = sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
    except botocore.exceptions.ClientError as error:
        exception_msg = "FAILURE TO DELETE SQS MSG: Queue Name [%s] with error: [%s]" %\
            (queue_name, error)
        LOG.exception(exception_msg)
        return None

    delete_log_msg_resp = "Response from delete from queue: %s" % response
    LOG.info(delete_log_msg_resp)
    return response

def create_sentiment(row):
    """Uses AWS Comprehend to Create Sentiments on a DataFrame"""

    LOG.info(f"Processing {row}")
    comprehend = boto3.client(service_name='comprehend')
    payload = comprehend.detect_sentiment(Text=row, LanguageCode='en')
    LOG.debug(f"Found Sentiment: {payload}")
    sentiment = payload['Sentiment']
    return sentiment

### S3 ###

def write_s3(df, bucket, name):
    """Write S3 Bucket"""

    csv_buffer = StringIO()
    df.to_csv(csv_buffer)
    s3_resource = boto3.resource('s3')
    filename = f"{name}_sentiment.csv"
    res = s3_resource.Object(bucket, filename).\
        put(Body=csv_buffer.getvalue())
    LOG.info(f"result of write to bucket: {bucket} with:\n {res}")



def lambda_handler(event, context):
    """Entry Point for Lambda"""

    LOG.info(f"SURVEYJOB LAMBDA, event {event}, context {context}")
    receipt_handle  = event['Records'][0]['receiptHandle'] #sqs message
    #'eventSourceARN': 'arn:aws:sqs:us-east-1:561744971673:producer'
    event_source_arn = event['Records'][0]['eventSourceARN']

    names = [] #Captured from Queue

    # Process Queue
    for record in event['Records']:
        body = json.loads(record['body'])
        crypto_name = body['name']

        #Capture for processing
        names.append(crypto_name)

        extra_logging = {"body": body, "crypto_name":crypto_name}
        LOG.info(f"SQS CONSUMER LAMBDA, splitting sqs arn with value: {event_source_arn}",extra=extra_logging)
        qname = event_source_arn.split(":")[-1]
        extra_logging["queue"] = qname
        LOG.info(f"Attemping Deleting SQS receiptHandle {receipt_handle} with queue_name {qname}", extra=extra_logging)
        res = delete_sqs_msg(queue_name=qname, receipt_handle=receipt_handle)
        LOG.info(f"Deleted SQS receipt_handle {receipt_handle} with res {res}", extra=extra_logging)

    # Make Pandas dataframe with wikipedia snippts
    LOG.info(f"Creating dataframe with values: {names}")
    quotes_keys = ['price', 'volume_24h', 'percent_change_1h', 'percent_change_24h', 'percent_change_7d', 'market_cap','last_updated']
    res_crypto = []
    for name in names:
        tweets = get_tweets(name)
        quotes = get_price(name)
        quotes_res = [quotes[k] for k in quotes_keys]
        if quotes['price'] < price_thred[name]:
            sr = []
            for tweet in tweets:
                sa = create_sentiment(tweet)
                sr.append(sa)
            s_mf = max(set(sr), key=sr.count)
            LOG.info(f"Most frequent sentiment: {s_mf}")
        crypto_record = [name, s_mf] + quotes_res
        res_crypto.append(crypto_record)
        LOG.info(f"Save crypto info: {crypto_record}")
    df_col = ['price','sentiment'] + quotes_keys
    df = pd.DataFrame(res_crypto, columns=df_col)
    
    LOG.info(f"Sentiment from FANG companies: {df.to_dict()}")

    # Write result to S3
    write_s3(df=df, bucket="cryptomonitor", name=names)