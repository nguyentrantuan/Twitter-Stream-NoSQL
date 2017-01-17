
#Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json
import boto3
import boto.dynamodb2
from boto.dynamodb2.table import Table

from boto.dynamodb2 import connect_to_region
from boto.provider import Provider

aws_settings_provider = Provider('aws')
ACCESS_KEY='xxx'
SECRET_KEY='xxx'
REGION = "us-east-1"


def match(tweet, text):
    text=text.upper()
    tweet=tweet.upper()
    if (' '+tweet+' ') in text or '"'+tweet+'"' in text or tweet+'.' in text or '#'+tweet in text or ' http://'+tweet in text: 
        return True
    return False

#Variables that contains the user credentials to access Twitter API
consumer_key = 'xxx'
consumer_secret ='xxx'
access_token =  'xxx'
access_token_secret =  'xxx'

class CustomStreamListener(StreamListener):

    def on_error(self, status_code):
        print >> sys.stderr, 'Encountered error with status code:', status_code
        return True # Don't kill the stream

    def on_timeout(self):
        print >> sys.stderr, 'Timeout...'
        return True # Don't kill the stream

#This is a basic listener that just prints received tweets to stdout.
class StdOutListener(CustomStreamListener):

    def on_data(self, data):
        tweet = json.loads(data)
        temp=str(tweet['text'].encode('ascii','ignore'))
        for i in ["CIBC","RBC","TD bank","TDBank","BMO","Scotiabank"]: 
            if match(i,temp):
                try:
                    tweets.put_item(data={
                     'id': str(tweet['id']),
                     'username': tweet['user']['name'],
                     'screen_name': tweet['user']['screen_name'],
                     'tweet': str(tweet['text'].encode('ascii','ignore')),
                     'followers_count': tweet['user']['followers_count'],
                     'location': str(tweet['user']['location']),
                     'geo': str(tweet['geo']),
                     'created_at': tweet['created_at']
                     })
                except (AttributeError,Exception) as e:
                    print (e)
                break 
        return True

    def on_error(self, status):
        print (status)
        return True # Don't kill the stream


if __name__ == '__main__':

    
    conn = boto3.resource(
    'dynamodb',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    region_name='us-east-1',
    endpoint_url="https://dynamodb.us-east-1.amazonaws.com")

    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    tweets = conn.Table('tweet_bank2')
    sapi = Stream(auth, l)
    ### Filter users within Canada
    sapi.filter(languages=["en"], locations=[ -109.99 , 48.99 , -101.36 , 60.00 , -120.00 , 48.99 , -109.99 , 60.00 , -139.06 , 48.30 , -114.03 , 60.00, -120.68 , 51.64 ,  -61.08 , 83.11, -136.44 , 60.00 , -101.98 , 78.76, -141.00 , 60.00 , -123.81 , 69.65
                           , -95.16 , 41.66 ,  -74.34 , 56.86,  -79.76 , 44.99 ,  -57.10 , 62.59,  -69.06 , 44.60 ,  -63.77 , 48.07 ,  -66.32 , 43.42 ,  -59.68 , 47.03,  -67.80 , 46.61 ,  -52.61 , 60.37,  -61.50 , 47.18 ,  -60.13 , 47.80, -102.03 , 48.99 ,  -88.94 , 60.00
                           ,  -64.41 , 45.95 ,  -61.97 , 47.06])
       
