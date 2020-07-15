import json
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from elasticsearch import Elasticsearch
from textblob import TextBlob

TCP_IP = 'localhost'
TCP_PORT = 9001


def sendToElasticsearch(tweet_data):
  es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
  if es.indices.exists(index="tweets") == False:
    print("INDEX DOES NOT EXIST. CREATING...")
    mapping = {
      "mappings": {
        "properties": {
          "sentiment": {"type": "keyword"},
          "created": {"type": "date"},
          "coords": {"type": "geo_point"},
          "country": {"type": "keyword"},
          "state": {"type": "keyword"}
        }
      }
    }
    es.indices.create(index="tweets", body=mapping)
  data = {
    'sentiment': tweet_data['sentiment'],
    'created': tweet_data['created'],
    'coords': tweet_data['location']['coords'],
    'country': tweet_data['location']['country'],
    'state': tweet_data['location']['state']
  }
  es.index(index="tweets", body=data)
  return True

def processTweet(tweet_json_string):
  tweet_data = json.loads(tweet_json_string)
  tweet = tweet_data['tweet']
  res = TextBlob(tweet)
  sentiment_score = res.sentiment.polarity
  if sentiment_score > 0:
    tweet_data['sentiment'] = "positive"
  elif sentiment_score == 0:
    tweet_data['sentiment'] = "neutral"
  else:
    tweet_data['sentiment'] = "negative"
  
  sendToElasticsearch(tweet_data)

# Pyspark
# create spark configuration
conf = SparkConf()
conf.setAppName('TwitterApp')
conf.setMaster('local[2]')

# create spark context with the above configuration
sc = SparkContext(conf=conf)
sc.setLogLevel("FATAL")

# create the Streaming Context from spark context with interval size 4 seconds
ssc = StreamingContext(sc, 4)
ssc.checkpoint("checkpoint_TwitterApp")

# read data from port 900
dataStream = ssc.socketTextStream(TCP_IP, TCP_PORT)
dataStream.foreachRDD(lambda rdd: rdd.foreach(processTweet))

ssc.start()
ssc.awaitTermination()
