from nltk.sentiment.vader import SentimentIntensityAnalyzer
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from elasticsearch import Elasticsearch
import json
import nltk
from datetime import datetime


TCP_IP = 'localhost'
TCP_PORT = 9001


def processTweet(es, tweet):
    # Here, you should implement:
    # (i) Sentiment analysis,
    # (ii) Get data corresponding to place where the tweet was generate (using geopy or googlemaps)
    # (iii) Index the data using Elastic Search

    tweetData = tweet.split("::")

    if len(tweetData) > 1:

        # Parse the data sent from stream.py
        rawLocation = tweetData[0]
        text = tweetData[1]
        timeCreated = tweetData[2]

        # Convert 'timeCreated' to ISO format
        try:
            timeCreated = datetime.now(timeCreated, '%Y-%m-%d %H:%M:%S').isoformat()
        except: 
            timeCreated = datetime.now().isoformat(),

        # (i) Apply Sentiment analysis in "text"
        # Use NLTK to compute the sentiment of the tweet
        sentiment = sid.polarity_scores(text)
        sentiment['result'] = getSentimentResult(sentiment)

        # (ii) Get geolocation (state, country, lat, lon, etc...) from rawLocation
        print("\n\n=========================\n ")
        print("Text: ", text)
        location = json.loads(rawLocation)

        # (iii) Post the index on ElasticSearch or log your data in some other way (you are always free!!)
        doc = {
            'doc': {
                'text': text,
                'location': location,
                'sentiment': sentiment,
                'time_stamp':  timeCreated,
            }
        }
        print(doc)
        print()

        res = es.index(index="tweets_index", body=doc)


def getSentimentResult(sentiment):
    if sentiment['pos'] > sentiment['neg'] and sentiment['pos'] > sentiment['neu']:
        return "Positive"

    if sentiment['neg'] > sentiment['pos'] and sentiment['neg'] > sentiment['neu']:
        return "Negative"

    if sentiment['neu'] > sentiment['neg'] and sentiment['neu'] > sentiment['pos']:
        return "Neutral"


def processPartition(rdd):
    es = Elasticsearch(['elasticsearch:9200'])
    for record in rdd:
        processTweet(es, record)


# Setup NLTK
# nltk.download([
#     "twitter_samples",
#     "vader_lexicon"])
sid = SentimentIntensityAnalyzer()
print("Done setting up NLTK")


# Pyspark
# create spark configuration
conf = SparkConf()
conf.setAppName('TwitterApp')
conf.setMaster('local[2]')

# create spark context with the above configuration
sc = SparkContext(conf=conf)

# create the Streaming Context from spark context with interval size 4 seconds
ssc = StreamingContext(sc, 4)
ssc.checkpoint("checkpoint_TwitterApp")

# read data from port 900
dataStream = ssc.socketTextStream(TCP_IP, TCP_PORT)


dataStream.foreachRDD(lambda rdd: rdd.foreachPartition(processPartition))


ssc.start()
ssc.awaitTermination()
