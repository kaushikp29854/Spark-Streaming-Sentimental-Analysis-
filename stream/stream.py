import tweepy
import socket
import re
import string
import json
from geopy.geocoders import Nominatim
# import preprocessor


# Enter your Twitter keys here!!!
ACCESS_TOKEN = "768549948388278272-sNq9WY7mrrsVwdbLUgdjN8oKwDSOB7T"
ACCESS_SECRET = "oCCgl3gGsXHHY67RiBnOwp7degd4fwUTx4bKiQUScFiMN"
CONSUMER_KEY = "u2GvGmAx1p1EsIrYEbrs83yGa"
CONSUMER_SECRET = "tUA2R1dI41L2cPs9dErQI63v8WZds4sk29cvayOrxZIcGm15Fd"


auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)


hashtags = ['#covid19', 'covid', 'coronavirus', 'hospital', 'recovery']

TCP_IP = 'localhost'
TCP_PORT = 9001


#  Variable "tweet" is a string containing the contents of the tweet being processed
def preprocessing(tweet):
    # print("Tweet [before] -> ", tweet)

    # Convert everything to lowercase
    tweet = tweet.lower()
    # Removing all non-ascii characters
    tweet = tweet.encode('ascii', errors='ignore').decode()
    # Remove all unnecessary white space
    tweet = re.sub('\s+', ' ', tweet)
    # gets rid of url links of http or https
    tweet = re.sub(r'https?://\S+', '', tweet)
    # gets rid of url links with or without www
    tweet = re.sub(r"www.[a-z]?.?(com)+|[a-z]+.(com)", '', tweet)
    # remove usernames
    tweet = re.sub('@[^\s]+', '', tweet)
    # remove the # in #hashtag
    tweet = re.sub(r'#([^\s]+)', r'\1', tweet)
    # remove punctuation
    tweet = tweet.translate(string.punctuation)
    return tweet


def getTweet(status):
    # You can explore fields/data other than location and the tweet itself.
    # Check what else you could explore in terms of data inside Status object

    tweet = ""
    location = ""

    location = status.user.location

    if hasattr(status, "retweeted_status"):  # Check if Retweet
        try:
            tweet = status.retweeted_status.extended_tweet["full_text"]
        except AttributeError:
            tweet = status.retweeted_status.text
    else:
        try:
            tweet = status.extended_tweet["full_text"]
        except AttributeError:
            tweet = status.text

    # print("location -> ", location)
    location = getLocation(location)
    print("location => ", location)

    return location, preprocessing(tweet)


def getLocation(location):
    filteredLoc = {}

    if (location != None):
        location = geolocator.geocode(location, addressdetails=True)
    if (location == None):
        return None

    location = location.raw
    if (location == None):
        return None

    if ('address' in location):
        # filteredLoc['adderess'] = location['address']

        if ('state' in location['address']):
            filteredLoc['state'] = location['address']['state']

        if ('city' in location['address']):
            filteredLoc['city'] = location['address']['city']

        if ('country' in location['address']):
            filteredLoc['country'] = location['address']['country']

    filteredLoc['location_coord'] = {}
    if ('lat' in location):
        filteredLoc['location_coord']['lat'] = float(location['lat'])

    if ('lon' in location):
        filteredLoc['location_coord']['lon'] = float(location['lon'])

    return filteredLoc


# create sockets
print("Waiting to connect with spark instance...")
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
conn, addr = s.accept()
print("Connected to spark streaming instance!\n")


class MyStreamListener(tweepy.StreamListener):

    def on_status(self, status):
        location, tweet = getTweet(status)
        print("Tweet received -> id: ", status.id_str, " | Created at: ", status.created_at, "\n")

        if (location != None and tweet != None):
            tweetLocation = json.dumps(location) + "::" + tweet + "::" + str(status.created_at) + "\n"
            conn.send(tweetLocation.encode('utf-8'))

        return True

    def on_error(self, status_code):
        if status_code == 420:
            return False
        else:
            print(status_code)

print("Waiting for twitter to send data...")

geolocator = Nominatim(user_agent="CS4371 Final Project")

while True:
    try:
        myStream = tweepy.Stream(auth=auth, listener=MyStreamListener())
        myStream.filter(track=hashtags, languages=["en"])
    except Exception as err:
        print(err)
