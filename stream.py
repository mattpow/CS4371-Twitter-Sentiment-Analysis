import re
import json
import socket
import tweepy
import preprocessor
import requests

# Enter your Twitter keys here!!!
ACCESS_TOKEN = "<REDACTED>"
ACCESS_SECRET = "<REDACTED>"
CONSUMER_KEY = "<REDACTED>"
CONSUMER_SECRET = "<REDACTED>"

GOOGLE_KEY = "<REDACTED>"

auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)

hashtag = '#covid19'

TCP_IP = 'localhost'
TCP_PORT = 9001

def preprocessing(tweet):
  # Add here your code to preprocess the tweets and  
  # remove Emoji patterns, emoticons, symbols & pictographs, transport & map symbols, flags (iOS), etc
  tweet = re.sub(r'[^\x00-\x7f]',r'', tweet)
  tweet = preprocessor.clean(tweet)
  return tweet

def getLocationData(location):
  request_url = "https://maps.googleapis.com/maps/api/geocode/json?address="+location+"&key="+GOOGLE_KEY
  res = requests.get(url=request_url)
  data = res.json()
  if len(data['results']) == 0:
    return
  geocode = data['results']
  if (len(geocode) > 0):
    coords = geocode[0]["geometry"]["location"]
    address = geocode[0]['address_components']
    locationData = {
      'coords': {
        'lat': coords['lat'],
        'lon': coords['lng'],
      },
      'state': None,
      'country': None
    }
    if address is not None:
      for el in address:
        if 'administrative_area_level_1' in el['types']:
          locationData['state'] = el['long_name']
        if 'country' in el['types']:
          locationData['country'] = el['long_name']
    return locationData


def getTweet(status):
  # You can explore fields/data other than location and the tweet itself. 
  # Check what else you could explore in terms of data inside Status object
  tweet = ''
  tweet_data = {'tweet': None, 'created': None, 'location': None}
  
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

  tweet_data['location'] = getLocationData(status.user.location)
  tweet_data['created'] = status.created_at.isoformat()
  tweet_data['tweet'] = preprocessing(tweet)
  return tweet_data


# create sockets
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
conn, addr = s.accept()

class MyStreamListener(tweepy.StreamListener):
  def on_status(self, status):
    if status.user.location is not None:
      tweet_data = getTweet(status)
      if tweet_data['location'] is not None:
        tweet_json = json.dumps(tweet_data) + "\n"
        conn.send(tweet_json.encode('utf-8'))
      return True

  def on_error(self, status_code):
      if status_code == 420:
          return False
      else:
          print(status_code)

myStream = tweepy.Stream(auth=auth, listener=MyStreamListener())
myStream.filter(track=[hashtag], languages=["en"])
