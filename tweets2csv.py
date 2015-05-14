import json
import tweepy
import sys
import os
import ConfigParser
import time
import csv
import pprint

class CustomStreamListener(tweepy.StreamListener):
    def on_status(selif, status):
        global tweetcount, f, csvf, csvfields
        tweetcount += 1
        tweetid = status.id_str
        tweettext = status.text

        f.write(json.dumps(status.json))
        f.write('\n')

        csvvalues = []
        for csvfield in csvfields:
            if isinstance(status.__dict__[csvfield], basestring):
                csvvalues.append(status.__dict__[csvfield].encode('utf-8').replace('\n', '\\n'))
            else:
                csvvalues.append(str(status.__dict__[csvfield]))

        pprint.pprint(csvvalues)
        sys.exit()
        csvf.writerow(csvvalues)

        print tweetcount, tweettext

        def on_error(self, status_code):
            print >> sys.stderr, 'Encountered error with status code:', status_code
            return True # Don't kill the stream

        def on_timeout(self):
            print >> sys.stderr, 'Timeout...'
            return True # Don't kill the stream

retrievaltype = sys.argv[1]
eventname = sys.argv[2]
eventquery = []
locationquery = []
for i in range(3, len(sys.argv)):
    if sys.argv[i][:4] == "geo:":
        loc = [float(l) for l in sys.argv[i][4:].split(',')]
        for l in loc:
            locationquery.append(l)
    else:
        eventquery.append(sys.argv[i])

cwd = os.path.dirname(os.path.abspath(__file__))

Config = ConfigParser.ConfigParser()
Config.read('twitter.ini')
consumer_key = Config.get('Twitter', 'consumer_key')
consumer_secret = Config.get('Twitter', 'consumer_secret')
access_key = Config.get('Twitter', 'access_key')
access_secret = Config.get('Twitter', 'access_secret')
csvfields = [field.strip() for field in Config.get('Output', 'fields').split('\n')]

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_key, access_secret)
api = tweepy.API(auth)

tweetcount = 0

if not os.path.exists(os.path.join('data', eventname)):
    os.mkdir(os.path.join('data', eventname))
    os.mkdir(os.path.join('data', eventname, 'csv'))
    os.mkdir(os.path.join('data', eventname, 'tweets'))

now = time.time()
f = open(os.path.join('data', eventname, 'tweets', str(now) + '.json'), 'w')
csvf = csv.writer(open(os.path.join('data', eventname, 'csv', str(now) + '.csv'), 'w'), delimiter=',', quotechar='"')
csvf.writerow(csvfields)

if retrievaltype == 'stream':
    sapi = tweepy.streaming.Stream(auth, CustomStreamListener(), timeout = 60)
    sapi.filter(track=eventquery,locations=locationquery)
elif 'search' in retrievaltype:
    if retrievaltype == 'search-recent':
        resulttype = 'recent'
    else:
        resulttype = 'popular'
    tweetsearch = tweepy.Cursor(api.search,q=",".join(eventquery),rpp=100,result_type="recent",include_entities=True).items()
    tweetids = {}
    while True:
        try:
            tweet = tweetsearch.next()
            tweetids[tweet.id_str] = 1
            print str("Recent: " + str(len(tweetids))) + " - " + ",".join(eventquery) + " - " + str(tweet.id_str)

            f.write(json.dumps(tweet.json))

            csvvalues = []
            for csvfield in csvfields:
                if isinstance(tweet.__dict__[csvfield], basestring):
                    csvvalues.append(tweet.__dict__[csvfield].encode('utf-8').replace('\n', '\\n'))
                else:
                    csvvalues.append(str(tweet.__dict__[csvfield]))
            csvf.writerow(csvvalues)

        except tweepy.TweepError:
            print "Sleeping..."
            time.sleep(60 * 15)
            continue
        except StopIteration:
            break
