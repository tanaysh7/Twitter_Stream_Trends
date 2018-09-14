
from pyspark import SparkContext
import tweepy
import time
import random




sc = SparkContext("local[*]",appName="TwitterStreaming")
sc.setLogLevel("ERROR")





auth = tweepy.OAuthHandler('', '') #Insert your Oauth Key
auth.set_access_token('', '')      #Insert your Access token
api = tweepy.API(auth)



class MyStreamListener(tweepy.StreamListener):

    def on_status(self, status):
        add_to_sample(status)
        
#['author', 'contributors', 'coordinates', 'created_at', 'destroy', 'display_text_range', 'entities', 'extended_tweet', 'favorite', 'favorite_count', 'favorited', 'filter_level', 'geo', 'id', 'id_str', 'in_reply_to_screen_name', 'in_reply_to_status_id', 'in_reply_to_status_id_str', 'in_reply_to_user_id', 'in_reply_to_user_id_str', 'is_quote_status', 'lang', 'parse', 'parse_list', 'place', 'quote_count', 'reply_count', 'retweet', 'retweet_count', 'retweeted', 'retweets', 'source', 'source_url', 'text', 'timestamp_ms', 'truncated', 'user']



myStreamListener = MyStreamListener()
myStream = tweepy.Stream(auth = api.auth, listener=myStreamListener)


def top_hashtags(S):
    alltags=dict()
    topdict=[]

    for hashlist in S:
        for i in hashlist:
            if i in alltags:
                alltags[i]+=1
            else:
                alltags[i]=1
    top=5
    return sorted(alltags.iteritems(), key=lambda (k,v): (v,k),reverse=True)[:top]
            


#trends1 = api.trends_place(1)

S=[]
K=100
global counter
global twt_length
counter=0
twt_length=0
def add_to_sample(tweet):
    global counter
    global S
    global twt_length
    counter+=1
    twt_length+=len(tweet.text)
    if(len(S)<K):
        S.append([i['text'] for i in tweet.entities.get('hashtags')])
        #twt_length+=len(tweet.text) 
    else:
            s = int(random.random() * counter)
            if s < K:
                S[s] = [i['text'] for i in tweet.entities.get('hashtags')] 
                print 'The number of the twitter from beginning:' +str(counter)
                print 'Top 5 hot hashtags:' 
                for i in top_hashtags(S):
                    print i[0].encode('utf-8')+' : '+str(i[1])
                
                print 'The average length of the twitter ' + str(float(twt_length)/counter)
                print ' '
    

    


myStream.filter(track=['#'])

