import os
import tweepy as tw
import pandas as pd
import json
import csv
import re
from textblob import TextBlob
import string
import time

# Oauth keys
consumer_key= 'yours'
consumer_secret= 'yours'
access_token= 'yours'
access_token_secret= 'yours'

# Authentication with Twitter
auth = tw.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tw.API(auth, wait_on_rate_limit=True)

def scraptweets(search_words, date_since, numTweets, numRuns):

    db_tweets = pd.DataFrame(columns = ['username', 'acctdesc', 'location', 'following',
                                        'followers', 'totaltweets', 'usercreatedts', 'tweetcreatedts',
                                        'retweetcount', 'text', 'hashtags']
                                )
   
    program_start = time.time()
    
    for i in range(0, numRuns):
        start_run = time.time()
        tweets = tw.Cursor(api.search, q=search_words, lang="en", since=date_since, tweet_mode='extended').items(numTweets)
        tweet_list = [tweet for tweet in tweets]

        noTweets = 0
    
        for tweet in tweet_list:
            username = tweet.user.screen_name
            acctdesc = tweet.user.description
            location = tweet.user.location
            following = tweet.user.friends_count
            followers = tweet.user.followers_count
            totaltweets = tweet.user.statuses_count
            usercreatedts = tweet.user.created_at
            tweetcreatedts = tweet.created_at
            retweetcount = tweet.retweet_count
            hashtags = tweet.entities['hashtags']
            
            try:
                text = tweet.retweeted_status.full_text
            
            except AttributeError:
                text = tweet.full_text
# Add the 11 variables to the empty list - ith_tweet:
            
            ith_tweet = [username, acctdesc, location, following, followers, totaltweets,
                         usercreatedts, tweetcreatedts, retweetcount, text, hashtags]
# Append to dataframe - db_tweets
            db_tweets.loc[len(db_tweets)] = ith_tweet
# increase counter - noTweets  
            noTweets += 1
        
        # Run ended:
        end_run = time.time()
        duration_run = round((end_run-start_run)/60, 2)
        
        print('no. of tweets scraped for run {} is {}'.format(i + 1, noTweets))
        print('time take for {} run to complete is {} mins'.format(i+1, duration_run))
        
        time.sleep(1) #15 minute sleep time

# Once all runs have completed, save them to a single csv file:
    from datetime import datetime
    
    # Obtain timestamp in a readable format
    to_csv_timestamp = datetime.today().strftime('%Y%m%d_%H%M%S')

# Define working path and filename
    path = os.getcwd()
    filename = path + '/sample_data/' + to_csv_timestamp + '_covid_tweets.csv'
    
# Store dataframe in csv with creation date timestamp
    db_tweets.to_csv(filename, index = False)
    
    program_end = time.time()
    print('Scraping has completed!')
    print('Total time taken to scrap is {} minutes.'.format(round(program_end - program_start)/60, 2))


# Initialise these variables:
search_words = "#coronavirus OR #covid OR #covid-19 OR #covid19" # relevant words to COVID-19
date_since = "2019-12-31" # COVID-19 start date
numTweets = 5000 # number of tweets
numRuns = 1

# Call the function scraptweets
scraptweets(search_words, date_since, numTweets, numRuns)