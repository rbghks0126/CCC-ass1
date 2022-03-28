import time
from shapely.geometry import shape, MultiPolygon, Point, Polygon
import geopandas as gpd
import json
import pandas as pd
import ijson
import util

# time
start_time = time.time()
twitter_rows = []
with open('./smallTwitter.json', 'r', encoding='utf-8') as f:
    twitter_data_rows=ijson.items(f,'total_rows')
    twitter_rows = [x for x in twitter_data_rows]

# loading all the files here.
twitter_data_file = open('./smallTwitter.json', 'r', encoding='utf-8')
twitter_data_file_json = ijson.items(twitter_data_file,'rows.item')

# https://datahub.io/core/language-codes

# get code:language mapping dict
lang_map = util.get_language_code_map('./data/language-codes_csv.csv')
# loading the sydGrid.json file
with open('./data/sydGrid.json', 'r', encoding='utf-8') as f:
    syd_grid = json.load(f)

chunk = 0
collective_df_tweets = pd.DataFrame()
while chunk < 5:
    count = 0
    tweet_data = []
    for tweet in twitter_data_file_json:
        tweet_data.append(tweet)
        if count == 1000:
            break
        else:
            count += 1

    #########################################
    # TO BE FILLED IN / REPLACED WITH STREAM/CHUNK PROCESSING?
    # this code block will change in multi-core setting
    # maybe output format won't be a dictionary in multi-core setting
    # can change accordingly once json stream/chunk parsing code is completed


    ########################################

    # extract key info from given dataset
    # (may be changed little depending on how json chunk is given
    # for the processor from code above)
    # will probably write it as a function once input format is
    # finalized

    rows_dict = {'tweet_id': [],
                 'language': [],
                 'coordinates': []}
    for tweet in tweet_data:
        rows_dict['tweet_id'].append(tweet['id'])
        rows_dict['language'].append(tweet['doc']['metadata']['iso_language_code'])
        rows_dict['coordinates'].append(tweet['doc']['coordinates'])
    df_tweets = pd.DataFrame(rows_dict)
    # output format should be a dataframe for the above code chunk

    # clean
    df_tweets = util.process_df(df_tweets, lang_map)
    # insert artificial row with inivalid language code
    # df_tweets.loc[df_tweets.shape[0]] = ['temp', 'zz', {
    #     'type': 'Point', 'coordinates': [151.211, 33]}]

    unmathced = util.count_unmatched(df_tweets)

    ############################################################
    # TO BE CHANGED.
    # EITHER RESOLVE BOUNDARY ISSUES BY KEEPING GEOPANDAS APPROACH,
    # OR GO WITH FOR LOOP APPROACH (WHICH IS EASIER PROBABLY?)
    # GRID LOCATION SHOULD BE APPENDED AS AN EXTRA COLUMN TO 'df_tweets'.
    # KEEP THE DF NAME THE SAME FOR CONVENIENCE (tweets_with_cells) (doesn't really matter)
    # also handy to keep 'cells_id' column name the same.

    #######
    # I have resolved the boundary issues by using the geopandas approach. There are some if-else statements that
    # are involved in the end that I could not get rid of. Everything is same as it was before. The subset of
    # the tweets which are not resolved by sjoin will go through a for loop and if-else statemnts to get resolved
    # according to the rules mentioned in assignment. This way only the tweets which are on the boundary will go
    # through the for loop and if-else statements.

    # Also grid id are appended to the original df_tweets in a way.

    # tweet-grid matching


    df_tweets = util.matching_grid(df_tweets,syd_grid)
    collective_df_tweets=collective_df_tweets.append(df_tweets, ignore_index=True)
    chunk +=1



############################################################


# final formatting
# count # of languages in each cell
df_total_tweets = util.count_total_tweets(collective_df_tweets)

# count # of occurences for each language in each cell
df_language_counts = util.count_language_counts(collective_df_tweets)

# flatten language counts for each cell
lang_counts = util.flatten_language_counts(df_language_counts)

# make df with top10 column with format shown in the ass. spec.
df_top10 = util.df_format_top_10(lang_counts)

# final output df format
df_final = df_total_tweets.merge(df_top10, on='cells_id')
df_final.to_csv('./results/output.csv', index=False)

end_time = time.time() - start_time
print(f'{end_time} secs')
