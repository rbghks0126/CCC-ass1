import time
from shapely.geometry import shape, MultiPolygon, Point, Polygon
import geopandas as gpd
import json
import pandas as pd
import util
import os
from mpi4py import MPI

COMM = MPI.COMM_WORLD
process_size = COMM.Get_size()
process_rank = COMM.Get_rank()
# time
if process_rank == 0:
    start_time = time.time()

twitter_rows = 0
##### Need to use the arguments to get file path and file name. This feels hardcoded for now.
twitter_data_file= open('./data/smallTwitter.json', 'r', encoding='utf-8')
chunk_sizes = []
if process_rank == 0:
    file_size= os.path.getsize('./data/smallTwitter.json')
    per_process = file_size/process_size
    twitter_data_rows=twitter_data_file.readline()

    # This is the code of calculating the number of tweets. Since we are doing things using the byte marker, calculating
    # the nnumber of rows feel redundant

    #####
    #if ',"rows":[\n' in twitter_data_rows:
    #    twitter_data_rows = twitter_data_rows.rstrip(',"rows":[\n')
    #    twitter_data_rows = twitter_data_rows+'}'
    #    twitter_data_rows = json.loads(twitter_data_rows)
    #    twitter_rows = int(twitter_data_rows['total_rows'])
    #    if 'offset' in twitter_data_rows:
    #        twitter_rows = twitter_rows + int(twitter_data_rows['offset'])
    #####

    # Below code could be put in the function.
    for index in range(process_size):
        startindex = twitter_data_file.tell()
        twitter_data_file.seek(startindex+per_process)
        twitter_data_file.readline()
        endindex=twitter_data_file.tell()
        if endindex > file_size:
            endindex=file_size
        chunk_sizes.append({'startindex':startindex,'endindex':endindex})
        twitter_data_file.readline()


# loading all the files here.
#twitter_data_file = open('./smallTwitter.json', 'r', encoding='utf-8')
#twitter_data_file_json = ijson.items(twitter_data_file,'rows.item')

# https://datahub.io/core/language-codes

# get code:language mapping dict
##### need to load the file through the argument.
lang_map = util.get_language_code_map('./data/language-codes_csv.csv')
# loading the sydGrid.json file
##### need to load the file through the argument.
with open('./data/sydGrid.json', 'r', encoding='utf-8') as f:
    syd_grid = json.load(f)

# This is so that all the process get to the same point in the code before getting the chunk sizes information.
COMM.Barrier()

chunk_sizes=COMM.scatter(chunk_sizes,root=0)
collective_df_tweets = pd.DataFrame()

startindex = chunk_sizes['startindex']
endindex = chunk_sizes['endindex']
twitter_data_file.seek(startindex)
while twitter_data_file.tell() < endindex:
    count = 0
    tweet_data = []
    while count < 1000:
        tweet=twitter_data_file.readline()
        #print(tweet)
        if tweet[-3:] == ']}\n' or tweet=="":
            tweet= tweet.rstrip(']}\n')
            break
        else:
            tweet = tweet.rstrip(',\n')
        #print(tweet)
        tweet_data_json = json.loads(tweet)
        tweet_data.append(tweet_data_json)
        count+=1

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
    collective_df_tweets=pd.concat([collective_df_tweets,df_tweets], ignore_index=True)
    #chunk +=1

COMM.Barrier()
print ('Processing complete at process', process_rank, '. Results from the process', process_rank, 'are\n ', len(collective_df_tweets.index._range) )
results_from_process = COMM.gather(collective_df_tweets,root=0)


if process_rank == 0:
    for result in results_from_process:
        collective_df_tweets = pd.concat([collective_df_tweets,result], ignore_index=True)

    ### this is to check for duplicates on the basis of tweet_id
    boolean = collective_df_tweets.duplicated(subset=['tweet_id'])
    collective_df_tweets = collective_df_tweets[~boolean]
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
