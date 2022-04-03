import time
from shapely.geometry import shape, MultiPolygon, Point, Polygon
import geopandas as gpd
import json
import pandas as pd
import util
from mpi4py import MPI
import os
import argparse


parser = argparse.ArgumentParser(description='Count the number of languages in a twitter dataset')
parser.add_argument('--twitter_data_file_path', type=str, default='./smallTwitter.json', help='path to the twitter dataset file in the json format')
parser.add_argument('--grid_file_path', type=str, default='./data/sydGrid.json', help='path to the grid file which contains the grid information')
parser.add_argument('--language_file', type=str, default='./data/language-codes_csv.csv', help='path to the language codes file which contains the information about different language codes')
parser.add_argument('--batch_size', type=int, default=1000, help='Number of tweets that a subprocesses handles at a time')
parser.add_argument('--out_file', type=str, default='./results/output.csv', help='the file name for the results along with the path')
args = parser.parse_args()



COMM = MPI.COMM_WORLD
process_size = COMM.Get_size()
process_rank = COMM.Get_rank()
# time
if process_rank == 0:
    start_time = time.time()
twitter_data_file_path=args.twitter_data_file_path

twitter_data_file= open(twitter_data_file_path, 'r', encoding='utf-8')

chunk_sizes = []
# getting the chunks' byte markers
if process_rank == 0:
    chunk_sizes = util.file_chunks(twitter_data_file_path, process_size)


# Loading the lnaguage codes
lang_map = util.get_language_code_map(args.language_file)
# loading the sydGrid.json file

with open(args.grid_file_path, 'r', encoding='utf-8') as f:
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
    while count < args.batch_size:
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

    unmathced = util.count_unmatched(df_tweets)


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
    df_final.to_csv(args.out_file, index=False)

    end_time = time.time() - start_time
    print(f'{end_time} secs')
