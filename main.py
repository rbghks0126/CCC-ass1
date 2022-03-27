import time
from shapely.geometry import shape, MultiPolygon, Point, Polygon
import geopandas as gpd
import json
import pandas as pd

import util

# time
start_time = time.time()

# https://datahub.io/core/language-codes

# get code:language mapping dict
lang_map = util.get_language_code_map('./data/language-codes_csv.csv')

#########################################
# TO BE FILLED IN / REPLACED WITH STREAM/CHUNK PROCESSING?
# this code block will change in multi-core setting
# maybe output format won't be a dictionary in multi-core setting
# can change accordingly once json stream/chunk parsing code is completed
with open('./data/smallTwitter.json', 'r', encoding='utf-8') as f:
    twitter_data = json.load(f)

########################################

# extract key info from given dataset
# (may be changed little depending on how json chunk is given
# for the processor from code above)
# will probably write it as a function once input format is
# finalized
rows_dict = {'tweet_id': [],
             'language': [],
             'coordinates': []}
for i, tweet in enumerate(twitter_data['rows']):
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

# tweet-grid matching
with open('./data/sydGrid.json', 'r', encoding='utf-8') as f:
    syd_grid = json.load(f)

syd_grid_coorindates = []
syd_grid_id = []
for features in syd_grid['features']:
    poly = Polygon(features['geometry']['coordinates'][0])
    syd_grid_coorindates += [poly]
    syd_grid_id.append(features['properties']['id'])

geodata = gpd.GeoDataFrame()
geodata['cells_id'] = syd_grid_id
geodata['geometry'] = syd_grid_coorindates
coords = [Point(xy) for xy in df_tweets['coordinates']]
gdf_locations = gpd.GeoDataFrame(df_tweets, geometry=coords)
# that requires rtree or pygeos package and can be installed using pip. rtree is not working for some reason, pygeos
# work but gives out compatibility issues warnings with shapely packge. On windows, shapely was installed
# indepenedenly to install geopandas. In linux env geopandas and all its dependencies will be installed using either
# conda and pip and therefore this compatibility issue will be resolved. for more information, visit
# https://github.com/geopandas/geopandas/issues/2355
tweets_with_cells = gpd.sjoin(
    gdf_locations, geodata, how='left', predicate='within')
tweets_with_cells = tweets_with_cells.dropna(subset=['cells_id'])
############################################################


# final formatting
# count # of languages in each cell
df_total_tweets = util.count_total_tweets(tweets_with_cells)

# count # of occurences for each language in each cell
df_language_counts = util.count_language_counts(tweets_with_cells)

# flatten language counts for each cell
lang_counts = util.flatten_language_counts(df_language_counts)

# make df with top10 column with format shown in the ass. spec.
df_top10 = util.df_format_top_10(lang_counts)

# final output df format
df_final = df_total_tweets.merge(df_top10, on='cells_id')
df_final.to_csv('./results/output.csv', index=False)

end_time = time.time() - start_time
print(f'{end_time} secs')
