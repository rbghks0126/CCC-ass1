import pandas as pd
import json
from collections import defaultdict
import geopandas as gpd
from shapely.geometry import shape, MultiPolygon, Point, Polygon


def get_language_code_map(lang_code_path):
    '''
    returns a dictionary of {2-letter-code: language_name} mapping.
    '''
    # lang_code_path = './data/language-codes_csv.csv'
    lang_map = pd.read_csv(lang_code_path)
    # manually map two known 'outliers'
    lang_map.loc[lang_map.shape[0]] = ['in', 'Indonesian']
    lang_map.loc[lang_map.shape[0]] = ['iw', 'Former Hebrew']
    lang_map = {code: name for code, name in zip(
        lang_map['alpha2'], lang_map['English'])}
    return lang_map


def process_df(df, lang_map):
    '''
    returns 'formatted' df with nones/unds/nulls removed, and
    language codes mapped to language names.
    '''
    df = df.copy()
    # drop any rows with None/nan language or coordinates
    df = df.dropna(subset=['language', 'coordinates'])
    # drop undefined or null language rows
    df = df[df['language'] != 'und']
    df = df[df['language'] != 'null']
    df['coordinates'] = df['coordinates'].apply(
        lambda x: x['coordinates'])
    df['language'] = df['language'].apply(
        lambda x: lang_map[x] if x in lang_map else 'ERROR:' + str(x))
    df = df.dropna(subset=['coordinates', 'tweet_id'])
    return df


def count_unmatched(df):
    '''
    return a dictinoary with unmatched codes and their counts.
    '''
    unmatched = defaultdict(int)
    for i, row in df.iterrows():
        if row['language'][:5] == 'ERROR':
            unmatched[row['language'][-2:]] += 1
            df.drop(i, inplace=True)
    return unmatched


def count_total_tweets(tweets_with_cells):
    '''
    returns a df with total number of tweets for each grid
    '''
    return pd.DataFrame(tweets_with_cells.groupby(
        ['cells_id']).size()).reset_index().rename(columns={0: '#Total Tweets'})


def count_language_counts(tweets_with_cells):
    '''
    returns a df with number of tweets for each language for each cell_id
    '''
    return pd.DataFrame(tweets_with_cells.groupby(
        ['cells_id', 'language'], as_index=False).size())


def flatten_language_counts(df_language_counts):
    '''
    returns a dictionary with key:value being cell_id:{dict of language counts in that cell} 
    '''
    lang_counts = {}
    for cell_id in df_language_counts['cells_id']:
        df_cell = df_language_counts[df_language_counts['cells_id'] == cell_id]
        lang_counts[cell_id] = [(row['language'], row['size'])
                                for i, row in df_cell[df_cell['cells_id'] == cell_id].iterrows()]

        # for testing only
        # if cell_id == 14.0:
        #     for j in range(20):
        #         lang_counts[14.0].append(('Chinese', int(0.3*j)))

        lang_counts[cell_id] = [
            (sorted(lang_counts[cell_id], key=lambda item: item[1], reverse=True))[:10]]
    return lang_counts


def df_format_top_10(lang_counts):
    return pd.DataFrame(lang_counts).T.reset_index().rename(columns={'index': 'cells_id', 0: 'Top 10 languages & tweets'})


def matching_grid(df,grid):
    syd_grid_coorindates = []
    syd_grid_id = []
    for features in grid['features']:
        poly = Polygon(features['geometry']['coordinates'][0])
        syd_grid_coorindates += [poly]
        syd_grid_id.append(features['properties']['id'])

    geodata = gpd.GeoDataFrame()
    geodata['cells_id'] = syd_grid_id
    geodata['geometry'] = syd_grid_coorindates
    coords = [Point(xy) for xy in df['coordinates']]
    gdf_locations = gpd.GeoDataFrame(df, geometry=coords)
    # that requires rtree or pygeos package and can be installed using pip. rtree is not working for some reason, pygeos
    # work but gives out compatibility issues warnings with shapely packge. On windows, shapely was installed
    # indepenedenly to install geopandas. In linux env geopandas and all its dependencies will be installed using either
    # conda and pip and therefore this compatibility issue will be resolved. for more information, visit
    # https://github.com/geopandas/geopandas/issues/2355
    tweets_with_cells = gpd.sjoin(
        gdf_locations, geodata, how='left', predicate='within')
    no_match_df = tweets_with_cells.loc[tweets_with_cells.index_right.isna()]

    for index in no_match_df.index:
        cells_id_location_list = []
        cells = 0
        no_match_cell= no_match_df['geometry'].loc[index]
        for geocoord in geodata['geometry']:
            if geocoord.intersects(no_match_cell) == True:
                cells_id_location=geodata.loc[geodata['geometry'] == geocoord]
                cells_id_location_list.append(cells_id_location.iloc[0][0])
        if len(cells_id_location_list) == 1:
            cells = cells_id_location_list[0]
        elif len(cells_id_location_list) == 2:
            if abs(cells_id_location_list[0]-cells_id_location_list[1]) == 4:
                cells=min(cells_id_location_list)
        elif len(cells_id_location_list) == 4:
            temp_cell=cells_id_location_list.sort()
            cells=temp_cell[1]
        tweets_with_cells.loc[index,'cells_id']=cells
    tweets_with_cells = tweets_with_cells.dropna(subset=['cells_id'])
    df = tweets_with_cells[['tweet_id','language','coordinates','cells_id']]
    return(df)
