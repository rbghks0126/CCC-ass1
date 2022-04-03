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

def file_chunks(twitter_data_file_path, process_size):
    chunk_sizes = []
    with open(twitter_data_file_path, 'r', encoding='utf-8') as twitter_data_file_path:
        file_size= os.path.getsize(twitter_data_file_path)
        per_process = file_size/process_size
        twitter_data_rows=twitter_data_file.readline()
        for index in range(process_size):
            startindex = twitter_data_file.tell()
            twitter_data_file.seek(startindex+per_process)
            twitter_data_file.readline()
            endindex=twitter_data_file.tell()
            if endindex > file_size:
                endindex=file_size
            chunk_sizes.append({'startindex':startindex,'endindex':endindex})
            twitter_data_file.readline()
    return(chunk_sizes)

def matching_grid(df,grid):
    syd_grid_coorindates = []
    syd_grid_id = []
    for features in grid['features']:
        poly = Polygon(features['geometry']['coordinates'][0])
        syd_grid_coorindates += [poly]
        syd_grid_id.append(features['properties']['id'])
    #changing the IDs to the alphabetical notation as per the desired output.
    # I have hard-coded this, because all the possible implementation seems very hardcoded to me.
    alpha_grid_id = ['C4','B4','A4','D3','C3','B3','A3','D2','C2','B2','A2','D1','C1','B1','A1','D4']

    geodata = gpd.GeoDataFrame()
    geodata['cells_id_numeric'] = syd_grid_id
    geodata['geometry'] = syd_grid_coorindates
    geodata['cells_id'] = alpha_grid_id
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
        #alpha_cells_id_location_list=[]
        cells = 0
        no_match_cell= no_match_df['geometry'].loc[index]
        for geocoord in geodata['geometry']:
            if geocoord.intersects(no_match_cell) == True:
                cells_id_location=geodata.loc[geodata['geometry'] == geocoord]
                cells_id_location_list.append(cells_id_location.iloc[0][0])
                #alpha_cells_id_location_list.append(cells_id_location.iloc[0][2])
        if len(cells_id_location_list) == 1:
            cells = cells_id_location_list[0]
            tweets_with_cells.loc[index,'cells_id_numeric']=cells
            tweets_with_cells.loc[index, 'cells_id']=geodata[geodata['cells_id_numeric']==cells]['cells_id'].values[0]
        elif len(cells_id_location_list) == 2:
            if abs(cells_id_location_list[0]-cells_id_location_list[1]) == 4:
                cells=min(cells_id_location_list)
            elif abs(cells_id_location_list[0]-cells_id_location_list[1]) == 1:
                cells=max(cells_id_location_list)
            tweets_with_cells.loc[index,'cells_id_numeric']=cells
            tweets_with_cells.loc[index, 'cells_id']=geodata[geodata['cells_id_numeric']==cells]['cells_id'].values[0]
        elif len(cells_id_location_list) == 4:
            temp_cell=cells_id_location_list.sort()
            cells=temp_cell[1]
            tweets_with_cells.loc[index,'cells_id_numeric']=cells
            tweets_with_cells.loc[index, 'cells_id']=geodata[geodata['cells_id_numeric']==cells]['cells_id'].values[0]
    tweets_with_cells = tweets_with_cells.dropna(subset=['cells_id_numeric'])
    df = tweets_with_cells[['tweet_id','language','coordinates','cells_id','cells_id_numeric']]
    return(df)
