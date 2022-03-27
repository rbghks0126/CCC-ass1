import pandas as pd
import json
from collections import defaultdict


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
