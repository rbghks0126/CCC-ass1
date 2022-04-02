### At the end, on the root nodeL:

## [df_total_tweets_2, df_total_tweets] -> replace with list of df_total_tweets gathered
df_cell_counts_agg = pd.concat([df_total_tweets_2, df_total_tweets]).groupby('cells_id').sum().reset_index()

## [df_language_counts, df_language_counts_2, df_language_counts_3] -> replace with list of df_language_counts gathered
df_lang_counts_agg = pd.concat([df_language_counts, df_language_counts_2, df_language_counts_3]).groupby(['cells_id','language']).sum().reset_index()
df_top10_agg = util.df_format_top_10(util.flatten_language_counts(df_lang_counts_agg))

df_final_output = df_cell_counts_agg.merge(df_top10_agg, on='cells_id')
print(df_final_output)