import pandas as pd
import numpy as np

def calc_adj_on_ice_shots(pbp_df):
    '''
    function to calculate on ice shot metrics for all situations

    Inputs:
    pbp_df - play by play dataframe

    Outputs:
    on_ice_shots_df - dataframe of on ice shot events while player was on ice
    '''

    home_shots_for_1 = pbp_df[pbp_df.is_home == 1]\
                          .groupby(['season', 'game_id', 'date',
                                    'homeplayer1_id', 'homeplayer1'])\
                                  ['adj_corsi', 'adj_fenwick',
                                   'is_shot', 'is_goal', 'adj_xg'].sum().reset_index()

    home_shots_against_1 = pbp_df[pbp_df.is_home == 0]\
                          .groupby(['season', 'game_id', 'date',
                                    'homeplayer1_id', 'homeplayer1'])\
                                  ['adj_corsi', 'adj_fenwick',
                                   'is_shot', 'is_goal', 'adj_xg'].sum().reset_index()

    home_shots_for_2 = pbp_df[pbp_df.is_home == 1]\
                          .groupby(['season', 'game_id', 'date',
                                    'homeplayer2_id', 'homeplayer2'])\
                                  ['adj_corsi', 'adj_fenwick',
                                   'is_shot', 'is_goal', 'adj_xg'].sum().reset_index()

    home_shots_against_2 = pbp_df[pbp_df.is_home == 0]\
                          .groupby(['season', 'game_id', 'date',
                                    'homeplayer2_id', 'homeplayer2'])\
                                  ['adj_corsi', 'adj_fenwick',
                                   'is_shot', 'is_goal', 'adj_xg'].sum().reset_index()

    home_shots_for_3 = pbp_df[pbp_df.is_home == 1]\
                          .groupby(['season', 'game_id', 'date',
                                    'homeplayer3_id', 'homeplayer3'])\
                                  ['adj_corsi', 'adj_fenwick',
                                   'is_shot', 'is_goal','adj_xg'].sum().reset_index()

    home_shots_against_3 = pbp_df[pbp_df.is_home == 0]\
                          .groupby(['season', 'game_id', 'date',
                                    'homeplayer3_id', 'homeplayer3'])\
                                  ['adj_corsi', 'adj_fenwick',
                                   'is_shot', 'is_goal','adj_xg'].sum().reset_index()

    home_shots_for_4 = pbp_df[pbp_df.is_home == 1]\
                          .groupby(['season', 'game_id', 'date',
                                    'homeplayer4_id', 'homeplayer4'])\
                                  ['adj_corsi', 'adj_fenwick',
                                   'is_shot', 'is_goal', 'adj_xg'].sum().reset_index()

    home_shots_against_4 = pbp_df[pbp_df.is_home == 0]\
                          .groupby(['season', 'game_id', 'date',
                                    'homeplayer4_id', 'homeplayer4'])\
                                  ['adj_corsi', 'adj_fenwick',
                                   'is_shot', 'is_goal', 'adj_xg'].sum().reset_index()

    home_shots_for_5 = pbp_df[pbp_df.is_home == 1]\
                          .groupby(['season', 'game_id', 'date',
                                    'homeplayer5_id', 'homeplayer5'])\
                                  ['adj_corsi', 'adj_fenwick',
                                   'is_shot', 'is_goal', 'adj_xg'].sum().reset_index()

    home_shots_against_5 = pbp_df[pbp_df.is_home == 0]\
                          .groupby(['season', 'game_id', 'date',
                                    'homeplayer5_id', 'homeplayer5'])\
                                  ['adj_corsi', 'adj_fenwick',
                                   'is_shot', 'is_goal', 'adj_xg'].sum().reset_index()

    home_shots_for_6 = pbp_df[pbp_df.is_home == 1]\
                          .groupby(['season', 'game_id', 'date',
                                    'homeplayer6_id', 'homeplayer6'])\
                                  ['adj_corsi', 'adj_fenwick',
                                   'is_shot', 'is_goal', 'adj_xg'].sum().reset_index()

    home_shots_against_6 = pbp_df[pbp_df.is_home == 0]\
                          .groupby(['season', 'game_id', 'date',
                                    'homeplayer6_id', 'homeplayer6'])\
                                  ['adj_corsi', 'adj_fenwick',
                                   'is_shot', 'is_goal', 'adj_xg'].sum().reset_index()

#refactor this into a for loop and store all the dataframes into a list probably
#can do the same with the code above
    home_shots_for_1.columns = ['season', 'game_id', 'date', 'player_id',
                                'player_name', 'CF', 'FF', 'SF', 'GF', 'xgf']
    home_shots_against_1.columns = ['season', 'game_id', 'date', 'player_id',
                                    'player_name', 'CA', 'FA', 'SA', 'GA', 'xga']

    home_onice_1 = home_shots_for_1.merge(home_shots_against_1,
                                          on=['season', 'game_id', 'date',
                                              'player_id', 'player_name'],
                                          how='outer')

    home_onice_1 = home_onice_1.fillna(0)

    home_onice_1.loc[:, ('season', 'game_id', 'player_id', 'SF',
                         'GF', 'SA', 'GA')] = home_onice_1\
                    .loc[:, ('season', 'game_id', 'player_id', 'SF',
                             'GF', 'SA', 'GA')].astype(int)

    home_shots_for_2.columns = ['season', 'game_id', 'date', 'player_id',
                                'player_name', 'CF', 'FF', 'SF', 'GF', 'xgf']
    home_shots_against_2.columns = ['season', 'game_id', 'date', 'player_id',
                                    'player_name', 'CA', 'FA', 'SA', 'GA', 'xga']

    home_onice_2 = home_shots_for_2.merge(home_shots_against_2,
                                          on=['season', 'game_id', 'date',
                                              'player_id', 'player_name'],
                                          how='outer')

    home_onice_2 = home_onice_2.fillna(0)

    home_onice_2.loc[:, ('season', 'game_id', 'player_id', 'SF',
                         'GF','SA', 'GA')] = home_onice_2\
                    .loc[:, ('season', 'game_id', 'player_id', 'SF',
                             'GF','SA', 'GA')].astype(int)

    home_shots_for_3.columns = ['season', 'game_id', 'date', 'player_id',
                                'player_name', 'CF', 'FF', 'SF', 'GF', 'xgf']
    home_shots_against_3.columns = ['season', 'game_id', 'date', 'player_id',
                                    'player_name', 'CA', 'FA', 'SA', 'GA', 'xga']


    home_onice_3 = home_shots_for_3.merge(home_shots_against_3,
                                          on=['season', 'game_id', 'date',
                                              'player_id', 'player_name'],
                                          how='outer')

    home_onice_3 = home_onice_3.fillna(0)

    home_onice_3.loc[:, ('season', 'game_id', 'player_id','SF',
                         'GF', 'SA', 'GA')] = home_onice_3\
                    .loc[:, ('season', 'game_id', 'player_id', 'SF',
                             'GF', 'SA', 'GA')].astype(int)
    home_shots_for_4.columns = ['season', 'game_id', 'date', 'player_id',
                                'player_name', 'CF', 'FF', 'SF', 'GF', 'xgf']
    home_shots_against_4.columns = ['season', 'game_id', 'date', 'player_id',
                                    'player_name', 'CA', 'FA', 'SA', 'GA', 'xga']

    home_onice_4 = home_shots_for_4.merge(home_shots_against_4,
                                          on=['season', 'game_id', 'date',
                                              'player_id', 'player_name'],
                                          how='outer')

    home_onice_4 = home_onice_4.fillna(0)

    home_onice_4.loc[:, ('season', 'game_id', 'player_id', 'SF',
                         'GF', 'SA', 'GA')] = home_onice_4\
                    .loc[:, ('season', 'game_id', 'player_id', 'SF',
                             'GF', 'SA', 'GA')].astype(int)

    home_shots_for_5.columns = ['season', 'game_id', 'date', 'player_id',
                                'player_name', 'CF', 'FF', 'SF', 'GF', 'xgf']
    home_shots_against_5.columns = ['season', 'game_id', 'date', 'player_id',
                                    'player_name', 'CA', 'FA', 'SA', 'GA', 'xga']

    home_onice_5 = home_shots_for_5.merge(home_shots_against_5,
                                          on=['season', 'game_id', 'date',
                                              'player_id', 'player_name'],
                                          how='outer')

    home_onice_5 = home_onice_5.fillna(0)

    home_onice_5.loc[:, ('season', 'game_id', 'player_id', 'SF',
                         'GF','SA', 'GA')] = home_onice_5\
                    .loc[:, ('season', 'game_id', 'player_id', 'CF', 'FF', 'SF',
                             'GF', 'CA', 'FA', 'SA', 'GA')].astype(int)

    home_shots_for_6.columns = ['season', 'game_id', 'date', 'player_id',
                                'player_name', 'CF', 'FF', 'SF', 'GF', 'xgf']
    home_shots_against_6.columns = ['season', 'game_id', 'date', 'player_id',
                                    'player_name', 'CA', 'FA', 'SA', 'GA', 'xga']

    home_onice_6 = home_shots_for_6.merge(home_shots_against_6,
                                          on=['season', 'game_id', 'date',
                                              'player_id', 'player_name'],
                                          how='outer')

    home_onice_6 = home_onice_6.fillna(0)

    home_onice_6.loc[:, ('season', 'game_id', 'player_id', 'SF',
                         'GF', 'SA', 'GA')] = home_onice_6\
                    .loc[:, ('season', 'game_id', 'player_id', 'SF',
                             'GF', 'SA', 'GA')].astype(int)

    home_onice_list = [home_onice_1, home_onice_2, home_onice_3, home_onice_4,
                       home_onice_5, home_onice_6]

    home_metrics = pd.concat(home_onice_list)

    home_metrics = home_metrics.groupby(['season', 'game_id', 'date',
                                         'player_id', 'player_name'])\
                                ['CF', 'CA', 'FF', 'FA', 'SF', 'SA', 'GF', 'GA', 'xgf', 'xga'].sum().reset_index()

    away_shots_for_1 = pbp_df[pbp_df.is_home == 0]\
                          .groupby(['season', 'game_id', 'date',
                                    'awayplayer1_id', 'awayplayer1'])\
                                  ['adj_corsi', 'adj_fenwick',
                                   'is_shot', 'is_goal', 'adj_xg'].sum().reset_index()

    away_shots_against_1 = pbp_df[pbp_df.is_home == 1]\
                          .groupby(['season', 'game_id', 'date',
                                    'awayplayer1_id', 'awayplayer1'])\
                                  ['adj_corsi', 'adj_fenwick',
                                   'is_shot', 'is_goal', 'adj_xg'].sum().reset_index()

    away_shots_for_2 = pbp_df[pbp_df.is_home == 0]\
                          .groupby(['season', 'game_id', 'date',
                                    'awayplayer2_id', 'awayplayer2'])\
                                  ['adj_corsi', 'adj_fenwick',
                                   'is_shot', 'is_goal', 'adj_xg'].sum().reset_index()

    away_shots_against_2 = pbp_df[pbp_df.is_home == 1]\
                          .groupby(['season', 'game_id', 'date',
                                    'awayplayer2_id', 'awayplayer2'])\
                                  ['adj_corsi', 'adj_fenwick',
                                   'is_shot', 'is_goal', 'adj_xg'].sum().reset_index()

    away_shots_for_3 = pbp_df[pbp_df.is_home == 0]\
                          .groupby(['season', 'game_id', 'date',
                                    'awayplayer3_id', 'awayplayer3'])\
                                  ['adj_corsi', 'adj_fenwick',
                                   'is_shot', 'is_goal', 'adj_xg'].sum().reset_index()

    away_shots_against_3 = pbp_df[pbp_df.is_home == 1]\
                          .groupby(['season', 'game_id', 'date',
                                    'awayplayer3_id', 'awayplayer3'])\
                                  ['adj_corsi', 'adj_fenwick',
                                   'is_shot', 'is_goal', 'adj_xg'].sum().reset_index()

    away_shots_for_4 = pbp_df[pbp_df.is_home == 0]\
                          .groupby(['season', 'game_id', 'date',
                                    'awayplayer4_id', 'awayplayer4'])\
                                  ['adj_corsi', 'adj_fenwick',
                                   'is_shot', 'is_goal', 'adj_xg'].sum().reset_index()

    away_shots_against_4 = pbp_df[pbp_df.is_home == 1]\
                          .groupby(['season', 'game_id', 'date',
                                    'awayplayer4_id', 'awayplayer4'])\
                                  ['adj_corsi', 'adj_fenwick',
                                   'is_shot', 'is_goal', 'adj_xg'].sum().reset_index()

    away_shots_for_5 = pbp_df[pbp_df.is_home == 0]\
                          .groupby(['season', 'game_id', 'date',
                                    'awayplayer5_id', 'awayplayer5'])\
                                  ['adj_corsi', 'adj_fenwick',
                                   'is_shot', 'is_goal', 'adj_xg'].sum().reset_index()

    away_shots_against_5 = pbp_df[pbp_df.is_home == 1]\
                          .groupby(['season', 'game_id', 'date',
                                    'awayplayer5_id', 'awayplayer5'])\
                                  ['adj_corsi', 'adj_fenwick',
                                   'is_shot', 'is_goal', 'adj_xg'].sum().reset_index()

    away_shots_for_6 = pbp_df[pbp_df.is_home == 0]\
                          .groupby(['season', 'game_id', 'date',
                                    'awayplayer6_id', 'awayplayer6'])\
                                  ['adj_corsi', 'adj_fenwick',
                                   'is_shot', 'is_goal', 'adj_xg'].sum().reset_index()

    away_shots_against_6 = pbp_df[pbp_df.is_home == 1]\
                          .groupby(['season', 'game_id', 'date',
                                    'awayplayer6_id', 'awayplayer6'])\
                                  ['adj_corsi', 'adj_fenwick',
                                   'is_shot', 'is_goal', 'adj_xg'].sum().reset_index()

    away_shots_for_1.columns = ['season', 'game_id', 'date', 'player_id',
                                'player_name', 'CF', 'FF', 'SF', 'GF', 'xgf']
    away_shots_against_1.columns = ['season', 'game_id', 'date', 'player_id',
                                    'player_name', 'CA', 'FA', 'SA', 'GA', 'xga']

    away_onice_1 = away_shots_for_1.merge(away_shots_against_1,
                                          on=['season', 'game_id', 'date',
                                              'player_id', 'player_name'],
                                          how='outer')

    away_onice_1 = away_onice_1.fillna(0)

    away_onice_1.loc[:, ('season', 'game_id', 'player_id', 'SF',
                         'GF',  'SA', 'GA')] = away_onice_1\
                    .loc[:, ('season', 'game_id', 'player_id', 'CF', 'FF', 'SF',
                             'GF', 'CA', 'FA', 'SA', 'GA')].astype(int)

    away_shots_for_2.columns = ['season', 'game_id', 'date', 'player_id',
                                'player_name', 'CF', 'FF', 'SF', 'GF', 'xgf']
    away_shots_against_2.columns = ['season', 'game_id', 'date', 'player_id',
                                    'player_name', 'CA', 'FA', 'SA', 'GA', 'xga']

    away_onice_2 = away_shots_for_2.merge(away_shots_against_2,
                                          on=['season', 'game_id', 'date',
                                              'player_id', 'player_name'],
                                          how='outer')

    away_onice_2 = away_onice_2.fillna(0)

    away_onice_2.loc[:, ('season', 'game_id', 'player_id', 'SF',
                         'GF',  'SA', 'GA')] = away_onice_2\
                    .loc[:, ('season', 'game_id', 'player_id',  'SF',
                             'GF', 'SA', 'GA')].astype(int)

    away_shots_for_3.columns = ['season', 'game_id', 'date', 'player_id',
                                'player_name', 'CF', 'FF', 'SF', 'GF', 'xgf']
    away_shots_against_3.columns = ['season', 'game_id', 'date', 'player_id',
                                    'player_name', 'CA', 'FA', 'SA', 'GA', 'xga']


    away_onice_3 = away_shots_for_3.merge(away_shots_against_3,
                                          on=['season', 'game_id', 'date',
                                              'player_id', 'player_name'],
                                          how='outer')

    away_onice_3 = away_onice_3.fillna(0)

    away_onice_3.loc[:, ('season', 'game_id', 'player_id','SF',
                         'GF', 'SA', 'GA')] = away_onice_3\
                    .loc[:, ('season', 'game_id', 'player_id', 'SF',
                             'GF', 'SA', 'GA')].astype(int)

    away_shots_for_4.columns = ['season', 'game_id', 'date', 'player_id',
                                'player_name', 'CF', 'FF', 'SF', 'GF', 'xgf']
    away_shots_against_4.columns = ['season', 'game_id', 'date', 'player_id',
                                    'player_name', 'CA', 'FA', 'SA', 'GA', 'xga']

    away_onice_4 = away_shots_for_4.merge(away_shots_against_4,
                                          on=['season', 'game_id', 'date',
                                              'player_id', 'player_name'],
                                          how='outer')

    away_onice_4 = away_onice_4.fillna(0)

    away_onice_4.loc[:, ('season', 'game_id', 'player_id', 'SF',
                         'GF', 'SA', 'GA')] = away_onice_4\
                    .loc[:, ('season', 'game_id', 'player_id', 'SF',
                             'GF',  'SA', 'GA')].astype(int)

    away_shots_for_5.columns = ['season', 'game_id', 'date', 'player_id',
                                'player_name', 'CF', 'FF', 'SF', 'GF', 'xgf']
    away_shots_against_5.columns = ['season', 'game_id', 'date', 'player_id',
                                    'player_name', 'CA', 'FA', 'SA', 'GA', 'xga']

    away_onice_5 = away_shots_for_5.merge(away_shots_against_5,
                                          on=['season', 'game_id', 'date',
                                              'player_id', 'player_name'],
                                          how='outer')

    away_onice_5 = away_onice_5.fillna(0)

    away_onice_5.loc[:, ('season', 'game_id', 'player_id', 'SF',
                         'GF', 'SA', 'GA')] = away_onice_5\
                    .loc[:, ('season', 'game_id', 'player_id', 'SF',
                             'GF', 'SA', 'GA')].astype(int)

    away_shots_for_6.columns = ['season', 'game_id', 'date', 'player_id',
                                'player_name', 'CF', 'FF', 'SF', 'GF', 'xgf']
    away_shots_against_6.columns = ['season', 'game_id', 'date', 'player_id',
                                    'player_name', 'CA', 'FA', 'SA', 'GA', 'xga']

    away_onice_6 = away_shots_for_6.merge(away_shots_against_6,
                                          on=['season', 'game_id', 'date',
                                              'player_id', 'player_name'],
                                          how='outer')

    away_onice_6 = away_onice_6.fillna(0)

    away_onice_6.loc[:, ('season', 'game_id', 'player_id', 'SF',
                         'GF', 'SA', 'GA')] = away_onice_6\
                    .loc[:, ('season', 'game_id', 'player_id', 'SF',
                             'GF', 'SA', 'GA')].astype(int)

    away_onice_list = [away_onice_1, away_onice_2, away_onice_3, away_onice_4,
                       away_onice_5, away_onice_6]

    away_metrics = pd.concat(away_onice_list)

    away_metrics = away_metrics.groupby(['season', 'game_id', 'date',
                                         'player_id', 'player_name'])\
                                ['CF', 'CA', 'FF', 'FA', 'SF', 'SA', 'GF', 'GA', 'xgf', 'xga'].sum().reset_index()

    away_metrics['team'] = pbp_df.away_team
    home_metrics['team'] = pbp_df.home_team

    shot_metrics = [away_metrics, home_metrics]

    shot_metrics_df = pd.concat(shot_metrics, sort=False)

    shot_metrics_df = shot_metrics_df[['season', 'game_id', 'date', 'team',
                                       'player_id',
                                       'player_name', 'CF', 'CA', 'FF', 'FA',
                                       'SF', 'SA', 'GF', 'GA', 'xgf', 'xga']]
    return shot_metrics_df
def calc_toi(pbp_df):
    '''
    This will calculate a players TOI for all situations in the game

    Inputs:
    pbp_df - play by play dataframe

    Outputs:
    toi_df - dataframe with each players TOI calculated
    '''

#compiling toi for each player column in the pbp_df to make sure that every
#player is accounted for as the player columns are not any set position
    home_1 = pbp_df.groupby(['season', 'game_id', 'date', 'homeplayer1_id',
                             'homeplayer1'])['event_length'].sum().reset_index()
    home_2 = pbp_df.groupby(['season', 'game_id', 'date', 'homeplayer2_id',
                             'homeplayer2'])['event_length'].sum().reset_index()
    home_3 = pbp_df.groupby(['season', 'game_id', 'date', 'homeplayer3_id',
                             'homeplayer3'])['event_length'].sum().reset_index()
    home_4 = pbp_df.groupby(['season', 'game_id', 'date', 'homeplayer4_id',
                             'homeplayer4'])['event_length'].sum().reset_index()
    home_5 = pbp_df.groupby(['season', 'game_id', 'date', 'homeplayer5_id',
                             'homeplayer5'])['event_length'].sum().reset_index()
    home_6 = pbp_df.groupby(['season', 'game_id', 'date', 'homeplayer6_id',
                             'homeplayer6'])['event_length'].sum().reset_index()

    away_1 = pbp_df.groupby(['season', 'game_id', 'date', 'awayplayer1_id',
                             'awayplayer1'])['event_length'].sum().reset_index()
    away_2 = pbp_df.groupby(['season', 'game_id', 'date', 'awayplayer2_id',
                             'awayplayer2'])['event_length'].sum().reset_index()
    away_3 = pbp_df.groupby(['season', 'game_id', 'date', 'awayplayer3_id',
                             'awayplayer3'])['event_length'].sum().reset_index()
    away_4 = pbp_df.groupby(['season', 'game_id', 'date', 'awayplayer4_id',
                             'awayplayer4'])['event_length'].sum().reset_index()
    away_5 = pbp_df.groupby(['season', 'game_id', 'date', 'awayplayer5_id',
                             'awayplayer5'])['event_length'].sum().reset_index()
    away_6 = pbp_df.groupby(['season', 'game_id', 'date', 'awayplayer6_id',
                             'awayplayer6'])['event_length'].sum().reset_index()

#making all the dataframes the same to that I can concat them
    home_1.columns = ['season', 'game_id', 'date', 'player_id', 'player_name', 'toi']
    home_2.columns = ['season', 'game_id', 'date', 'player_id', 'player_name', 'toi']
    home_3.columns = ['season', 'game_id', 'date', 'player_id', 'player_name', 'toi']
    home_4.columns = ['season', 'game_id', 'date', 'player_id', 'player_name', 'toi']
    home_5.columns = ['season', 'game_id', 'date', 'player_id', 'player_name', 'toi']
    home_6.columns = ['season', 'game_id', 'date', 'player_id', 'player_name', 'toi']

    away_1.columns = ['season', 'game_id', 'date', 'player_id', 'player_name', 'toi']
    away_2.columns = ['season', 'game_id', 'date', 'player_id', 'player_name', 'toi']
    away_3.columns = ['season', 'game_id', 'date', 'player_id', 'player_name', 'toi']
    away_4.columns = ['season', 'game_id', 'date', 'player_id', 'player_name', 'toi']
    away_5.columns = ['season', 'game_id', 'date', 'player_id', 'player_name', 'toi']
    away_6.columns = ['season', 'game_id', 'date', 'player_id', 'player_name', 'toi']

#joining all the seperate toi dataframes into one big dataframe that I will
#group by and sum their TOI
    home_toi = pd.concat([home_1, home_2, home_3, home_4, home_5, home_6])

    away_toi = pd.concat([away_1, away_2, away_3, away_4, away_5, away_6])
    away_toi = away_toi.groupby(['season', 'game_id', 'date', 'player_id', 'player_name'])['toi'].sum().reset_index()
    home_toi = home_toi.groupby(['season', 'game_id', 'date', 'player_id', 'player_name'])['toi'].sum().reset_index()

    toi_df = pd.concat([home_toi, away_toi])

    toi_df = toi_df.groupby(['player_id', 'player_name'])['toi'].sum().reset_index()
    #toi_df.loc[:, ('toi')] = round(toi_df.toi/60, 2)

    return toi_df

def calc_on_ice_shots(pbp_df):
    '''
    function to calculate on ice shot metrics for all situations

    Inputs:
    pbp_df - play by play dataframe

    Outputs:
    on_ice_shots_df - dataframe of on ice shot events while player was on ice
    '''

    home_shots_for_1 = pbp_df[pbp_df.is_home == 1]\
                          .groupby(['season', 'game_id', 'date',
                                    'homeplayer1_id', 'homeplayer1'])\
                                  ['is_corsi', 'is_fenwick',
                                   'is_shot', 'is_goal', 'xg'].sum().reset_index()

    home_shots_against_1 = pbp_df[pbp_df.is_home == 0]\
                          .groupby(['season', 'game_id', 'date',
                                    'homeplayer1_id', 'homeplayer1'])\
                                  ['is_corsi', 'is_fenwick',
                                   'is_shot', 'is_goal', 'xg'].sum().reset_index()

    home_shots_for_2 = pbp_df[pbp_df.is_home == 1]\
                          .groupby(['season', 'game_id', 'date',
                                    'homeplayer2_id', 'homeplayer2'])\
                                  ['is_corsi', 'is_fenwick',
                                   'is_shot', 'is_goal', 'xg'].sum().reset_index()

    home_shots_against_2 = pbp_df[pbp_df.is_home == 0]\
                          .groupby(['season', 'game_id', 'date',
                                    'homeplayer2_id', 'homeplayer2'])\
                                  ['is_corsi', 'is_fenwick',
                                   'is_shot', 'is_goal', 'xg'].sum().reset_index()

    home_shots_for_3 = pbp_df[pbp_df.is_home == 1]\
                          .groupby(['season', 'game_id', 'date',
                                    'homeplayer3_id', 'homeplayer3'])\
                                  ['is_corsi', 'is_fenwick',
                                   'is_shot', 'is_goal','xg'].sum().reset_index()

    home_shots_against_3 = pbp_df[pbp_df.is_home == 0]\
                          .groupby(['season', 'game_id', 'date',
                                    'homeplayer3_id', 'homeplayer3'])\
                                  ['is_corsi', 'is_fenwick',
                                   'is_shot', 'is_goal','xg'].sum().reset_index()

    home_shots_for_4 = pbp_df[pbp_df.is_home == 1]\
                          .groupby(['season', 'game_id', 'date',
                                    'homeplayer4_id', 'homeplayer4'])\
                                  ['is_corsi', 'is_fenwick',
                                   'is_shot', 'is_goal', 'xg'].sum().reset_index()

    home_shots_against_4 = pbp_df[pbp_df.is_home == 0]\
                          .groupby(['season', 'game_id', 'date',
                                    'homeplayer4_id', 'homeplayer4'])\
                                  ['is_corsi', 'is_fenwick',
                                   'is_shot', 'is_goal', 'xg'].sum().reset_index()

    home_shots_for_5 = pbp_df[pbp_df.is_home == 1]\
                          .groupby(['season', 'game_id', 'date',
                                    'homeplayer5_id', 'homeplayer5'])\
                                  ['is_corsi', 'is_fenwick',
                                   'is_shot', 'is_goal', 'xg'].sum().reset_index()

    home_shots_against_5 = pbp_df[pbp_df.is_home == 0]\
                          .groupby(['season', 'game_id', 'date',
                                    'homeplayer5_id', 'homeplayer5'])\
                                  ['is_corsi', 'is_fenwick',
                                   'is_shot', 'is_goal', 'xg'].sum().reset_index()

    home_shots_for_6 = pbp_df[pbp_df.is_home == 1]\
                          .groupby(['season', 'game_id', 'date',
                                    'homeplayer6_id', 'homeplayer6'])\
                                  ['is_corsi', 'is_fenwick',
                                   'is_shot', 'is_goal', 'xg'].sum().reset_index()

    home_shots_against_6 = pbp_df[pbp_df.is_home == 0]\
                          .groupby(['season', 'game_id', 'date',
                                    'homeplayer6_id', 'homeplayer6'])\
                                  ['is_corsi', 'is_fenwick',
                                   'is_shot', 'is_goal', 'xg'].sum().reset_index()

#refactor this into a for loop and store all the dataframes into a list probably
#can do the same with the code above
    home_shots_for_1.columns = ['season', 'game_id', 'date', 'player_id',
                                'player_name', 'CF', 'FF', 'SF', 'GF', 'xgf']
    home_shots_against_1.columns = ['season', 'game_id', 'date', 'player_id',
                                    'player_name', 'CA', 'FA', 'SA', 'GA', 'xga']

    home_onice_1 = home_shots_for_1.merge(home_shots_against_1,
                                          on=['season', 'game_id', 'date',
                                              'player_id', 'player_name'],
                                          how='outer')

    home_onice_1 = home_onice_1.fillna(0)

    home_onice_1.loc[:, ('season', 'game_id', 'player_id', 'CF', 'FF', 'SF',
                         'GF', 'CA', 'FA', 'SA', 'GA')] = home_onice_1\
                    .loc[:, ('season', 'game_id', 'player_id', 'CF', 'FF', 'SF',
                             'GF', 'CA', 'FA', 'SA', 'GA')].astype(int)

    home_shots_for_2.columns = ['season', 'game_id', 'date', 'player_id',
                                'player_name', 'CF', 'FF', 'SF', 'GF', 'xgf']
    home_shots_against_2.columns = ['season', 'game_id', 'date', 'player_id',
                                    'player_name', 'CA', 'FA', 'SA', 'GA', 'xga']

    home_onice_2 = home_shots_for_2.merge(home_shots_against_2,
                                          on=['season', 'game_id', 'date',
                                              'player_id', 'player_name'],
                                          how='outer')

    home_onice_2 = home_onice_2.fillna(0)

    home_onice_2.loc[:, ('season', 'game_id', 'player_id', 'CF', 'FF', 'SF',
                         'GF', 'CA', 'FA', 'SA', 'GA')] = home_onice_2\
                    .loc[:, ('season', 'game_id', 'player_id', 'CF', 'FF', 'SF',
                             'GF', 'CA', 'FA', 'SA', 'GA')].astype(int)

    home_shots_for_3.columns = ['season', 'game_id', 'date', 'player_id',
                                'player_name', 'CF', 'FF', 'SF', 'GF', 'xgf']
    home_shots_against_3.columns = ['season', 'game_id', 'date', 'player_id',
                                    'player_name', 'CA', 'FA', 'SA', 'GA', 'xga']


    home_onice_3 = home_shots_for_3.merge(home_shots_against_3,
                                          on=['season', 'game_id', 'date',
                                              'player_id', 'player_name'],
                                          how='outer')

    home_onice_3 = home_onice_3.fillna(0)

    home_onice_3.loc[:, ('season', 'game_id', 'player_id', 'CF', 'FF', 'SF',
                         'GF', 'CA', 'FA', 'SA', 'GA')] = home_onice_3\
                    .loc[:, ('season', 'game_id', 'player_id', 'CF', 'FF', 'SF',
                             'GF', 'CA', 'FA', 'SA', 'GA')].astype(int)
    home_shots_for_4.columns = ['season', 'game_id', 'date', 'player_id',
                                'player_name', 'CF', 'FF', 'SF', 'GF', 'xgf']
    home_shots_against_4.columns = ['season', 'game_id', 'date', 'player_id',
                                    'player_name', 'CA', 'FA', 'SA', 'GA', 'xga']

    home_onice_4 = home_shots_for_4.merge(home_shots_against_4,
                                          on=['season', 'game_id', 'date',
                                              'player_id', 'player_name'],
                                          how='outer')

    home_onice_4 = home_onice_4.fillna(0)

    home_onice_4.loc[:, ('season', 'game_id', 'player_id', 'CF', 'FF', 'SF',
                         'GF', 'CA', 'FA', 'SA', 'GA')] = home_onice_4\
                    .loc[:, ('season', 'game_id', 'player_id', 'CF', 'FF', 'SF',
                             'GF', 'CA', 'FA', 'SA', 'GA')].astype(int)

    home_shots_for_5.columns = ['season', 'game_id', 'date', 'player_id',
                                'player_name', 'CF', 'FF', 'SF', 'GF', 'xgf']
    home_shots_against_5.columns = ['season', 'game_id', 'date', 'player_id',
                                    'player_name', 'CA', 'FA', 'SA', 'GA', 'xga']

    home_onice_5 = home_shots_for_5.merge(home_shots_against_5,
                                          on=['season', 'game_id', 'date',
                                              'player_id', 'player_name'],
                                          how='outer')

    home_onice_5 = home_onice_5.fillna(0)

    home_onice_5.loc[:, ('season', 'game_id', 'player_id', 'CF', 'FF', 'SF',
                         'GF', 'CA', 'FA', 'SA', 'GA')] = home_onice_5\
                    .loc[:, ('season', 'game_id', 'player_id', 'CF', 'FF', 'SF',
                             'GF', 'CA', 'FA', 'SA', 'GA')].astype(int)

    home_shots_for_6.columns = ['season', 'game_id', 'date', 'player_id',
                                'player_name', 'CF', 'FF', 'SF', 'GF', 'xgf']
    home_shots_against_6.columns = ['season', 'game_id', 'date', 'player_id',
                                    'player_name', 'CA', 'FA', 'SA', 'GA', 'xga']

    home_onice_6 = home_shots_for_6.merge(home_shots_against_6,
                                          on=['season', 'game_id', 'date',
                                              'player_id', 'player_name'],
                                          how='outer')

    home_onice_6 = home_onice_6.fillna(0)

    home_onice_6.loc[:, ('season', 'game_id', 'player_id', 'CF', 'FF', 'SF',
                         'GF', 'CA', 'FA', 'SA', 'GA')] = home_onice_6\
                    .loc[:, ('season', 'game_id', 'player_id', 'CF', 'FF', 'SF',
                             'GF', 'CA', 'FA', 'SA', 'GA')].astype(int)

    home_onice_list = [home_onice_1, home_onice_2, home_onice_3, home_onice_4,
                       home_onice_5, home_onice_6]

    home_metrics = pd.concat(home_onice_list)

    home_metrics = home_metrics.groupby(['season', 'game_id', 'date',
                                         'player_id', 'player_name'])\
                                ['CF', 'CA', 'FF', 'FA', 'SF', 'SA', 'GF', 'GA', 'xgf', 'xga'].sum().reset_index()

    away_shots_for_1 = pbp_df[pbp_df.is_home == 0]\
                          .groupby(['season', 'game_id', 'date',
                                    'awayplayer1_id', 'awayplayer1'])\
                                  ['is_corsi', 'is_fenwick',
                                   'is_shot', 'is_goal', 'xg'].sum().reset_index()

    away_shots_against_1 = pbp_df[pbp_df.is_home == 1]\
                          .groupby(['season', 'game_id', 'date',
                                    'awayplayer1_id', 'awayplayer1'])\
                                  ['is_corsi', 'is_fenwick',
                                   'is_shot', 'is_goal', 'xg'].sum().reset_index()

    away_shots_for_2 = pbp_df[pbp_df.is_home == 0]\
                          .groupby(['season', 'game_id', 'date',
                                    'awayplayer2_id', 'awayplayer2'])\
                                  ['is_corsi', 'is_fenwick',
                                   'is_shot', 'is_goal', 'xg'].sum().reset_index()

    away_shots_against_2 = pbp_df[pbp_df.is_home == 1]\
                          .groupby(['season', 'game_id', 'date',
                                    'awayplayer2_id', 'awayplayer2'])\
                                  ['is_corsi', 'is_fenwick',
                                   'is_shot', 'is_goal', 'xg'].sum().reset_index()

    away_shots_for_3 = pbp_df[pbp_df.is_home == 0]\
                          .groupby(['season', 'game_id', 'date',
                                    'awayplayer3_id', 'awayplayer3'])\
                                  ['is_corsi', 'is_fenwick',
                                   'is_shot', 'is_goal', 'xg'].sum().reset_index()

    away_shots_against_3 = pbp_df[pbp_df.is_home == 1]\
                          .groupby(['season', 'game_id', 'date',
                                    'awayplayer3_id', 'awayplayer3'])\
                                  ['is_corsi', 'is_fenwick',
                                   'is_shot', 'is_goal', 'xg'].sum().reset_index()

    away_shots_for_4 = pbp_df[pbp_df.is_home == 0]\
                          .groupby(['season', 'game_id', 'date',
                                    'awayplayer4_id', 'awayplayer4'])\
                                  ['is_corsi', 'is_fenwick',
                                   'is_shot', 'is_goal', 'xg'].sum().reset_index()

    away_shots_against_4 = pbp_df[pbp_df.is_home == 1]\
                          .groupby(['season', 'game_id', 'date',
                                    'awayplayer4_id', 'awayplayer4'])\
                                  ['is_corsi', 'is_fenwick',
                                   'is_shot', 'is_goal', 'xg'].sum().reset_index()

    away_shots_for_5 = pbp_df[pbp_df.is_home == 0]\
                          .groupby(['season', 'game_id', 'date',
                                    'awayplayer5_id', 'awayplayer5'])\
                                  ['is_corsi', 'is_fenwick',
                                   'is_shot', 'is_goal', 'xg'].sum().reset_index()

    away_shots_against_5 = pbp_df[pbp_df.is_home == 1]\
                          .groupby(['season', 'game_id', 'date',
                                    'awayplayer5_id', 'awayplayer5'])\
                                  ['is_corsi', 'is_fenwick',
                                   'is_shot', 'is_goal', 'xg'].sum().reset_index()

    away_shots_for_6 = pbp_df[pbp_df.is_home == 0]\
                          .groupby(['season', 'game_id', 'date',
                                    'awayplayer6_id', 'awayplayer6'])\
                                  ['is_corsi', 'is_fenwick',
                                   'is_shot', 'is_goal', 'xg'].sum().reset_index()

    away_shots_against_6 = pbp_df[pbp_df.is_home == 1]\
                          .groupby(['season', 'game_id', 'date',
                                    'awayplayer6_id', 'awayplayer6'])\
                                  ['is_corsi', 'is_fenwick',
                                   'is_shot', 'is_goal', 'xg'].sum().reset_index()

    away_shots_for_1.columns = ['season', 'game_id', 'date', 'player_id',
                                'player_name', 'CF', 'FF', 'SF', 'GF', 'xgf']
    away_shots_against_1.columns = ['season', 'game_id', 'date', 'player_id',
                                    'player_name', 'CA', 'FA', 'SA', 'GA', 'xga']

    away_onice_1 = away_shots_for_1.merge(away_shots_against_1,
                                          on=['season', 'game_id', 'date',
                                              'player_id', 'player_name'],
                                          how='outer')

    away_onice_1 = away_onice_1.fillna(0)

    away_onice_1.loc[:, ('season', 'game_id', 'player_id', 'CF', 'FF', 'SF',
                         'GF', 'CA', 'FA', 'SA', 'GA')] = away_onice_1\
                    .loc[:, ('season', 'game_id', 'player_id', 'CF', 'FF', 'SF',
                             'GF', 'CA', 'FA', 'SA', 'GA')].astype(int)

    away_shots_for_2.columns = ['season', 'game_id', 'date', 'player_id',
                                'player_name', 'CF', 'FF', 'SF', 'GF', 'xgf']
    away_shots_against_2.columns = ['season', 'game_id', 'date', 'player_id',
                                    'player_name', 'CA', 'FA', 'SA', 'GA', 'xga']

    away_onice_2 = away_shots_for_2.merge(away_shots_against_2,
                                          on=['season', 'game_id', 'date',
                                              'player_id', 'player_name'],
                                          how='outer')

    away_onice_2 = away_onice_2.fillna(0)

    away_onice_2.loc[:, ('season', 'game_id', 'player_id', 'CF', 'FF', 'SF',
                         'GF', 'CA', 'FA', 'SA', 'GA')] = away_onice_2\
                    .loc[:, ('season', 'game_id', 'player_id', 'CF', 'FF', 'SF',
                             'GF', 'CA', 'FA', 'SA', 'GA')].astype(int)

    away_shots_for_3.columns = ['season', 'game_id', 'date', 'player_id',
                                'player_name', 'CF', 'FF', 'SF', 'GF', 'xgf']
    away_shots_against_3.columns = ['season', 'game_id', 'date', 'player_id',
                                    'player_name', 'CA', 'FA', 'SA', 'GA', 'xga']


    away_onice_3 = away_shots_for_3.merge(away_shots_against_3,
                                          on=['season', 'game_id', 'date',
                                              'player_id', 'player_name'],
                                          how='outer')

    away_onice_3 = away_onice_3.fillna(0)

    away_onice_3.loc[:, ('season', 'game_id', 'player_id', 'CF', 'FF', 'SF',
                         'GF', 'CA', 'FA', 'SA', 'GA')] = away_onice_3\
                    .loc[:, ('season', 'game_id', 'player_id', 'CF', 'FF', 'SF',
                             'GF', 'CA', 'FA', 'SA', 'GA')].astype(int)

    away_shots_for_4.columns = ['season', 'game_id', 'date', 'player_id',
                                'player_name', 'CF', 'FF', 'SF', 'GF', 'xgf']
    away_shots_against_4.columns = ['season', 'game_id', 'date', 'player_id',
                                    'player_name', 'CA', 'FA', 'SA', 'GA', 'xga']

    away_onice_4 = away_shots_for_4.merge(away_shots_against_4,
                                          on=['season', 'game_id', 'date',
                                              'player_id', 'player_name'],
                                          how='outer')

    away_onice_4 = away_onice_4.fillna(0)

    away_onice_4.loc[:, ('season', 'game_id', 'player_id', 'CF', 'FF', 'SF',
                         'GF', 'CA', 'FA', 'SA', 'GA')] = away_onice_4\
                    .loc[:, ('season', 'game_id', 'player_id', 'CF', 'FF', 'SF',
                             'GF', 'CA', 'FA', 'SA', 'GA')].astype(int)

    away_shots_for_5.columns = ['season', 'game_id', 'date', 'player_id',
                                'player_name', 'CF', 'FF', 'SF', 'GF', 'xgf']
    away_shots_against_5.columns = ['season', 'game_id', 'date', 'player_id',
                                    'player_name', 'CA', 'FA', 'SA', 'GA', 'xga']

    away_onice_5 = away_shots_for_5.merge(away_shots_against_5,
                                          on=['season', 'game_id', 'date',
                                              'player_id', 'player_name'],
                                          how='outer')

    away_onice_5 = away_onice_5.fillna(0)

    away_onice_5.loc[:, ('season', 'game_id', 'player_id', 'CF', 'FF', 'SF',
                         'GF', 'CA', 'FA', 'SA', 'GA')] = away_onice_5\
                    .loc[:, ('season', 'game_id', 'player_id', 'CF', 'FF', 'SF',
                             'GF', 'CA', 'FA', 'SA', 'GA')].astype(int)

    away_shots_for_6.columns = ['season', 'game_id', 'date', 'player_id',
                                'player_name', 'CF', 'FF', 'SF', 'GF', 'xgf']
    away_shots_against_6.columns = ['season', 'game_id', 'date', 'player_id',
                                    'player_name', 'CA', 'FA', 'SA', 'GA', 'xga']

    away_onice_6 = away_shots_for_6.merge(away_shots_against_6,
                                          on=['season', 'game_id', 'date',
                                              'player_id', 'player_name'],
                                          how='outer')

    away_onice_6 = away_onice_6.fillna(0)

    away_onice_6.loc[:, ('season', 'game_id', 'player_id', 'CF', 'FF', 'SF',
                         'GF', 'CA', 'FA', 'SA', 'GA')] = away_onice_6\
                    .loc[:, ('season', 'game_id', 'player_id', 'CF', 'FF', 'SF',
                             'GF', 'CA', 'FA', 'SA', 'GA')].astype(int)

    away_onice_list = [away_onice_1, away_onice_2, away_onice_3, away_onice_4,
                       away_onice_5, away_onice_6]

    away_metrics = pd.concat(away_onice_list)

    away_metrics = away_metrics.groupby(['season', 'game_id', 'date',
                                         'player_id', 'player_name'])\
                                ['CF', 'CA', 'FF', 'FA', 'SF', 'SA', 'GF', 'GA', 'xgf', 'xga'].sum().reset_index()

    away_metrics['team'] = pbp_df.away_team
    home_metrics['team'] = pbp_df.home_team

    shot_metrics = [away_metrics, home_metrics]

    shot_metrics_df = pd.concat(shot_metrics, sort=False)

    shot_metrics_df = shot_metrics_df[['season', 'game_id', 'date', 'team',
                                       'player_id',
                                       'player_name', 'CF', 'CA', 'FF', 'FA',
                                       'SF', 'SA', 'GF', 'GA', 'xgf', 'xga']]
    return shot_metrics_df


def calc_on_ice_pens(pbp_df):
    '''
    function to calculate the on ice penalties taken and drawn for each team
    in a play by play dataframe

    Inputs:
    pbp_df - play by play dataframe

    Outputs:
    penalties_df - dataframe with players who were on the ice for penalties
                   taken and drawn
    '''

    home_pens_drawn_1 = pbp_df[pbp_df.is_home == 0]\
                          .groupby(['season', 'game_id', 'date',
                                    'homeplayer1_id', 'homeplayer1'])\
                                  ['is_penalty'].sum().reset_index()

    home_pens_drawn_2 = pbp_df[pbp_df.is_home == 0]\
                          .groupby(['season', 'game_id', 'date',
                                    'homeplayer2_id', 'homeplayer2'])\
                                  ['is_penalty'].sum().reset_index()

    home_pens_drawn_3 = pbp_df[pbp_df.is_home == 0]\
                          .groupby(['season', 'game_id', 'date',
                                    'homeplayer3_id', 'homeplayer3'])\
                                  ['is_penalty'].sum().reset_index()

    home_pens_drawn_4 = pbp_df[pbp_df.is_home == 0]\
                          .groupby(['season', 'game_id', 'date',
                                    'homeplayer4_id', 'homeplayer4'])\
                                  ['is_penalty'].sum().reset_index()

    home_pens_drawn_5 = pbp_df[pbp_df.is_home == 0]\
                          .groupby(['season', 'game_id', 'date',
                                    'homeplayer5_id', 'homeplayer5'])\
                                  ['is_penalty'].sum().reset_index()

    home_pens_drawn_6 = pbp_df[pbp_df.is_home == 0]\
                          .groupby(['season', 'game_id', 'date',
                                    'homeplayer6_id', 'homeplayer6'])\
                                  ['is_penalty'].sum().reset_index()

    home_pens_taken_1 = pbp_df[pbp_df.is_home == 1]\
                          .groupby(['season', 'game_id', 'date',
                                    'homeplayer1_id', 'homeplayer1'])\
                                  ['is_penalty'].sum().reset_index()

    home_pens_taken_2 = pbp_df[pbp_df.is_home == 1]\
                          .groupby(['season', 'game_id', 'date',
                                    'homeplayer2_id', 'homeplayer2'])\
                                  ['is_penalty'].sum().reset_index()

    home_pens_taken_3 = pbp_df[pbp_df.is_home == 1]\
                          .groupby(['season', 'game_id', 'date',
                                    'homeplayer3_id', 'homeplayer3'])\
                                  ['is_penalty'].sum().reset_index()

    home_pens_taken_4 = pbp_df[pbp_df.is_home == 1]\
                          .groupby(['season', 'game_id', 'date',
                                    'homeplayer4_id', 'homeplayer4'])\
                                  ['is_penalty'].sum().reset_index()

    home_pens_taken_5 = pbp_df[pbp_df.is_home == 1]\
                          .groupby(['season', 'game_id', 'date',
                                    'homeplayer5_id', 'homeplayer5'])\
                                  ['is_penalty'].sum().reset_index()

    home_pens_taken_6 = pbp_df[pbp_df.is_home == 1]\
                          .groupby(['season', 'game_id', 'date',
                                    'homeplayer6_id', 'homeplayer6'])\
                                  ['is_penalty'].sum().reset_index()


    home_pens_drawn_1.columns = ['season', 'game_id', 'date', 'player_id',
                                'player_name', 'PEND']
    home_pens_taken_1.columns = ['season', 'game_id', 'date', 'player_id',
                                    'player_name', 'PENT']

    home_oi_pens_1 = home_pens_drawn_1.merge(home_pens_taken_1,
                                          on=['season', 'game_id', 'date',
                                              'player_id', 'player_name'],
                                          how='outer')

    home_oi_pens_1 = home_oi_pens_1.fillna(0)

    home_pens_drawn_2.columns = ['season', 'game_id', 'date', 'player_id',
                                'player_name', 'PEND']
    home_pens_taken_2.columns = ['season', 'game_id', 'date', 'player_id',
                                    'player_name', 'PENT']

    home_oi_pens_2 = home_pens_drawn_2.merge(home_pens_taken_2,
                                          on=['season', 'game_id', 'date',
                                              'player_id', 'player_name'],
                                          how='outer')

    home_oi_pens_2 = home_oi_pens_2.fillna(0)

    home_pens_drawn_3.columns = ['season', 'game_id', 'date', 'player_id',
                                'player_name', 'PEND']
    home_pens_taken_3.columns = ['season', 'game_id', 'date', 'player_id',
                                    'player_name', 'PENT']

    home_oi_pens_3 = home_pens_drawn_3.merge(home_pens_taken_3,
                                          on=['season', 'game_id', 'date',
                                              'player_id', 'player_name'],
                                          how='outer')

    home_oi_pens_3 = home_oi_pens_3.fillna(0)

    home_pens_drawn_4.columns = ['season', 'game_id', 'date', 'player_id',
                                'player_name', 'PEND']
    home_pens_taken_4.columns = ['season', 'game_id', 'date', 'player_id',
                                    'player_name', 'PENT']

    home_oi_pens_4 = home_pens_drawn_4.merge(home_pens_taken_4,
                                          on=['season', 'game_id', 'date',
                                              'player_id', 'player_name'],
                                          how='outer')

    home_oi_pens_4 = home_oi_pens_4.fillna(0)

    home_pens_drawn_5.columns = ['season', 'game_id', 'date', 'player_id',
                                'player_name', 'PEND']
    home_pens_taken_5.columns = ['season', 'game_id', 'date', 'player_id',
                                    'player_name', 'PENT']

    home_oi_pens_5 = home_pens_drawn_5.merge(home_pens_taken_5,
                                          on=['season', 'game_id', 'date',
                                              'player_id', 'player_name'],
                                          how='outer')

    home_oi_pens_5 = home_oi_pens_5.fillna(0)

    home_pens_drawn_6.columns = ['season', 'game_id', 'date', 'player_id',
                                'player_name', 'PEND']
    home_pens_taken_6.columns = ['season', 'game_id', 'date', 'player_id',
                                    'player_name', 'PENT']

    home_oi_pens_6 = home_pens_drawn_6.merge(home_pens_taken_6,
                                          on=['season', 'game_id', 'date',
                                              'player_id', 'player_name'],
                                          how='outer')

    home_oi_pens_6 = home_oi_pens_6.fillna(0)

    home_oi_pens_list = [home_oi_pens_1, home_oi_pens_2, home_oi_pens_3, home_oi_pens_4,
                       home_oi_pens_5, home_oi_pens_6]

    home_pens = pd.concat(home_oi_pens_list)

    home_pens = home_pens.groupby(['season', 'game_id', 'date',
                                         'player_id', 'player_name'])\
                                ['PEND', 'PENT'].sum().reset_index()

    away_pens_drawn_1 = pbp_df[pbp_df.is_home == 1]\
                          .groupby(['season', 'game_id', 'date',
                                    'awayplayer1_id', 'awayplayer1'])\
                                  ['is_penalty'].sum().reset_index()

    away_pens_drawn_2 = pbp_df[pbp_df.is_home == 1]\
                          .groupby(['season', 'game_id', 'date',
                                    'awayplayer2_id', 'awayplayer2'])\
                                  ['is_penalty'].sum().reset_index()

    away_pens_drawn_3 = pbp_df[pbp_df.is_home == 1]\
                          .groupby(['season', 'game_id', 'date',
                                    'awayplayer3_id', 'awayplayer3'])\
                                  ['is_penalty'].sum().reset_index()

    away_pens_drawn_4 = pbp_df[pbp_df.is_home == 1]\
                          .groupby(['season', 'game_id', 'date',
                                    'awayplayer4_id', 'awayplayer4'])\
                                  ['is_penalty'].sum().reset_index()

    away_pens_drawn_5 = pbp_df[pbp_df.is_home == 1]\
                          .groupby(['season', 'game_id', 'date',
                                    'awayplayer5_id', 'awayplayer5'])\
                                  ['is_penalty'].sum().reset_index()

    away_pens_drawn_6 = pbp_df[pbp_df.is_home == 1]\
                          .groupby(['season', 'game_id', 'date',
                                    'awayplayer6_id', 'awayplayer6'])\
                                  ['is_penalty'].sum().reset_index()

    away_pens_taken_1 = pbp_df[pbp_df.is_home == 0]\
                          .groupby(['season', 'game_id', 'date',
                                    'awayplayer1_id', 'awayplayer1'])\
                                  ['is_penalty'].sum().reset_index()

    away_pens_taken_2 = pbp_df[pbp_df.is_home == 0]\
                          .groupby(['season', 'game_id', 'date',
                                    'awayplayer2_id', 'awayplayer2'])\
                                  ['is_penalty'].sum().reset_index()

    away_pens_taken_3 = pbp_df[pbp_df.is_home == 0]\
                          .groupby(['season', 'game_id', 'date',
                                    'awayplayer3_id', 'awayplayer3'])\
                                  ['is_penalty'].sum().reset_index()

    away_pens_taken_4 = pbp_df[pbp_df.is_home == 0]\
                          .groupby(['season', 'game_id', 'date',
                                    'awayplayer4_id', 'awayplayer4'])\
                                  ['is_penalty'].sum().reset_index()

    away_pens_taken_5 = pbp_df[pbp_df.is_home == 0]\
                          .groupby(['season', 'game_id', 'date',
                                    'awayplayer5_id', 'awayplayer5'])\
                                  ['is_penalty'].sum().reset_index()

    away_pens_taken_6 = pbp_df[pbp_df.is_home == 0]\
                          .groupby(['season', 'game_id', 'date',
                                    'awayplayer6_id', 'awayplayer6'])\
                                  ['is_penalty'].sum().reset_index()

    away_pens_drawn_1.columns = ['season', 'game_id', 'date', 'player_id',
                                'player_name', 'PEND']
    away_pens_taken_1.columns = ['season', 'game_id', 'date', 'player_id',
                                    'player_name', 'PENT']

    away_oi_pens_1 = away_pens_taken_1.merge(away_pens_drawn_1,
                                          on=['season', 'game_id', 'date',
                                              'player_id', 'player_name'],
                                          how='outer')

    away_oi_pens_1 = away_oi_pens_1.fillna(0)

    away_pens_drawn_2.columns = ['season', 'game_id', 'date', 'player_id',
                                'player_name', 'PEND']
    away_pens_taken_2.columns = ['season', 'game_id', 'date', 'player_id',
                                    'player_name', 'PENT']

    away_oi_pens_2 = away_pens_taken_2.merge(away_pens_drawn_2,
                                          on=['season', 'game_id', 'date',
                                              'player_id', 'player_name'],
                                          how='outer')

    away_oi_pens_2 = away_oi_pens_2.fillna(0)

    away_pens_drawn_3.columns = ['season', 'game_id', 'date', 'player_id',
                                'player_name', 'PEND']
    away_pens_taken_3.columns = ['season', 'game_id', 'date', 'player_id',
                                    'player_name', 'PENT']

    away_oi_pens_3 = away_pens_taken_3.merge(away_pens_drawn_3,
                                          on=['season', 'game_id', 'date',
                                              'player_id', 'player_name'],
                                          how='outer')

    away_oi_pens_3 = away_oi_pens_3.fillna(0)

    away_pens_drawn_4.columns = ['season', 'game_id', 'date', 'player_id',
                                'player_name', 'PEND']
    away_pens_taken_4.columns = ['season', 'game_id', 'date', 'player_id',
                                    'player_name', 'PENT']

    away_oi_pens_4 = away_pens_taken_4.merge(away_pens_drawn_4,
                                          on=['season', 'game_id', 'date',
                                              'player_id', 'player_name'],
                                          how='outer')

    away_oi_pens_4 = away_oi_pens_4.fillna(0)

    away_pens_drawn_5.columns = ['season', 'game_id', 'date', 'player_id',
                                'player_name', 'PEND']
    away_pens_taken_5.columns = ['season', 'game_id', 'date', 'player_id',
                                    'player_name', 'PENT']

    away_oi_pens_5 = away_pens_taken_5.merge(away_pens_drawn_5,
                                          on=['season', 'game_id', 'date',
                                              'player_id', 'player_name'],
                                          how='outer')

    away_oi_pens_5 = away_oi_pens_5.fillna(0)

    away_pens_drawn_6.columns = ['season', 'game_id', 'date', 'player_id',
                                'player_name', 'PEND']
    away_pens_taken_6.columns = ['season', 'game_id', 'date', 'player_id',
                                    'player_name', 'PENT']

    away_oi_pens_6 = away_pens_taken_6.merge(away_pens_drawn_6,
                                          on=['season', 'game_id', 'date',
                                              'player_id', 'player_name'],
                                          how='outer')

    away_oi_pens_6 = away_oi_pens_6.fillna(0)

    away_oi_pens_list = [away_oi_pens_1, away_oi_pens_2, away_oi_pens_3, away_oi_pens_4,
                       away_oi_pens_5, away_oi_pens_6]

    away_pens = pd.concat(away_oi_pens_list)

    away_pens = away_pens.groupby(['season', 'game_id', 'date',
                                         'player_id', 'player_name'])\
                                ['PEND', 'PENT'].sum().reset_index()

    away_pens['team'] = pbp_df.away_team
    home_pens['team'] = pbp_df.home_team

    penalties = [away_pens, home_pens]

    penalties_df = pd.concat(penalties, sort=False).reset_index()

    penalties_df = penalties_df[['season', 'game_id', 'date', 'team',
                                 'player_id', 'player_name', 'PENT', 'PEND']]
    penalties_df[['game_id']] = penalties_df[['game_id']].astype(int)

    return penalties_df

def calc_adj_onice_stats(pbp_df):
    '''
    this function combines all the onice stats for all situations into one
    dataframe. These stats include shot metrics, TOI, and penalties taken and
    drawn.

    Inputs:
    pbp_df - play by play dataframe

    Outputs:
    onice_df - dataframe of players onice statistics
    '''
    toi_df = calc_toi(pbp_df)
    shots_df = calc_adj_on_ice_shots(pbp_df)
    pens_df = calc_on_ice_pens(pbp_df)

    on_ice_stats_df = shots_df.merge(toi_df,
                                     on=['player_id', 'player_name'],
                                     how='outer')

    on_ice_stats_df = on_ice_stats_df.merge(pens_df,
                                            on=['season', 'game_id', 'date',
                                                'player_id', 'player_name',
                                                'team'],
                                            how='outer')

    on_ice_stats_df.loc[:, ('toi')] = round(on_ice_stats_df.loc[:, ('toi')] / 60, 2)

    on_ice_stats_df = on_ice_stats_df[on_ice_stats_df.player_id != 0]
    on_ice_stats_df = on_ice_stats_df[~on_ice_stats_df.player_id.isin(pbp_df.home_goalie_id.unique())]
    on_ice_stats_df = on_ice_stats_df[~on_ice_stats_df.player_id.isin(pbp_df.away_goalie_id.unique())]

    on_ice_stats_df = on_ice_stats_df[['season', 'game_id', 'date', 'team',
                                       'player_id', 'player_name', 'toi', 'CF',
                                       'CA', 'FF', 'FA', 'SF', 'SA', 'GF',
                                       'GA', 'xga', 'xgf', 'PENT', 'PEND']]

    return on_ice_stats_df.reset_index(drop=True)

def calc_onice_stats(pbp_df):
    '''
    this function combines all the onice stats for all situations into one
    dataframe. These stats include shot metrics, TOI, and penalties taken and
    drawn.

    Inputs:
    pbp_df - play by play dataframe

    Outputs:
    onice_df - dataframe of players onice statistics
    '''
    toi_df = calc_toi(pbp_df)
    shots_df = calc_on_ice_shots(pbp_df)
    pens_df = calc_on_ice_pens(pbp_df)

    on_ice_stats_df = shots_df.merge(toi_df,
                                     on=['player_id', 'player_name'],
                                     how='outer')

    on_ice_stats_df = on_ice_stats_df.merge(pens_df,
                                            on=['season', 'game_id', 'date',
                                                'player_id', 'player_name',
                                                'team'],
                                            how='outer')

    on_ice_stats_df.loc[:, ('toi')] = round(on_ice_stats_df.loc[:, ('toi')] / 60, 2)

    on_ice_stats_df = on_ice_stats_df[on_ice_stats_df.player_id != 0]
    on_ice_stats_df = on_ice_stats_df[~on_ice_stats_df.player_id.isin(pbp_df.home_goalie_id.unique())]
    on_ice_stats_df = on_ice_stats_df[~on_ice_stats_df.player_id.isin(pbp_df.away_goalie_id.unique())]

    on_ice_stats_df = on_ice_stats_df[['season', 'game_id', 'date', 'team',
                                       'player_id', 'player_name', 'toi', 'CF',
                                       'CA', 'FF', 'FA', 'SF', 'SA', 'GF',
                                       'GA', 'xga', 'xgf', 'PENT', 'PEND']]

    return on_ice_stats_df.reset_index(drop=True)


def main():

    return

if __name__ == '__main__':
    main()
