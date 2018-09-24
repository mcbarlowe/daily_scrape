'''
This script calculates individual stats for both home and away team given
the strength state passed to the functions. It works for all strength states
except for all situations. Harry Shomers skater totals include the goalie so
if you wanted 5v5 you would actually pass 6 for each skaters. In cases where
the strength state is not even the first number passed will be the first number
in the strength state i.e. 5 and 6 would be equivalent to 4v5 and 6 and 5 is
5v4 etc.
'''
import pandas as pd
import numpy as np
import calc_all_sits_ind_stats as es_metrics


def calc_adj_ind_shot_metrics(pbp_df, pp_skaters_num, pk_skaters_num):
    '''
    function to calculate individual shot metrics and return a data
    frame with them

    Inputs:
    pbp_df - play by play dataframe

    Ouputs:
    ind_shots_df - df with calculated iSF, iCF, iFF need to add ixG to
                   this later once xg model is finished
    '''

    corsi = ['SHOT', 'BLOCK', 'MISS', 'GOAL']
    fenwick = ['SHOT', 'MISS', 'GOAL']
    shot = ['SHOT', 'GOAL']

    home_5v4_df = pbp_df[(pbp_df.home_players == pp_skaters_num) &
                         (pbp_df.away_players == pk_skaters_num) &
                         (~pbp_df.home_goalie.isna())]

    away_5v4_df = pbp_df[(pbp_df.home_players == pk_skaters_num) &
                         (pbp_df.away_players == pp_skaters_num) &
                         (~pbp_df.away_goalie.isna())]

    home_corsi = home_5v4_df[(home_5v4_df.event.isin(corsi)) &
                             ((home_5v4_df.p1_id == home_5v4_df.homeplayer1_id) |
                              (home_5v4_df.p1_id == home_5v4_df.homeplayer2_id) |
                              (home_5v4_df.p1_id == home_5v4_df.homeplayer3_id) |
                              (home_5v4_df.p1_id == home_5v4_df.homeplayer4_id) |
                              (home_5v4_df.p1_id == home_5v4_df.homeplayer5_id) |
                              (home_5v4_df.p1_id == home_5v4_df.homeplayer6_id))].\
                 groupby(['season', 'game_id', 'date',
                          'p1_id', 'p1_name'])['adj_corsi'].sum().reset_index()

    home_fenwick = home_5v4_df[(home_5v4_df.event.isin(fenwick)) &
                               ((home_5v4_df.p1_id == home_5v4_df.homeplayer1_id) |
                                (home_5v4_df.p1_id == home_5v4_df.homeplayer2_id) |
                                (home_5v4_df.p1_id == home_5v4_df.homeplayer3_id) |
                                (home_5v4_df.p1_id == home_5v4_df.homeplayer4_id) |
                                (home_5v4_df.p1_id == home_5v4_df.homeplayer5_id) |
                                (home_5v4_df.p1_id == home_5v4_df.homeplayer6_id))].\
                 groupby(['season', 'game_id', 'date',
                          'p1_id', 'p1_name'])['adj_fenwick'].sum().reset_index()

    home_xg = home_5v4_df[(home_5v4_df.event.isin(fenwick)) &
                               ((home_5v4_df.p1_id == home_5v4_df.homeplayer1_id) |
                                (home_5v4_df.p1_id == home_5v4_df.homeplayer2_id) |
                                (home_5v4_df.p1_id == home_5v4_df.homeplayer3_id) |
                                (home_5v4_df.p1_id == home_5v4_df.homeplayer4_id) |
                                (home_5v4_df.p1_id == home_5v4_df.homeplayer5_id) |
                                (home_5v4_df.p1_id == home_5v4_df.homeplayer6_id))].\
                 groupby(['season', 'game_id', 'date',
                          'p1_id', 'p1_name'])['adj_xg'].sum().reset_index()

    home_shot = home_5v4_df[(home_5v4_df.event.isin(corsi)) &
                            ((home_5v4_df.p1_id == home_5v4_df.homeplayer1_id) |
                             (home_5v4_df.p1_id == home_5v4_df.homeplayer2_id) |
                             (home_5v4_df.p1_id == home_5v4_df.homeplayer3_id) |
                             (home_5v4_df.p1_id == home_5v4_df.homeplayer4_id) |
                             (home_5v4_df.p1_id == home_5v4_df.homeplayer5_id) |
                             (home_5v4_df.p1_id == home_5v4_df.homeplayer6_id))].\
                 groupby(['season', 'game_id', 'date',
                          'p1_id', 'p1_name'])['is_shot'].sum().reset_index()

    home_corsi.columns = ['season', 'game_id', 'date',  'player_id',
                          'player_name', 'iCF']

    home_fenwick.columns = ['season', 'game_id', 'date',
                            'player_id', 'player_name', 'iFF']

    home_shot.columns = ['season', 'game_id', 'date',
                         'player_id', 'player_name', 'iSF']

    home_xg.columns = ['season', 'game_id', 'date',
                       'player_id', 'player_name', 'ixg']

    home_metrics_df = home_corsi.merge(home_fenwick,
                                       on=['season', 'game_id', 'date',
                                           'player_id', 'player_name'],
                                       how='outer')

    home_metrics_df = home_metrics_df.merge(home_shot,
                                            on=['season', 'game_id', 'date',
                                                'player_id', 'player_name'],
                                            how='outer')

    home_metrics_df = home_metrics_df.merge(home_xg,
                                            on=['season', 'game_id', 'date',
                                                'player_id', 'player_name'],
                                            how='outer')

    home_metrics_df = home_metrics_df.fillna(0)

    away_corsi = away_5v4_df[(away_5v4_df.event.isin(corsi)) &
                             ((away_5v4_df.p1_id == away_5v4_df.awayplayer1_id) |
                              (away_5v4_df.p1_id == away_5v4_df.awayplayer2_id) |
                              (away_5v4_df.p1_id == away_5v4_df.awayplayer3_id) |
                              (away_5v4_df.p1_id == away_5v4_df.awayplayer4_id) |
                              (away_5v4_df.p1_id == away_5v4_df.awayplayer5_id) |
                              (away_5v4_df.p1_id == away_5v4_df.awayplayer6_id))].\
                 groupby(['season', 'game_id', 'date',
                          'p1_id', 'p1_name'])['adj_corsi'].sum().reset_index()

    away_fenwick = away_5v4_df[(away_5v4_df.event.isin(fenwick)) &
                               ((away_5v4_df.p1_id == away_5v4_df.awayplayer1_id) |
                                (away_5v4_df.p1_id == away_5v4_df.awayplayer2_id) |
                                (away_5v4_df.p1_id == away_5v4_df.awayplayer3_id) |
                                (away_5v4_df.p1_id == away_5v4_df.awayplayer4_id) |
                                (away_5v4_df.p1_id == away_5v4_df.awayplayer5_id) |
                                (away_5v4_df.p1_id == away_5v4_df.awayplayer6_id))].\
                 groupby(['season', 'game_id', 'date',
                          'p1_id', 'p1_name'])['adj_fenwick'].sum().reset_index()

    away_xg = away_5v4_df[(away_5v4_df.event.isin(fenwick)) &
                               ((away_5v4_df.p1_id == away_5v4_df.awayplayer1_id) |
                                (away_5v4_df.p1_id == away_5v4_df.awayplayer2_id) |
                                (away_5v4_df.p1_id == away_5v4_df.awayplayer3_id) |
                                (away_5v4_df.p1_id == away_5v4_df.awayplayer4_id) |
                                (away_5v4_df.p1_id == away_5v4_df.awayplayer5_id) |
                                (away_5v4_df.p1_id == away_5v4_df.awayplayer6_id))].\
                 groupby(['season', 'game_id', 'date',
                          'p1_id', 'p1_name'])['adj_xg'].sum().reset_index()

    away_shot = away_5v4_df[(away_5v4_df.event.isin(corsi)) &
                            ((away_5v4_df.p1_id == away_5v4_df.awayplayer1_id) |
                             (away_5v4_df.p1_id == away_5v4_df.awayplayer2_id) |
                             (away_5v4_df.p1_id == away_5v4_df.awayplayer3_id) |
                             (away_5v4_df.p1_id == away_5v4_df.awayplayer4_id) |
                             (away_5v4_df.p1_id == away_5v4_df.awayplayer5_id) |
                             (away_5v4_df.p1_id == away_5v4_df.awayplayer6_id))].\
                 groupby(['season', 'game_id', 'date',
                          'p1_id', 'p1_name'])['is_shot'].sum().reset_index()

    away_corsi.columns = ['season', 'game_id', 'date',  'player_id',
                          'player_name', 'iCF']

    away_fenwick.columns = ['season', 'game_id', 'date',
                            'player_id', 'player_name', 'iFF']

    away_shot.columns = ['season', 'game_id', 'date',
                         'player_id', 'player_name', 'iSF']

    away_xg.columns = ['season', 'game_id', 'date',
                         'player_id', 'player_name', 'ixg']

    away_metrics_df = away_corsi.merge(away_fenwick,
                                       on=['season', 'game_id', 'date',
                                           'player_id', 'player_name'],
                                       how='outer')

    away_metrics_df = away_metrics_df.merge(away_shot,
                                            on=['season', 'game_id', 'date',
                                                'player_id', 'player_name'],
                                            how='outer')

    away_metrics_df = away_metrics_df.merge(away_xg,
                                            on=['season', 'game_id', 'date',
                                                'player_id', 'player_name'],
                                            how='outer')

    away_metrics_df = away_metrics_df.fillna(0)

    metrics_df = pd.concat([away_metrics_df, home_metrics_df], sort=False)

    metrics_df.loc[:, ('player_id', 'game_id','iCF', 'iFF', 'iSF')] = \
        metrics_df.loc[:, ('player_id', 'game_id', 'iCF', 'iFF', 'iSF')].astype(int)

    return metrics_df
def calc_ind_shot_metrics(pbp_df, pp_skaters_num, pk_skaters_num):
    '''
    function to calculate individual shot metrics and return a data
    frame with them

    Inputs:
    pbp_df - play by play dataframe

    Ouputs:
    ind_shots_df - df with calculated iSF, iCF, iFF need to add ixG to
                   this later once xg model is finished
    '''

    corsi = ['SHOT', 'BLOCK', 'MISS', 'GOAL']
    fenwick = ['SHOT', 'MISS', 'GOAL']
    shot = ['SHOT', 'GOAL']

    home_5v4_df = pbp_df[(pbp_df.home_players == pp_skaters_num) &
                         (pbp_df.away_players == pk_skaters_num) &
                         (~pbp_df.home_goalie.isna())]

    away_5v4_df = pbp_df[(pbp_df.home_players == pk_skaters_num) &
                         (pbp_df.away_players == pp_skaters_num) &
                         (~pbp_df.away_goalie.isna())]

    home_corsi = home_5v4_df[(home_5v4_df.event.isin(corsi)) &
                             ((home_5v4_df.p1_id == home_5v4_df.homeplayer1_id) |
                              (home_5v4_df.p1_id == home_5v4_df.homeplayer2_id) |
                              (home_5v4_df.p1_id == home_5v4_df.homeplayer3_id) |
                              (home_5v4_df.p1_id == home_5v4_df.homeplayer4_id) |
                              (home_5v4_df.p1_id == home_5v4_df.homeplayer5_id) |
                              (home_5v4_df.p1_id == home_5v4_df.homeplayer6_id))].\
                 groupby(['season', 'game_id', 'date',
                          'p1_id', 'p1_name'])['is_corsi'].sum().reset_index()

    home_fenwick = home_5v4_df[(home_5v4_df.event.isin(fenwick)) &
                               ((home_5v4_df.p1_id == home_5v4_df.homeplayer1_id) |
                                (home_5v4_df.p1_id == home_5v4_df.homeplayer2_id) |
                                (home_5v4_df.p1_id == home_5v4_df.homeplayer3_id) |
                                (home_5v4_df.p1_id == home_5v4_df.homeplayer4_id) |
                                (home_5v4_df.p1_id == home_5v4_df.homeplayer5_id) |
                                (home_5v4_df.p1_id == home_5v4_df.homeplayer6_id))].\
                 groupby(['season', 'game_id', 'date',
                          'p1_id', 'p1_name'])['is_fenwick'].sum().reset_index()

    home_xg = home_5v4_df[(home_5v4_df.event.isin(fenwick)) &
                               ((home_5v4_df.p1_id == home_5v4_df.homeplayer1_id) |
                                (home_5v4_df.p1_id == home_5v4_df.homeplayer2_id) |
                                (home_5v4_df.p1_id == home_5v4_df.homeplayer3_id) |
                                (home_5v4_df.p1_id == home_5v4_df.homeplayer4_id) |
                                (home_5v4_df.p1_id == home_5v4_df.homeplayer5_id) |
                                (home_5v4_df.p1_id == home_5v4_df.homeplayer6_id))].\
                 groupby(['season', 'game_id', 'date',
                          'p1_id', 'p1_name'])['xg'].sum().reset_index()

    home_shot = home_5v4_df[(home_5v4_df.event.isin(corsi)) &
                            ((home_5v4_df.p1_id == home_5v4_df.homeplayer1_id) |
                             (home_5v4_df.p1_id == home_5v4_df.homeplayer2_id) |
                             (home_5v4_df.p1_id == home_5v4_df.homeplayer3_id) |
                             (home_5v4_df.p1_id == home_5v4_df.homeplayer4_id) |
                             (home_5v4_df.p1_id == home_5v4_df.homeplayer5_id) |
                             (home_5v4_df.p1_id == home_5v4_df.homeplayer6_id))].\
                 groupby(['season', 'game_id', 'date',
                          'p1_id', 'p1_name'])['is_shot'].sum().reset_index()

    home_corsi.columns = ['season', 'game_id', 'date',  'player_id',
                          'player_name', 'iCF']

    home_fenwick.columns = ['season', 'game_id', 'date',
                            'player_id', 'player_name', 'iFF']

    home_shot.columns = ['season', 'game_id', 'date',
                         'player_id', 'player_name', 'iSF']

    home_xg.columns = ['season', 'game_id', 'date',
                       'player_id', 'player_name', 'ixg']

    home_metrics_df = home_corsi.merge(home_fenwick,
                                       on=['season', 'game_id', 'date',
                                           'player_id', 'player_name'],
                                       how='outer')

    home_metrics_df = home_metrics_df.merge(home_shot,
                                            on=['season', 'game_id', 'date',
                                                'player_id', 'player_name'],
                                            how='outer')

    home_metrics_df = home_metrics_df.merge(home_xg,
                                            on=['season', 'game_id', 'date',
                                                'player_id', 'player_name'],
                                            how='outer')

    home_metrics_df = home_metrics_df.fillna(0)

    away_corsi = away_5v4_df[(away_5v4_df.event.isin(corsi)) &
                             ((away_5v4_df.p1_id == away_5v4_df.awayplayer1_id) |
                              (away_5v4_df.p1_id == away_5v4_df.awayplayer2_id) |
                              (away_5v4_df.p1_id == away_5v4_df.awayplayer3_id) |
                              (away_5v4_df.p1_id == away_5v4_df.awayplayer4_id) |
                              (away_5v4_df.p1_id == away_5v4_df.awayplayer5_id) |
                              (away_5v4_df.p1_id == away_5v4_df.awayplayer6_id))].\
                 groupby(['season', 'game_id', 'date',
                          'p1_id', 'p1_name'])['is_corsi'].sum().reset_index()

    away_fenwick = away_5v4_df[(away_5v4_df.event.isin(fenwick)) &
                               ((away_5v4_df.p1_id == away_5v4_df.awayplayer1_id) |
                                (away_5v4_df.p1_id == away_5v4_df.awayplayer2_id) |
                                (away_5v4_df.p1_id == away_5v4_df.awayplayer3_id) |
                                (away_5v4_df.p1_id == away_5v4_df.awayplayer4_id) |
                                (away_5v4_df.p1_id == away_5v4_df.awayplayer5_id) |
                                (away_5v4_df.p1_id == away_5v4_df.awayplayer6_id))].\
                 groupby(['season', 'game_id', 'date',
                          'p1_id', 'p1_name'])['is_fenwick'].sum().reset_index()

    away_xg = away_5v4_df[(away_5v4_df.event.isin(fenwick)) &
                               ((away_5v4_df.p1_id == away_5v4_df.awayplayer1_id) |
                                (away_5v4_df.p1_id == away_5v4_df.awayplayer2_id) |
                                (away_5v4_df.p1_id == away_5v4_df.awayplayer3_id) |
                                (away_5v4_df.p1_id == away_5v4_df.awayplayer4_id) |
                                (away_5v4_df.p1_id == away_5v4_df.awayplayer5_id) |
                                (away_5v4_df.p1_id == away_5v4_df.awayplayer6_id))].\
                 groupby(['season', 'game_id', 'date',
                          'p1_id', 'p1_name'])['xg'].sum().reset_index()

    away_shot = away_5v4_df[(away_5v4_df.event.isin(corsi)) &
                            ((away_5v4_df.p1_id == away_5v4_df.awayplayer1_id) |
                             (away_5v4_df.p1_id == away_5v4_df.awayplayer2_id) |
                             (away_5v4_df.p1_id == away_5v4_df.awayplayer3_id) |
                             (away_5v4_df.p1_id == away_5v4_df.awayplayer4_id) |
                             (away_5v4_df.p1_id == away_5v4_df.awayplayer5_id) |
                             (away_5v4_df.p1_id == away_5v4_df.awayplayer6_id))].\
                 groupby(['season', 'game_id', 'date',
                          'p1_id', 'p1_name'])['is_shot'].sum().reset_index()

    away_corsi.columns = ['season', 'game_id', 'date',  'player_id',
                          'player_name', 'iCF']

    away_fenwick.columns = ['season', 'game_id', 'date',
                            'player_id', 'player_name', 'iFF']

    away_shot.columns = ['season', 'game_id', 'date',
                         'player_id', 'player_name', 'iSF']

    away_xg.columns = ['season', 'game_id', 'date',
                         'player_id', 'player_name', 'ixg']

    away_metrics_df = away_corsi.merge(away_fenwick,
                                       on=['season', 'game_id', 'date',
                                           'player_id', 'player_name'],
                                       how='outer')

    away_metrics_df = away_metrics_df.merge(away_shot,
                                            on=['season', 'game_id', 'date',
                                                'player_id', 'player_name'],
                                            how='outer')

    away_metrics_df = away_metrics_df.merge(away_xg,
                                            on=['season', 'game_id', 'date',
                                                'player_id', 'player_name'],
                                            how='outer')

    away_metrics_df = away_metrics_df.fillna(0)

    metrics_df = pd.concat([away_metrics_df, home_metrics_df], sort=False)

    metrics_df.loc[:, ('game_id', 'player_id', 'iCF', 'iFF', 'iSF')] = \
        metrics_df.loc[:, ('game_id', 'player_id', 'iCF', 'iFF', 'iSF')].astype(int)

    return metrics_df

def calc_ind_hits(pbp_df, pp_skaters_num, pk_skaters_num):
    '''
    function calculates hits for and against from the pbp_df.

    Input:
    pbp_df - play by play dataframe

    Output:
    hit_df - dataframe of each players hits stats
    '''

    home_5v4_df = pbp_df[(pbp_df.home_players == pp_skaters_num) &
                         (pbp_df.away_players == pk_skaters_num) &
                         (~pbp_df.home_goalie.isna())]

    away_5v4_df = pbp_df[(pbp_df.home_players == pk_skaters_num) &
                         (pbp_df.away_players == pp_skaters_num) &
                         (~pbp_df.away_goalie.isna())]

    home_hit_for = home_5v4_df[(home_5v4_df.event == 'HIT') &
                               ((home_5v4_df.p1_id == home_5v4_df.homeplayer1_id) |
                                (home_5v4_df.p1_id == home_5v4_df.homeplayer2_id) |
                                (home_5v4_df.p1_id == home_5v4_df.homeplayer3_id) |
                                (home_5v4_df.p1_id == home_5v4_df.homeplayer4_id) |
                                (home_5v4_df.p1_id == home_5v4_df.homeplayer5_id) |
                                (home_5v4_df.p1_id == home_5v4_df.homeplayer6_id))].\
                 groupby(['season', 'game_id', 'date',
                          'p1_id', 'p1_name']).size().\
                 reset_index()

    home_hit_for.columns = ['season', 'game_id', 'date',
                            'player_id', 'player_name', 'iHF']

    home_hit_against = home_5v4_df[(home_5v4_df.event == 'HIT') &
                              ((home_5v4_df.p2_id == home_5v4_df.homeplayer1_id) |
                              (home_5v4_df.p2_id == home_5v4_df.homeplayer2_id) |
                              (home_5v4_df.p2_id == home_5v4_df.homeplayer3_id) |
                              (home_5v4_df.p2_id == home_5v4_df.homeplayer4_id) |
                              (home_5v4_df.p2_id == home_5v4_df.homeplayer5_id) |
                              (home_5v4_df.p2_id == home_5v4_df.homeplayer6_id))].\
                 groupby(['season', 'game_id', 'date',
                          'p2_id', 'p2_name']).size().\
                 reset_index()

    home_hit_against.columns = ['season', 'game_id', 'date',
                                'player_id', 'player_name', 'iHA']

    home_hit_df = home_hit_for.merge(home_hit_against,
                                     on=['season', 'game_id', 'date',
                                         'player_id', 'player_name'],
                                     how='outer')

    home_hit_df = home_hit_df.fillna(0)

    away_hit_for = away_5v4_df[(away_5v4_df.event == 'HIT') &
                               ((away_5v4_df.p1_id == away_5v4_df.awayplayer1_id) |
                                (away_5v4_df.p1_id == away_5v4_df.awayplayer2_id) |
                                (away_5v4_df.p1_id == away_5v4_df.awayplayer3_id) |
                                (away_5v4_df.p1_id == away_5v4_df.awayplayer4_id) |
                                (away_5v4_df.p1_id == away_5v4_df.awayplayer5_id) |
                                (away_5v4_df.p1_id == away_5v4_df.awayplayer6_id))].\
                 groupby(['season', 'game_id', 'date',
                          'p1_id', 'p1_name']).size().\
                 reset_index()

    away_hit_for.columns = ['season', 'game_id', 'date',
                            'player_id', 'player_name', 'iHF']

    away_hit_against = away_5v4_df[(away_5v4_df.event == 'HIT') &
                              ((away_5v4_df.p2_id == away_5v4_df.awayplayer1_id) |
                              (away_5v4_df.p2_id == away_5v4_df.awayplayer2_id) |
                              (away_5v4_df.p2_id == away_5v4_df.awayplayer3_id) |
                              (away_5v4_df.p2_id == away_5v4_df.awayplayer4_id) |
                              (away_5v4_df.p2_id == away_5v4_df.awayplayer5_id) |
                              (away_5v4_df.p2_id == away_5v4_df.awayplayer6_id))].\
                 groupby(['season', 'game_id', 'date',
                          'p2_id', 'p2_name']).size().\
                 reset_index()

    away_hit_against.columns = ['season', 'game_id', 'date',
                                'player_id', 'player_name', 'iHA']

    away_hit_df = away_hit_for.merge(away_hit_against,
                                     on=['season', 'game_id', 'date',
                                         'player_id', 'player_name'],
                                     how='outer')

    away_hit_df = away_hit_df.fillna(0)

    hit_df_list = [home_hit_df, away_hit_df]

    hit_df = pd.concat(hit_df_list, sort=False).reset_index()

    hit_df = hit_df[['season', 'game_id', 'date',
                     'player_id', 'player_name', 'iHF', 'iHA']]

    hit_df.loc[:, ('season', 'game_id', 'player_id', 'iHF', 'iHA')] = \
            hit_df.loc[:, ('season', 'game_id', 'player_id', 'iHF', 'iHA')].astype(int)

    return hit_df

def calc_pp_gata(pbp_df, pp_skaters_num, pk_skaters_num):
    '''
    function calculates giveaways and takeaways from the pbp_df.

    Input:
    pbp_df - play by play dataframe

    Output:
    hit_df - dataframe of each players GA/TA stats
    '''
    home_5v4_df = pbp_df[(pbp_df.home_players == pp_skaters_num) &
                         (pbp_df.away_players == pk_skaters_num) &
                         (~pbp_df.home_goalie.isna())]

    away_5v4_df = pbp_df[(pbp_df.home_players == pk_skaters_num) &
                         (pbp_df.away_players == pp_skaters_num) &
                         (~pbp_df.away_goalie.isna())]

    home_ga = home_5v4_df[(home_5v4_df.event == 'GIVE') &
                              ((home_5v4_df.p1_id == home_5v4_df.homeplayer1_id) |
                              (home_5v4_df.p1_id == home_5v4_df.homeplayer2_id) |
                              (home_5v4_df.p1_id == home_5v4_df.homeplayer3_id) |
                              (home_5v4_df.p1_id == home_5v4_df.homeplayer4_id) |
                              (home_5v4_df.p1_id == home_5v4_df.homeplayer5_id) |
                              (home_5v4_df.p1_id == home_5v4_df.homeplayer6_id))].\
                 groupby(['season', 'game_id', 'date',
                          'p1_id', 'p1_name']).size().\
                 reset_index()

    home_ga.columns = ['season', 'game_id', 'date', 'player_id',
                       'player_name', 'iGA']

    home_ta = home_5v4_df[(home_5v4_df.event == 'TAKE') &
                              ((home_5v4_df.p1_id == home_5v4_df.homeplayer1_id) |
                              (home_5v4_df.p1_id == home_5v4_df.homeplayer2_id) |
                              (home_5v4_df.p1_id == home_5v4_df.homeplayer3_id) |
                              (home_5v4_df.p1_id == home_5v4_df.homeplayer4_id) |
                              (home_5v4_df.p1_id == home_5v4_df.homeplayer5_id) |
                              (home_5v4_df.p1_id == home_5v4_df.homeplayer6_id))].\
                 groupby(['season', 'game_id', 'date',
                          'p1_id', 'p1_name']).size().\
                 reset_index()

    home_ta.columns = ['season', 'game_id', 'date', 'player_id',
                       'player_name', 'iTA']


    home_gata = home_ga.merge(home_ta, on=['season', 'game_id', 'date',
                                           'player_id', 'player_name'],
                              how='outer')

    home_gata = home_gata.fillna(0)

    away_ga = away_5v4_df[(away_5v4_df.event == 'GIVE') &
                              ((away_5v4_df.p1_id == away_5v4_df.awayplayer1_id) |
                              (away_5v4_df.p1_id == away_5v4_df.awayplayer2_id) |
                              (away_5v4_df.p1_id == away_5v4_df.awayplayer3_id) |
                              (away_5v4_df.p1_id == away_5v4_df.awayplayer4_id) |
                              (away_5v4_df.p1_id == away_5v4_df.awayplayer5_id) |
                              (away_5v4_df.p1_id == away_5v4_df.awayplayer6_id))].\
                 groupby(['season', 'game_id', 'date',
                          'p1_id', 'p1_name']).size().\
                 reset_index()

    away_ga.columns = ['season', 'game_id', 'date', 'player_id',
                       'player_name', 'iGA']

    away_ta = away_5v4_df[(away_5v4_df.event == 'TAKE') &
                              ((away_5v4_df.p1_id == away_5v4_df.awayplayer1_id) |
                              (away_5v4_df.p1_id == away_5v4_df.awayplayer2_id) |
                              (away_5v4_df.p1_id == away_5v4_df.awayplayer3_id) |
                              (away_5v4_df.p1_id == away_5v4_df.awayplayer4_id) |
                              (away_5v4_df.p1_id == away_5v4_df.awayplayer5_id) |
                              (away_5v4_df.p1_id == away_5v4_df.awayplayer6_id))].\
                 groupby(['season', 'game_id', 'date',
                          'p1_id', 'p1_name']).size().\
                 reset_index()

    away_ta.columns = ['season', 'game_id', 'date', 'player_id',
                       'player_name', 'iTA']


    away_gata = away_ga.merge(away_ta, on=['season', 'game_id', 'date',
                                           'player_id', 'player_name'],
                              how='outer')


    away_gata = away_gata.fillna(0)

    gata = [home_gata, away_gata]

    gata_df = pd.concat(gata, sort=False)

    gata_df.loc[:, ('season', 'game_id', 'player_id', 'iGA', 'iTA')] = \
    gata_df.loc[:, ('season', 'game_id', 'player_id', 'iGA', 'iTA')].astype(int)

    gata_df = gata_df[['season', 'game_id', 'date', 'player_id',
                       'player_name', 'iGA', 'iTA']]

    return gata_df

def calc_pp_blocks(pbp_df, pp_skaters_num, pk_skaters_num):
    '''
    function to calculate a players blocks while on the pp

    Inputs:
    pbp_df - dataframe of play by play data

    Outputs:
    blk_df - dataframe of blocks by players on the power play
    '''

    home_5v4_df = pbp_df[(pbp_df.home_players == pp_skaters_num) &
                         (pbp_df.away_players == pk_skaters_num) &
                         (~pbp_df.home_goalie.isna())]

    away_5v4_df = pbp_df[(pbp_df.home_players == pk_skaters_num) &
                         (pbp_df.away_players == pp_skaters_num) &
                         (~pbp_df.away_goalie.isna())]

    home_blk_df = home_5v4_df[(home_5v4_df.event == 'BLOCK') &
                              ((home_5v4_df.p2_id == home_5v4_df.homeplayer1_id) |
                              (home_5v4_df.p2_id == home_5v4_df.homeplayer2_id) |
                              (home_5v4_df.p2_id == home_5v4_df.homeplayer3_id) |
                              (home_5v4_df.p2_id == home_5v4_df.homeplayer4_id) |
                              (home_5v4_df.p2_id == home_5v4_df.homeplayer5_id) |
                              (home_5v4_df.p2_id == home_5v4_df.homeplayer6_id))].\
                 groupby(['season', 'game_id', 'date',
                          'p2_id', 'p2_name']).size().\
                 reset_index()

    away_blk_df = away_5v4_df[(away_5v4_df.event == 'BLOCK') &
                              ((away_5v4_df.p2_id == away_5v4_df.awayplayer1_id) |
                              (away_5v4_df.p2_id == away_5v4_df.awayplayer2_id) |
                              (away_5v4_df.p2_id == away_5v4_df.awayplayer3_id) |
                              (away_5v4_df.p2_id == away_5v4_df.awayplayer4_id) |
                              (away_5v4_df.p2_id == away_5v4_df.awayplayer5_id) |
                              (away_5v4_df.p2_id == away_5v4_df.awayplayer6_id))].\
                 groupby(['season', 'game_id', 'date',
                          'p2_id', 'p2_name']).size().\
                 reset_index()


    blk_list = [home_blk_df, away_blk_df]

    blk_df = pd.concat(blk_list, sort=False)

    blk_df.columns = ['season', 'game_id', 'date',
                      'player_id', 'player_name', 'BLK']

    blk_df.loc[:, ('season', 'game_id', 'player_id', 'BLK')] = \
    blk_df.loc[:, ('season', 'game_id', 'player_id', 'BLK')].astype(int)

    return blk_df

def calc_pp_faceoffs(pbp_df, pp_skaters_num, pk_skaters_num):
    '''
    calculate the faceoffs won and lost by a player whose team is on the power
    player

    Inputs:
    pbp_df - play by play dataframe
    pp_skaters_num - number of skaters for team on the power play
    pk_skaters_num - number of skaters for team on the penalty kill

    Outputs
    fo_df - dataframe of FOW and FOL for teams on the PP
    '''

    home_5v4_df = pbp_df[(pbp_df.home_players == pp_skaters_num) &
                         (pbp_df.away_players == pk_skaters_num) &
                         (~pbp_df.home_goalie.isna())]

    away_5v4_df = pbp_df[(pbp_df.home_players == pk_skaters_num) &
                         (pbp_df.away_players == pp_skaters_num) &
                         (~pbp_df.away_goalie.isna())]

    home_fo_won = home_5v4_df[(home_5v4_df.event == 'FAC') &
                    ((home_5v4_df.p1_id == home_5v4_df.homeplayer1_id) |
                     (home_5v4_df.p1_id == home_5v4_df.homeplayer2_id) |
                     (home_5v4_df.p1_id == home_5v4_df.homeplayer3_id) |
                     (home_5v4_df.p1_id == home_5v4_df.homeplayer4_id) |
                     (home_5v4_df.p1_id == home_5v4_df.homeplayer5_id) |
                     (home_5v4_df.p1_id == home_5v4_df.homeplayer6_id))].\
                 groupby(['season', 'game_id', 'date',
                          'p1_id', 'p1_name']).size().\
                 reset_index()

    home_fo_won.columns = ['season', 'game_id', 'date',
                      'player_id', 'player_name', 'FOW']

    home_fo_lost = home_5v4_df[(home_5v4_df.event == 'FAC') &
                    ((home_5v4_df.p2_id == home_5v4_df.homeplayer1_id) |
                     (home_5v4_df.p2_id == home_5v4_df.homeplayer2_id) |
                     (home_5v4_df.p2_id == home_5v4_df.homeplayer3_id) |
                     (home_5v4_df.p2_id == home_5v4_df.homeplayer4_id) |
                     (home_5v4_df.p2_id == home_5v4_df.homeplayer5_id) |
                     (home_5v4_df.p2_id == home_5v4_df.homeplayer6_id))].\
                 groupby(['season', 'game_id', 'date',
                          'p2_id', 'p2_name']).size().\
                 reset_index()

    home_fo_lost.columns = ['season', 'game_id', 'date',
                      'player_id', 'player_name', 'FOL']

    home_5v4_fo_df = home_fo_won.merge(home_fo_lost,
                                       on=['season', 'game_id', 'date',
                                           'player_id', 'player_name'],
                                       how='outer')

    away_fo_won = away_5v4_df[(away_5v4_df.event == 'FAC') &
                    ((away_5v4_df.p1_id == away_5v4_df.awayplayer1_id) |
                     (away_5v4_df.p1_id == away_5v4_df.awayplayer2_id) |
                     (away_5v4_df.p1_id == away_5v4_df.awayplayer3_id) |
                     (away_5v4_df.p1_id == away_5v4_df.awayplayer4_id) |
                     (away_5v4_df.p1_id == away_5v4_df.awayplayer5_id) |
                     (away_5v4_df.p1_id == away_5v4_df.awayplayer6_id))].\
                 groupby(['season', 'game_id', 'date',
                          'p1_id', 'p1_name']).size().\
                 reset_index()

    away_fo_won.columns = ['season', 'game_id', 'date',
                      'player_id', 'player_name', 'FOW']

    away_fo_lost = away_5v4_df[(away_5v4_df.event == 'FAC') &
                    ((away_5v4_df.p2_id == away_5v4_df.awayplayer1_id) |
                     (away_5v4_df.p2_id == away_5v4_df.awayplayer2_id) |
                     (away_5v4_df.p2_id == away_5v4_df.awayplayer3_id) |
                     (away_5v4_df.p2_id == away_5v4_df.awayplayer4_id) |
                     (away_5v4_df.p2_id == away_5v4_df.awayplayer5_id) |
                     (away_5v4_df.p2_id == away_5v4_df.awayplayer6_id))].\
                 groupby(['season', 'game_id', 'date',
                          'p2_id', 'p2_name']).size().\
                 reset_index()

    away_fo_lost.columns = ['season', 'game_id', 'date',
                      'player_id', 'player_name', 'FOL']

    away_5v4_fo_df = away_fo_won.merge(away_fo_lost,
                                       on=['season', 'game_id', 'date',
                                           'player_id', 'player_name'],
                                       how='outer')

    fo_dfs = [home_5v4_fo_df, away_5v4_fo_df]

    fo_5v4 = pd.concat(fo_dfs, sort=False)

    fo_5v4 = fo_5v4.fillna(0)

    fo_5v4 = fo_5v4[['season', 'game_id', 'date', 'player_id',
                             'player_name', 'FOW', 'FOL']]

    fo_5v4.loc[:, ('season', 'game_id', 'player_id', 'FOW', 'FOL')] = \
    fo_5v4.loc[:, ('season', 'game_id', 'player_id', 'FOW', 'FOL')].astype(int)


    return fo_5v4


def calc_pp_ind_points(pbp_df, pp_skaters_num, pk_skaters_num):
    '''
    This function calculates the individual goals and assists scored while at
    a strength state of 5v4

    Input:
    pbp_df - play by play dataframe

    Output:
    five_v_4_df - play by play dataframe of events taken at 5v4 strength
    '''

    home_pp_df = pbp_df[(pbp_df.ev_team == pbp_df.home_team) &
                        (pbp_df.home_players == pp_skaters_num) &
                        (pbp_df.away_players == pk_skaters_num) &
                        (~pbp_df.home_goalie.isna())]

    away_pp_df = pbp_df[(pbp_df.ev_team == pbp_df.away_team) &
                        (pbp_df.home_players == pk_skaters_num) &
                        (pbp_df.away_players == pp_skaters_num) &
                        (~pbp_df.home_goalie.isna())]

    home_pp_points = es_metrics.calc_ind_points(home_pp_df)

    away_pp_points = es_metrics.calc_ind_points(away_pp_df)

    pts_pp = [home_pp_points, away_pp_points]

    pts_pp_df = pd.concat(pts_pp, sort=False)


    pts_pp_df = pts_pp_df[['season', 'game_id', 'date', 'player_id',
                             'player_name', 'g', 'a1', 'a2']]

    pts_pp_df.loc[:, ('season', 'game_id')] = pts_pp_df.loc[:, ('season', 'game_id')].astype(int)

    return pts_pp_df

def calc_pp_penalties(pbp_df, pp_skaters_num, pk_skaters_num):
    '''
    function to calculate penalties drawn and taken for teams on the
    '''

    home_pp_df = pbp_df[(pbp_df.ev_team == pbp_df.home_team) &
                        (pbp_df.home_players == pp_skaters_num) &
                        (pbp_df.away_players == pk_skaters_num) &
                        (pbp_df.is_penalty > 0) &
                        (~pbp_df.home_goalie.isna())]

    away_pp_df = pbp_df[(pbp_df.ev_team == pbp_df.away_team) &
                        (pbp_df.home_players == pk_skaters_num) &
                        (pbp_df.away_players == pp_skaters_num) &
                        (pbp_df.is_penalty > 0) &
                        (~pbp_df.home_goalie.isna())]

    home_pent = home_pp_df[(home_pp_df.event == 'PENL') &
                    ((home_pp_df.p1_id == home_pp_df.homeplayer1_id) |
                     (home_pp_df.p1_id == home_pp_df.homeplayer2_id) |
                     (home_pp_df.p1_id == home_pp_df.homeplayer3_id) |
                     (home_pp_df.p1_id == home_pp_df.homeplayer4_id) |
                     (home_pp_df.p1_id == home_pp_df.homeplayer5_id) |
                     (home_pp_df.p1_id == home_pp_df.homeplayer6_id))].\
                 groupby(['season', 'game_id', 'date',
                          'p1_id', 'p1_name'])['is_penalty'].sum().\
                 reset_index()

    home_pent.columns = ['season', 'game_id', 'date',
                         'player_id', 'player_name', 'iPENT']

    home_pend = home_pp_df[(home_pp_df.event == 'PENL') &
                    ((home_pp_df.p2_id == home_pp_df.homeplayer1_id) |
                     (home_pp_df.p2_id == home_pp_df.homeplayer2_id) |
                     (home_pp_df.p2_id == home_pp_df.homeplayer3_id) |
                     (home_pp_df.p2_id == home_pp_df.homeplayer4_id) |
                     (home_pp_df.p2_id == home_pp_df.homeplayer5_id) |
                     (home_pp_df.p2_id == home_pp_df.homeplayer6_id))].\
                 groupby(['season', 'game_id', 'date',
                          'p2_id', 'p2_name'])['is_penalty'].sum().\
                 reset_index()

    home_pend.columns = ['season', 'game_id', 'date',
                         'player_id', 'player_name', 'iPEND']

    home_pp_penl = home_pent.merge(home_pend,
                                       on=['season', 'game_id', 'date',
                                           'player_id', 'player_name'],
                                       how='outer')

    away_pent = away_pp_df[(away_pp_df.event == 'PENL') &
                    ((away_pp_df.p1_id == away_pp_df.awayplayer1_id) |
                     (away_pp_df.p1_id == away_pp_df.awayplayer2_id) |
                     (away_pp_df.p1_id == away_pp_df.awayplayer3_id) |
                     (away_pp_df.p1_id == away_pp_df.awayplayer4_id) |
                     (away_pp_df.p1_id == away_pp_df.awayplayer5_id) |
                     (away_pp_df.p1_id == away_pp_df.awayplayer6_id))].\
                 groupby(['season', 'game_id', 'date',
                          'p1_id', 'p1_name'])['is_penalty'].sum().\
                 reset_index()

    away_pent.columns = ['season', 'game_id', 'date',
                         'player_id', 'player_name', 'iPENT']

    away_pend = away_pp_df[(away_pp_df.event == 'PENL') &
                    ((away_pp_df.p2_id == away_pp_df.awayplayer1_id) |
                     (away_pp_df.p2_id == away_pp_df.awayplayer2_id) |
                     (away_pp_df.p2_id == away_pp_df.awayplayer3_id) |
                     (away_pp_df.p2_id == away_pp_df.awayplayer4_id) |
                     (away_pp_df.p2_id == away_pp_df.awayplayer5_id) |
                     (away_pp_df.p2_id == away_pp_df.awayplayer6_id))].\
                 groupby(['season', 'game_id', 'date',
                          'p2_id', 'p2_name'])['is_penalty'].sum().\
                 reset_index()

    away_pend.columns = ['season', 'game_id', 'date',
                         'player_id', 'player_name', 'iPEND']

    away_pp_penl = away_pent.merge(away_pend, on=['season', 'game_id', 'date',
                                                  'player_id', 'player_name'],
                                   how='outer')

    penl_dfs = [home_pp_penl, away_pp_penl]

    pp_penl_dfs = pd.concat(penl_dfs, sort=False)

    pp_penl_dfs = pp_penl_dfs.fillna(0)

    pp_penl_dfs = pp_penl_dfs[['season', 'game_id', 'date', 'player_id',
                               'player_name', 'iPENT', 'iPEND']]

    pp_penl_dfs.loc[:, ('season', 'game_id', 'player_id', 'iPENT', 'iPEND')] = \
    pp_penl_dfs.loc[:, ('season', 'game_id', 'player_id', 'iPENT', 'iPEND')].astype(int)

    return pp_penl_dfs


def calc_ppespk_ind_metrics(pbp_df, pp_skaters_num,
                            pk_skaters_num, calc_blk=calc_pp_blocks, \
                            calc_fo=calc_pp_faceoffs,
                            calc_points=calc_pp_ind_points,
                            calc_penalties=calc_pp_penalties,
                            calc_hits=calc_ind_hits,
                            calc_shot_metrics=calc_ind_shot_metrics,
                            calc_gata=calc_pp_gata):
    '''
    this function calculates the individual metrics of each players
    contribution during the game

    Input:
    pbp_df - play by play df
    pp_skaters_num - the first number of the strength state wanted for 5v5
                     would be 6 because of the scraper for 4v5 would be five
    pk_skaters_num - the second number of the strength state wanted for 5v5
                     would be 6 because of the scraper for 4v5 would be six

    Output:
    player_df - individual player stats df
    '''

#calculate each individual stats data frames and then join them all together
#will pull in teams with the on ice measures
    points_df = calc_points(pbp_df, pp_skaters_num, pk_skaters_num)
    metrics_df = calc_shot_metrics(pbp_df, pp_skaters_num, pk_skaters_num)
    penalty_df = calc_penalties(pbp_df, pp_skaters_num, pk_skaters_num)
    hit_df = calc_hits(pbp_df, pp_skaters_num, pk_skaters_num)
    gata_df = calc_gata(pbp_df, pp_skaters_num, pk_skaters_num)
    fo_df = calc_fo(pbp_df, pp_skaters_num, pk_skaters_num)
    blk_df = calc_blk(pbp_df, pp_skaters_num, pk_skaters_num)

    ind_stats_df = metrics_df.merge(points_df,
                                      on=['season', 'game_id', 'date',
                                          'player_id', 'player_name'],
                                    how='outer')

    ind_stats_df = ind_stats_df.merge(penalty_df,
                                      on=['season', 'game_id', 'date',
                                          'player_id', 'player_name'],
                                      how='outer')

    ind_stats_df = ind_stats_df.merge(hit_df,
                                      on=['season', 'game_id', 'date',
                                          'player_id', 'player_name'],
                                      how='outer')

    ind_stats_df = ind_stats_df.merge(gata_df,
                                      on=['season', 'game_id', 'date',
                                          'player_id', 'player_name'],
                                      how='outer')

    ind_stats_df = ind_stats_df.merge(fo_df,
                                      on=['season', 'game_id', 'date',
                                          'player_id', 'player_name'],
                                      how='outer')

    ind_stats_df = ind_stats_df.merge(blk_df,
                                      on=['season', 'game_id', 'date',
                                          'player_id', 'player_name'],
                                      how='outer')

    ind_stats_df = ind_stats_df.fillna(0)


    ind_stats_df.loc[:, ('player_id', 'iCF', 'iFF', 'iSF', 'g',
                         'a1', 'a2', 'iPENT', 'iPEND', 'iHF', 'iHA',
                         'iGA', 'iTA', 'FOW', 'FOL', 'BLK')] = \
    ind_stats_df.loc[:, ('player_id', 'iCF', 'iFF', 'iSF', 'g',
                         'a1', 'a2', 'iPENT', 'iPEND', 'iHF', 'iHA',
                         'iGA', 'iTA', 'FOW', 'FOL', 'BLK')].astype(int)


    ind_stats_df = ind_stats_df[['season', 'game_id', 'date', 'player_id', 'player_name',
                                 'iCF', 'iFF', 'iSF', 'ixg', 'g',
                                 'a1', 'a2', 'iPENT', 'iPEND', 'iHF', 'iHA',
                                 'iGA', 'iTA', 'FOW', 'FOL', 'BLK']]

    ind_stats_df = ind_stats_df[ind_stats_df.player_id != 0]
    ind_stats_df = ind_stats_df[~ind_stats_df.player_id.isin(pbp_df.home_goalie_id.unique())]
    ind_stats_df = ind_stats_df[~ind_stats_df.player_id.isin(pbp_df.away_goalie_id.unique())]

    return ind_stats_df.reset_index(drop=True)

def calc_adj_ppespk_ind_metrics(pbp_df, pp_skaters_num,
                                pk_skaters_num, calc_blk=calc_pp_blocks, \
                                calc_fo=calc_pp_faceoffs,
                                calc_points=calc_pp_ind_points,
                                calc_penalties=calc_pp_penalties,
                                calc_hits=calc_ind_hits,
                                calc_shot_metrics=calc_adj_ind_shot_metrics,
                                calc_gata=calc_pp_gata):
    '''
    this function calculates the individual metrics of each players
    contribution during the game

    Input:
    pbp_df - play by play df
    pp_skaters_num - the first number of the strength state wanted for 5v5
                     would be 6 because of the scraper for 4v5 would be five
    pk_skaters_num - the second number of the strength state wanted for 5v5
                     would be 6 because of the scraper for 4v5 would be six

    Output:
    player_df - individual player stats df
    '''




#calculate each individual stats data frames and then join them all together
#will pull in teams with the on ice measures
    points_df = calc_points(pbp_df, pp_skaters_num, pk_skaters_num)
    metrics_df = calc_shot_metrics(pbp_df, pp_skaters_num, pk_skaters_num)
    penalty_df = calc_penalties(pbp_df, pp_skaters_num, pk_skaters_num)
    hit_df = calc_hits(pbp_df, pp_skaters_num, pk_skaters_num)
    gata_df = calc_gata(pbp_df, pp_skaters_num, pk_skaters_num)
    fo_df = calc_fo(pbp_df, pp_skaters_num, pk_skaters_num)
    blk_df = calc_blk(pbp_df, pp_skaters_num, pk_skaters_num)

    ind_stats_df = metrics_df.merge(points_df,
                                      on=['season', 'game_id', 'date',
                                          'player_id', 'player_name'],
                                    how='outer')

    ind_stats_df = ind_stats_df.merge(penalty_df,
                                      on=['season', 'game_id', 'date',
                                          'player_id', 'player_name'],
                                      how='outer')

    ind_stats_df = ind_stats_df.merge(hit_df,
                                      on=['season', 'game_id', 'date',
                                          'player_id', 'player_name'],
                                      how='outer')

    ind_stats_df = ind_stats_df.merge(gata_df,
                                      on=['season', 'game_id', 'date',
                                          'player_id', 'player_name'],
                                      how='outer')

    ind_stats_df = ind_stats_df.merge(fo_df,
                                      on=['season', 'game_id', 'date',
                                          'player_id', 'player_name'],
                                      how='outer')

    ind_stats_df = ind_stats_df.merge(blk_df,
                                      on=['season', 'game_id', 'date',
                                          'player_id', 'player_name'],
                                      how='outer')

    ind_stats_df = ind_stats_df.fillna(0)


    ind_stats_df.loc[:, ('game_id', 'player_id', 'iCF', 'iFF', 'iSF', 'g',
                         'a1', 'a2', 'iPENT', 'iPEND', 'iHF', 'iHA',
                         'iGA', 'iTA', 'FOW', 'FOL', 'BLK')] = \
    ind_stats_df.loc[:, ('game_id', 'player_id', 'iCF', 'iFF', 'iSF', 'g',
                         'a1', 'a2', 'iPENT', 'iPEND', 'iHF', 'iHA',
                         'iGA', 'iTA', 'FOW', 'FOL', 'BLK')].astype(int)


    ind_stats_df = ind_stats_df[['season', 'game_id', 'date', 'player_id', 'player_name',
                                 'iCF', 'iFF', 'iSF', 'ixg', 'g',
                                 'a1', 'a2', 'iPENT', 'iPEND', 'iHF', 'iHA',
                                 'iGA', 'iTA', 'FOW', 'FOL', 'BLK']]

    ind_stats_df = ind_stats_df[ind_stats_df.player_id != 0]
    ind_stats_df = ind_stats_df[~ind_stats_df.player_id.isin(pbp_df.home_goalie_id.unique())]
    ind_stats_df = ind_stats_df[~ind_stats_df.player_id.isin(pbp_df.away_goalie_id.unique())]

    return ind_stats_df.reset_index(drop=True)

def main():

    return

if __name__ == '__main__':
    main()
