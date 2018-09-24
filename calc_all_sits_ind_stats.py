import pandas as pd
import numpy as np


def calc_ind_points(pbp_df):
    '''
    function calculates individual points for each player in the data
    frame and returns a dataframe with those stats

    Inputs:
    pbp_df - play by play dataframe

    Outputs:
    points_df - dataframe with individual points of players
    '''

    goal_df = pbp_df[pbp_df.event == 'GOAL']\
              .groupby(['season', 'game_id', 'date',
                        'p1_id', 'p1_name'])['is_goal'].sum().reset_index()

    a1_df = pbp_df[pbp_df.event == 'GOAL']\
            .groupby(['season', 'game_id', 'date',
                      'p2_id', 'p2_name'])['is_goal'].sum().reset_index()


    a2_df = pbp_df[pbp_df.event == 'GOAL']\
            .groupby(['season', 'game_id', 'date',
                      'p3_id', 'p3_name'])['is_goal'].sum().reset_index()



    goal_df.columns = ['season', 'game_id', 'date',  'player_id',
                       'player_name', 'g']

    a1_df.columns = ['season', 'game_id', 'date',
                     'player_id', 'player_name', 'a1']

    a2_df.columns = ['season', 'game_id', 'date',
                     'player_id', 'player_name', 'a2']

    points_df = goal_df.merge(a1_df, on=['season', 'game_id', 'date',
                                         'player_id', 'player_name'],
                              how='outer')

    points_df = points_df.merge(a2_df, on=['season', 'game_id', 'date',
                                           'player_id', 'player_name'],
                                how='outer')


    points_df = points_df.fillna(0)

    points_df.loc[:, ('player_id', 'g', 'a1', 'a2')] = \
        points_df.loc[:, ('player_id', 'g', 'a1', 'a2')].astype(int)


    return points_df

def calc_adj_ind_shot_metrics(pbp_df):
    '''
    function to calculate adjusted individual shot metrics and return a data
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

    corsi_df = pbp_df[pbp_df.event.isin(corsi)]\
              .groupby(['season', 'game_id', 'date',
                        'p1_id', 'p1_name'])['adj_corsi'].sum().reset_index()

    fenwick_df = pbp_df[pbp_df.event.isin(fenwick)]\
                 .groupby(['season', 'game_id', 'date',
                           'p1_id', 'p1_name'])['adj_fenwick'].sum().reset_index()

    shot_df = pbp_df[pbp_df.event.isin(shot)]\
                 .groupby(['season', 'game_id', 'date',
                           'p1_id', 'p1_name'])['is_shot'].sum().reset_index()

    xg_df = pbp_df[pbp_df.event.isin(fenwick)]\
                 .groupby(['season', 'game_id', 'date',
                           'p1_id', 'p1_name'])['adj_xg'].sum().reset_index()

    corsi_df.columns = ['season', 'game_id', 'date',  'player_id',
                        'player_name', 'iCF']

    fenwick_df.columns = ['season', 'game_id', 'date',
                          'player_id', 'player_name', 'iFF']

    shot_df.columns = ['season', 'game_id', 'date',
                       'player_id', 'player_name', 'iSF']

    xg_df.columns = ['season', 'game_id', 'date',
                     'player_id', 'player_name', 'ixg']

    metrics_df = corsi_df.merge(fenwick_df,
                                on=['season', 'game_id', 'date',
                                    'player_id', 'player_name'],
                                how='outer')

    metrics_df = metrics_df.merge(shot_df,
                                  on=['season', 'game_id', 'date',
                                      'player_id', 'player_name'],
                                  how='outer')

    metrics_df = metrics_df.merge(xg_df,
                                  on=['season', 'game_id', 'date',
                                      'player_id', 'player_name'],
                                  how='outer')

    metrics_df = metrics_df.fillna(0)

    metrics_df.loc[:, ('player_id', 'iCF', 'iFF', 'iSF')] = \
        metrics_df.loc[:, ('player_id', 'iCF', 'iFF', 'iSF')].astype(int)

    return metrics_df

def calc_ind_shot_metrics(pbp_df):
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

    corsi_df = pbp_df[pbp_df.event.isin(corsi)]\
              .groupby(['season', 'game_id', 'date',
                        'p1_id', 'p1_name'])['is_corsi'].sum().reset_index()

    fenwick_df = pbp_df[pbp_df.event.isin(fenwick)]\
                 .groupby(['season', 'game_id', 'date',
                           'p1_id', 'p1_name'])['is_fenwick'].sum().reset_index()

    shot_df = pbp_df[pbp_df.event.isin(shot)]\
                 .groupby(['season', 'game_id', 'date',
                           'p1_id', 'p1_name'])['is_shot'].sum().reset_index()

    xg_df = pbp_df[pbp_df.event.isin(fenwick)]\
                 .groupby(['season', 'game_id', 'date',
                           'p1_id', 'p1_name'])['xg'].sum().reset_index()

    corsi_df.columns = ['season', 'game_id', 'date',  'player_id',
                        'player_name', 'iCF']

    fenwick_df.columns = ['season', 'game_id', 'date',
                          'player_id', 'player_name', 'iFF']

    shot_df.columns = ['season', 'game_id', 'date',
                       'player_id', 'player_name', 'iSF']

    xg_df.columns = ['season', 'game_id', 'date',
                     'player_id', 'player_name', 'ixg']

    metrics_df = corsi_df.merge(fenwick_df,
                                on=['season', 'game_id', 'date',
                                    'player_id', 'player_name'],
                                how='outer')

    metrics_df = metrics_df.merge(shot_df,
                                  on=['season', 'game_id', 'date',
                                      'player_id', 'player_name'],
                                  how='outer')

    metrics_df = metrics_df.merge(xg_df,
                                  on=['season', 'game_id', 'date',
                                      'player_id', 'player_name'],
                                  how='outer')

    metrics_df = metrics_df.fillna(0)

    metrics_df.loc[:, ('player_id', 'iCF', 'iFF', 'iSF')] = \
        metrics_df.loc[:, ('player_id', 'iCF', 'iFF', 'iSF')].astype(int)

    return metrics_df


def calc_ind_gata(pbp_df):
    '''
    function calculates giveaways and takeaways from the pbp_df.

    Input:
    pbp_df - play by play dataframe

    Output:
    hit_df - dataframe of each players GA/TA stats
    '''
    ga_df = pbp_df[pbp_df.event == 'GIVE'].groupby(['season', 'game_id', 'date', 'p1_id', 'p1_name']).size().reset_index()

    ta_df = pbp_df[pbp_df.event == 'TAKE'].groupby(['season', 'game_id', 'date', 'p1_id', 'p1_name']).size().reset_index()


    ga_df.columns = ['season', 'game_id', 'date', 'player_id', 'player_name', 'iGA']
    ta_df.columns = ['season', 'game_id', 'date', 'player_id', 'player_name', 'iTA']
    gata_df = ga_df.merge(ta_df, on=['season', 'game_id', 'date', 'player_id', 'player_name'], how='outer')

    gata_df = gata_df.fillna(0)

    return gata_df

def calc_ind_hits(pbp_df):
    '''
    function calculates hits for and against from the pbp_df.

    Input:
    pbp_df - play by play dataframe

    Output:
    hit_df - dataframe of each players hits stats
    '''


    hit_for = pbp_df[pbp_df.event == 'HIT'].\
                 groupby(['season', 'game_id', 'date',
                          'p1_id', 'p1_name'])['is_hit'].sum().\
                 reset_index()

    hit_for.columns = ['season', 'game_id', 'date',
                       'player_id', 'player_name', 'iHF']

    hit_against = pbp_df[pbp_df.event == 'HIT'].\
                 groupby(['season', 'game_id', 'date',
                          'p2_id', 'p2_name'])['is_hit'].sum().\
                 reset_index()

    hit_against.columns = ['season', 'game_id', 'date',
                           'player_id', 'player_name', 'iHA']

    hit_df = hit_for.merge(hit_against, on=['season', 'game_id', 'date',
                                            'player_id', 'player_name'],
                           how='outer')

    hit_df = hit_df.fillna(0)

    hit_df = hit_df.groupby(['season', 'game_id', 'date',
                             'player_id', 'player_name'])['iHF', 'iHA'].sum().reset_index()

    hit_df.loc[:, ('iHF', 'iHA')] = hit_df.loc[:, ('iHF', 'iHA')].astype(int)

    return hit_df

def calc_ind_penalties(pbp_df):
    '''
    function calculates penalties drawn and taken from the pbp_df. It
    excludes fighting and miscounduct penalties

    Input:
    pbp_df - play by play dataframe

    Output:
    penalty_df - dataframe of each players penalties taken and drawn
    '''


    penalty_taken = pbp_df[(pbp_df.event == 'PENL') & (pbp_df.is_penalty > 0)].\
                 groupby(['season', 'game_id', 'date',
                          'p1_id', 'p1_name'])['is_penalty'].sum().\
                 reset_index()

    penalty_taken.columns = ['season', 'game_id', 'date',
                             'player_id', 'player_name', 'iPENT']

    penalty_drawn = pbp_df[(pbp_df.event == 'PENL') & (pbp_df.is_penalty > 0)].\
                 groupby(['season', 'game_id', 'date',
                          'p2_id', 'p2_name'])['is_penalty'].sum().\
                 reset_index()

    penalty_drawn.columns = ['season', 'game_id', 'date',
                             'player_id', 'player_name', 'iPEND']

    penalty_df = penalty_taken.merge(penalty_drawn,
                                     on=['season', 'game_id', 'date',
                                         'player_id', 'player_name'],
                                     how='outer')

    penalty_df = penalty_df.fillna(0)

    penalty_df = penalty_df.groupby(['season', 'game_id', 'date',
                                     'player_id', 'player_name'])['iPENT', 'iPEND'].sum().reset_index()

    penalty_df.loc[:, ('iPENT', 'iPEND')] = penalty_df.loc[:, ('iPENT', 'iPEND')].astype(int)

    return penalty_df

def calc_faceoffs(pbp_df):
    '''
    function calculates faceoffs won and lost

    Input:
    pbp_df - play by play dataframe

    Output:
    penalty_df - dataframe of each players faceoff stats
    '''


    fo_won = pbp_df[pbp_df.event == 'FAC'].\
                 groupby(['season', 'game_id', 'date',
                          'p1_id', 'p1_name']).size().\
                 reset_index()

    fo_won.columns = ['season', 'game_id', 'date',
                      'player_id', 'player_name', 'FOW']

    fo_lost = pbp_df[pbp_df.event == 'FAC'].\
                 groupby(['season', 'game_id', 'date',
                          'p2_id', 'p2_name']).size().\
                 reset_index()

    fo_lost.columns = ['season', 'game_id', 'date',
                             'player_id', 'player_name', 'FOL']

    fo_df = fo_won.merge(fo_lost, on=['season', 'game_id', 'date',
                                      'player_id', 'player_name'],
                         how='outer')

    fo_df = fo_df.fillna(0)

    fo_df = fo_df.groupby(['season', 'game_id', 'date',
                                     'player_id', 'player_name'])['FOW', 'FOL'].sum().reset_index()

    fo_df.loc[:, ('FOW', 'FOL')] = fo_df.loc[:, ('FOW', 'FOL')].astype(int)

    return fo_df

def calc_blocks(pbp_df):

    blk_df = pbp_df[pbp_df.event == 'BLOCK'].\
                 groupby(['season', 'game_id', 'date',
                          'p2_id', 'p2_name']).size().\
                 reset_index()

    blk_df.columns = ['season', 'game_id', 'date',
                      'player_id', 'player_name', 'BLK']
    return blk_df

def calc_adj_ind_metrics(pbp_df, calc_blk=calc_blocks, \
                         calc_fo=calc_faceoffs,
                         calc_points=calc_ind_points,
                         calc_penalties=calc_ind_penalties,
                         calc_hits=calc_ind_hits,
                         calc_shot_metrics=calc_adj_ind_shot_metrics,
                         calc_gata=calc_ind_gata):
    '''
    this function calculates the individual metrics of each players
    contribution during the game

    Input:
    pbp_df - play by play df

    Output:
    player_df - individual player stats df
    '''

#calculate each individual stats data frames and then join them all together
#will pull in teams with the on ice measures
    points_df = calc_points(pbp_df)
    metrics_df = calc_shot_metrics(pbp_df)
    penalty_df = calc_penalties(pbp_df)
    hit_df = calc_hits(pbp_df)
    gata_df = calc_gata(pbp_df)
    fo_df = calc_fo(pbp_df)
    blk_df = calc_blk(pbp_df)

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

def calc_ind_metrics(pbp_df, calc_blk=calc_blocks, \
                     calc_fo=calc_faceoffs,
                     calc_points=calc_ind_points,
                     calc_penalties=calc_ind_penalties,
                     calc_hits=calc_ind_hits,
                     calc_shot_metrics=calc_ind_shot_metrics,
                     calc_gata=calc_ind_gata):
    '''
    this function calculates the individual metrics of each players
    contribution during the game

    Input:
    pbp_df - play by play df

    Output:
    player_df - individual player stats df
    '''

#calculate each individual stats data frames and then join them all together
#will pull in teams with the on ice measures
    points_df = calc_points(pbp_df)
    metrics_df = calc_shot_metrics(pbp_df)
    penalty_df = calc_penalties(pbp_df)
    hit_df = calc_hits(pbp_df)
    gata_df = calc_gata(pbp_df)
    fo_df = calc_fo(pbp_df)
    blk_df = calc_blk(pbp_df)

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
