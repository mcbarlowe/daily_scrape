import pandas as pd
import numpy as np

def calc_team_metrics(pbp_df, first_skaters, second_skaters):
    '''
    This function calculates team metrics for all sits and returns a dataframe
    with the calulated stat dataframe

    Input:
    pbp_df - play by play dataframe
    first_skaters - an array containing the number of skaters that mathces the first
                    number of the strength state 5v5 is 5, 4v3 is 4, etc. For
                    all situations would pass 6, 5, 4 as the goalie is counted
                    in this scraper

    first_skaters - an array containing the number of skaters that mathces the first
                    number of the strength state 5v5 is 5, 4v3 is 4, etc. For
                    all situations would pass 6, 5, 4 as the goalie is counted
                    in this scraper

    Output:
    all_sits_team_df - data frame of calculated team stats
    '''

    corsi = ['SHOT', 'BLOCK', 'MISS', 'GOAL']
    fenwick = ['SHOT', 'MISS', 'GOAL']
    shot = ['SHOT', 'GOAL']

    pbp_df = pbp_df[pbp_df.period < 5]


    pbp_df['home_corsi'] = np.where((pbp_df['event'].isin(corsi)) &
                                    (pbp_df['is_home'] == 1), 1, 0)
    pbp_df['home_fenwick'] = np.where((pbp_df['event'].isin(fenwick)) &
                                    (pbp_df['is_home'] == 1), 1, 0)

    pbp_df['home_shot'] = np.where((pbp_df['event'].isin(shot)) &
                                    (pbp_df['is_home'] == 1), 1, 0)

    pbp_df['home_goal'] = np.where((pbp_df['is_goal'] == 1) &
                                    (pbp_df['is_home'] == 1), 1, 0)

    pbp_df['home_xg'] = np.where(pbp_df['is_home'] == 1, pbp_df['xg'], 0)

    pbp_df['home_pen'] = np.where((pbp_df['is_penalty'].isin([1,2])) &
                                (pbp_df['is_home'] == 1), pbp_df['is_penalty'], 0)

    pbp_df['home_blk'] = np.where((pbp_df['event'] == 'BLOCK') &
                                (pbp_df['is_home'] == 1), 1, 0)

    pbp_df['home_hits'] = np.where((pbp_df['event'] == 'HIT') &
                                   (pbp_df['is_home'] ==1), 1, 0)

    pbp_df['home_give'] = np.where((pbp_df['event'] == 'GIVE') &
                                   (pbp_df['is_home'] == 1), 1, 0)

    pbp_df['home_take'] = np.where((pbp_df['event'] == 'TAKE') &
                                   (pbp_df['is_home'] == 1), 1, 0)

    pbp_df['home_face'] = np.where((pbp_df['event'] == 'FAC') &
                                   (pbp_df['ev_team'] == pbp_df['home_team']),
                                   1,0)

    pbp_df['home_xg_adj'] = np.where(pbp_df['is_home'] == 1, pbp_df['adj_xg'], 0)

    pbp_df['home_corsi_adj'] = np.where((pbp_df['event'].isin(corsi)) &
                                    (pbp_df['is_home'] == 1), pbp_df['adj_corsi'], 0)
    pbp_df['home_fenwick_adj'] = np.where((pbp_df['event'].isin(fenwick)) &
                                    (pbp_df['is_home'] == 1), pbp_df['adj_fenwick'], 0)

    pbp_df['away_corsi'] = np.where((pbp_df['event'].isin(corsi)) &
                                    (pbp_df['is_home'] == 0), 1, 0)
    pbp_df['away_fenwick'] = np.where((pbp_df['event'].isin(fenwick)) &
                                    (pbp_df['is_home'] == 0 ), 1, 0)

    pbp_df['away_shot'] = np.where((pbp_df['event'].isin(shot)) &
                                    (pbp_df['is_home'] == 0), 1, 0)

    pbp_df['away_goal'] = np.where((pbp_df['is_goal'] == 1) &
                                    (pbp_df['is_home'] == 0), 1, 0)

    pbp_df['away_xg'] = np.where(pbp_df['is_home'] == 0, pbp_df['xg'], 0)

    pbp_df['away_pen'] = np.where((pbp_df['is_penalty'].isin([1,2])) &
                                (pbp_df['is_home'] == 0), pbp_df['is_penalty'], 0)

    pbp_df['away_blk'] = np.where((pbp_df['event'] == 'BLOCK') &
                                (pbp_df['is_home'] == 0), 1, 0)

    pbp_df['away_hits'] = np.where((pbp_df['event'] == 'HIT') &
                                   (pbp_df['is_home'] == 0), 1, 0)

    pbp_df['away_give'] = np.where((pbp_df['event'] == 'GIVE') &
                                   (pbp_df['is_home'] == 0), 1, 0)

    pbp_df['away_take'] = np.where((pbp_df['event'] == 'TAKE') &
                                   (pbp_df['is_home'] == 0), 1, 0)

    pbp_df['away_face'] = np.where((pbp_df['event'] == 'FAC') &
                                   (pbp_df['ev_team'] == pbp_df['away_team']),
                                   1,0)

    pbp_df['away_xg_adj'] = np.where(pbp_df['is_home'] == 0, pbp_df['adj_xg'], 0)

    pbp_df['away_corsi_adj'] = np.where((pbp_df['event'].isin(corsi)) &
                                    (pbp_df['is_home'] == 0), pbp_df['adj_corsi'], 0)
    pbp_df['away_fenwick_adj'] = np.where((pbp_df['event'].isin(fenwick)) &
                                    (pbp_df['is_home'] == 0), pbp_df['adj_fenwick'], 0)

    home_str_df = pbp_df[(pbp_df.home_players.isin(first_skaters)) &
                         (pbp_df.away_players.isin(second_skaters))
                         ]

    away_str_df = pbp_df[(pbp_df.home_players.isin(second_skaters)) &
                         (pbp_df.away_players.isin(first_skaters))
                         ]

    home_stats = home_str_df.groupby(['season', 'game_id', 'date', 'home_team'])\
            ['event_length', 'home_corsi', 'away_corsi', 'home_fenwick', 'away_fenwick',
             'home_shot', 'away_shot', 'home_goal', 'away_goal', 'home_xg',
             'away_xg', 'away_pen', 'home_pen', 'home_hits', 'away_hits',
             'home_blk', 'home_give', 'home_take', 'home_face', 'away_face',
             'home_xg_adj', 'away_xg_adj', 'home_corsi_adj', 'away_corsi_adj',
             'home_fenwick_adj', 'away_fenwick_adj'].sum().reset_index()

    away_stats = away_str_df.groupby(['season', 'game_id', 'date', 'away_team'])\
            ['event_length', 'away_corsi', 'home_corsi', 'away_fenwick', 'home_fenwick',
             'away_shot', 'home_shot', 'away_goal', 'home_goal', 'away_xg',
             'home_xg', 'home_pen', 'away_pen', 'away_hits', 'home_hits',
             'away_blk', 'away_give', 'away_take', 'away_face', 'home_face',
             'away_xg_adj', 'home_xg_adj', 'away_corsi_adj', 'home_corsi_adj',
             'away_fenwick_adj', 'home_fenwick_adj'].sum().reset_index()


    home_stats.columns = ['season', 'game_id', 'date', 'team','toi', 'cf',
                          'ca', 'ff', 'fa', 'sf', 'sa', 'gf', 'ga', 'xgf',
                          'xga', 'pend', 'pent', 'hf', 'ha', 'blk', 'give', 'take',
                          'fow', 'fol', 'xgf_adj', 'xga_adj', 'cf_adj', 'ca_adj',
                          'ff_adj', 'fa_adj']

    away_stats.columns = ['season', 'game_id', 'date', 'team', 'toi', 'cf',
                          'ca', 'ff', 'fa', 'sf', 'sa', 'gf', 'ga', 'xgf',
                          'xga', 'pend', 'pent', 'hf', 'ha', 'blk', 'give', 'take',
                          'fow', 'fol', 'xgf_adj', 'xga_adj', 'cf_adj', 'ca_adj',
                          'ff_adj', 'fa_adj']

    home_stats['toi'] = home_stats['toi'] / 60
    away_stats['toi'] = away_stats['toi'] / 60

    #need to add in toi here
    team_stats = pd.concat([home_stats, away_stats])

    return team_stats
