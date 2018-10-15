import pandas as pd
import numpy as np

def calc_goalie_metrics(pbp_df, first_skaters, second_skaters):
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


    pbp_df['away_corsi'] = np.where((pbp_df['event'].isin(corsi)) &
                                    (pbp_df['is_home'] == 0), 1, 0)
    pbp_df['away_fenwick'] = np.where((pbp_df['event'].isin(fenwick)) &
                                    (pbp_df['is_home'] == 0 ), 1, 0)

    pbp_df['away_shot'] = np.where((pbp_df['event'].isin(shot)) &
                                    (pbp_df['is_home'] == 0), 1, 0)

    pbp_df['away_goal'] = np.where((pbp_df['is_goal'] == 1) &
                                    (pbp_df['is_home'] == 0), 1, 0)

    pbp_df['away_xg'] = np.where(pbp_df['is_home'] == 0, pbp_df['xg'], 0)

    home_str_df = pbp_df[(pbp_df.home_players.isin(first_skaters)) &
                         (pbp_df.away_players.isin(second_skaters)) &
                         (pbp_df.home_goalie.notnull())]

    away_str_df = pbp_df[(pbp_df.home_players.isin(second_skaters)) &
                         (pbp_df.away_players.isin(first_skaters)) &
                         (pbp_df.away_goalie.notnull())]

    home_goalie_stats = home_str_df.groupby(['season', 'game_id', 'date', 'home_team', 'home_goalie'])\
            ['event_length', 'away_corsi', 'away_fenwick',
             'away_shot','away_goal', 'away_xg'].sum().reset_index()

    away_goalie_stats = away_str_df.groupby(['season', 'game_id', 'date', 'away_team', 'away_goalie'])\
            ['event_length', 'home_corsi', 'home_fenwick',
             'home_shot','home_goal', 'home_xg'].sum().reset_index()


    home_goalie_stats.columns = ['season', 'game_id', 'date', 'team', 'goalie',
                                 'toi', 'ca', 'fa', 'sa', 'ga', 'xga']

    away_goalie_stats.columns = ['season', 'game_id', 'date', 'team', 'goalie',
                                 'toi', 'ca', 'fa', 'sa', 'ga', 'xga']

    home_goalie_stats['toi'] = home_goalie_stats['toi'] / 60
    away_goalie_stats['toi'] = away_goalie_stats['toi'] / 60

    #need to add in toi here
    goalie_stats = pd.concat([home_goalie_stats, away_goalie_stats])

    try:
        goalie_stats = goalie_stats[goalie_stats.goalie != '']
        return goalie_stats
    except TypeError as e:
        return goalie_stats


