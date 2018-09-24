'''
this script combines all the parsing of player, team, and goalie stats into
functions to clean up the code and not repeat things
'''

import pandas as pd
from calc_all_sits_ind_stats import calc_ind_metrics, calc_adj_ind_metrics
from calc_all_sits_onice_stats import calc_onice_stats, calc_adj_onice_stats
from calc_pppkes_ind_stats import calc_ppespk_ind_metrics, calc_adj_ppespk_ind_metrics
from calc_pppkes_onice_stats import calc_onice_str_stats, calc_adj_onice_str_stats

def get_player_dfs(new_pbp_df, first_skater, second_skater, columns):
    '''
    this function will calculate ind and onice stats for the passed strength
    state and return an adjusted and unadjusted dataframe for player stats

    Inputs:
    new_pbp - play by play dataframe with all xg features and line changes
              calculated
    first_skater - first skater number of strength state for these play by play
                   dataframes the goalie is included in this number so 5 is 6
                   and 4 is 5 etc.
    second_skater - the second number in the strength state
    columns - a list of column names to pass to create the empty dataframes for
              when an error happens

    Outputs:
    player_adj_df - player stats adjusted for home and venue
    player_df - player stats unadjusted for home and venue
    '''

    try:
        ind_stats = calc_ppespk_ind_metrics(new_pbp_df, first_skater, second_skater)
        onice_stats = calc_onice_str_stats(new_pbp_df, first_skater, second_skater)
        ind_stats_adj = calc_adj_ppespk_ind_metrics(new_pbp_df, first_skater, second_skater)
        onice_stats_adj = calc_adj_onice_str_stats(new_pbp_df, first_skater, second_skater)
    except ValueError:
        ind_stats = pd.DataFrame(columns=list(columns))
        onice_stats = pd.DataFrame(columns=list(columns))

        return ind_stats, onice_stats

    player_df = onice_stats.merge(ind_stats,
                                  on=['season', 'game_id', 'date',
                                      'player_id', 'player_name'],
                                  how='left')

    player_df = player_df.fillna(0)

    player_adj_df = onice_stats_adj.merge(ind_stats_adj,
                                          on=['season', 'game_id', 'date',
                                              'player_id', 'player_name'],
                                          how='left')

    player_adj_df = player_adj_df.fillna(0)

    return player_df, player_adj_df
