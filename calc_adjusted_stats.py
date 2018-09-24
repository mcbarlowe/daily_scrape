import pandas as pd
import numpy as np

score_venue_adj_dic = {-3: {'home_weight': .850, 'away_weight': 1.214},
                       -2: {'home_weight': .882, 'away_weight': 1.154},
                       -1: {'home_weight': .915, 'away_weight': 1.103},
                       0: {'home_weight': .970, 'away_weight': 1.032},
                       1: {'home_weight': 1.026, 'away_weight': .975},
                       2: {'home_weight': 1.074, 'away_weight': .936},
                       3: {'home_weight': 1.132, 'away_weight': .895}}

wght_shots_goals = {-3: {'home_weight': .943, 'away_weight': 1.057},
                    -2: {'home_weight': .976, 'away_weight': 1.024},
                    -1: {'home_weight': .936, 'away_weight': 1.064},
                    0: {'home_weight': .942, 'away_weight': 1.058},
                    1: {'home_weight': .995, 'away_weight': 1.005},
                    2: {'home_weight': 1.01, 'away_weight': .990},
                    3: {'home_weight': 1.017, 'away_weight': .983}}

wght_shots_shot = {-3: {'home_weight': .163, 'away_weight': .237},
                   -2: {'home_weight': .171, 'away_weight': .229},
                   -1: {'home_weight': .180, 'away_weight': .220},
                   0: {'home_weight': .196, 'away_weight': .204},
                   1: {'home_weight': .213, 'away_weight': .187},
                   2: {'home_weight': .221, 'away_weight': .179},
                   3: {'home_weight': .227, 'away_weight': .173}}

def calc_adjusted_columns(row, adj_matrix=score_venue_adj_dic):
    '''
    This function creates adjusted columns for corsi, fenwick, and
    xG

    Inputs:
    row - play by play dataframe row
    adj_matrix - dictionary of home team goal differences amounts to adjust
                 the events

    Outputs:
    row - play by play dataframe row with adjusted corsi, fenwick and xg columns
             calculated
    '''
    row['adj_corsi'] = np.where(row['is_home'] == 1,
                                   row['is_corsi'] * adj_matrix[row.score_diff]['home_weight'],
                                   row['is_corsi'] * adj_matrix[row.score_diff]['away_weight'])

    row['adj_fenwick'] = np.where(row['is_home'] == 1,
                                   row['is_fenwick'] * adj_matrix[row.score_diff]['home_weight'],
                                   row['is_fenwick'] * adj_matrix[row.score_diff]['away_weight'])

    row['adj_xg'] = np.where(row['is_home'] == 1,
                             row['xg'] * .9468472,
                             row['xg'] * 1.059477)

    row[['adj_corsi', 'adj_fenwick', 'adj_xg']] = row[['adj_corsi', 'adj_fenwick', 'adj_xg']].fillna(0)
    row[['adj_corsi', 'adj_fenwick', 'adj_xg']] = row[['adj_corsi', 'adj_fenwick', 'adj_xg']].astype(float)


    return row




