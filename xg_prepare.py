import pandas as pd
import numpy as np
import pickle

def fixed_seconds_elapsed(pbp_df):
    '''
    This function fixes the seconds elapsed column to tally the total seconds
    elapsed for the whole game instead of just seconds elapsed for the period

    Inputs:
    pbp_df - pbp_df without seconds_elapsed fixed

    Outputs:
    pbp_df - pbp_df with seconds_elapsed correctly calculated
    '''

    pbp_df.loc[:, 'seconds_elapsed'] = pbp_df.loc[:, 'seconds_elapsed'] + (1200 * (pbp_df.period -1))

    return pbp_df

def switch_block_shots(pbp_df):
    '''
    This function switches the p1 and p2 of blocked shots because Harry's
    scraper lists p1 as the blocker instead of the shooter

    Inputs:
    pbp_df - dataframe of play by play to be cleaned

    Outputs:
    pbp_df - cleaned dataframe
    '''

#creating new columns where I switch the players around for blocked shots
    pbp_df.loc[:, ('new_p1_name')] = np.where(pbp_df.event == 'BLOCK',
                                              pbp_df.p2_name, pbp_df.p1_name)
    pbp_df.loc[:, ('new_p2_name')] = np.where(pbp_df.event == 'BLOCK',
                                              pbp_df.p1_name, pbp_df.p2_name)
    pbp_df.loc[:, ('new_p1_id')] = np.where(pbp_df.event == 'BLOCK',
                                            pbp_df.p2_id, pbp_df.p1_id)
    pbp_df.loc[:, ('new_p2_id')] = np.where(pbp_df.event == 'BLOCK',
                                            pbp_df.p1_id, pbp_df.p2_id)

#saving the new columns as the old ones
    pbp_df.loc[:, ('p1_name')] = pbp_df['new_p1_name']
    pbp_df.loc[:, ('p2_name')] = pbp_df['new_p2_name']
    pbp_df.loc[:, ('p1_id')] = pbp_df['new_p1_id']
    pbp_df.loc[:, ('p2_id')] = pbp_df['new_p2_id']

#drop unused new columns
    pbp_df = pbp_df.drop(['new_p1_name', 'new_p2_name',
                          'new_p1_id', 'new_p2_id'], axis=1)

    return pbp_df

def calc_distance(pbp_df):
    '''
    This function calculates the distance from the coordinate given for the
    event to the center of the goal

    Input:
    pbp_df - play by play dataframe

    Output:
    pbp_df - play by play dataframe with distance calculated
    '''
    pbp_df.loc[:, ('distance')] = np.sqrt((87.95-abs(pbp_df.xc))**2
                                          + pbp_df.yc**2)

    return pbp_df

def calc_angle(pbp_df):
    '''
    This function calculates the angle of the shooter from center ice with the
    vertex of the angle being located at the center of the goal

    Input:
    pbp_df - play by play dataframe

    Output:
    pbp_df - play by play dataframe with shooter angle calculated
    '''

    pbp_df.loc[:, ('angle')] = (np.arcsin(abs(pbp_df.yc)/np.sqrt((87.95-abs(pbp_df.xc))**2 + pbp_df.yc**2)) * 180) / 3.14

    pbp_df.loc[:, ('angle')] = np.where((pbp_df.xc > 88) | (pbp_df.xc < -88), 90 + (180-(90 + pbp_df.angle)), pbp_df.angle)

    return pbp_df

def calc_time_diff(pbp_df):
    '''
    This function calculates the time difference between events

    Input:
    pbp_df - play by play dataframe

    Output:
    pbp_df - play by play dataframe with time difference calculated
    '''

    pbp_df.loc[:, ('time_diff')] = pbp_df.seconds_elapsed - pbp_df.seconds_elapsed.shift(1)

    return pbp_df

def calc_event_length(pbp_df):
    '''
    This function calculates the time difference between events

    Input:
    pbp_df - play by play dataframe

    Output:
    pbp_df - play by play dataframe with time difference calculated
    '''

    pbp_df.loc[:, ('event_length')] = pbp_df.seconds_elapsed.shift(-1) - pbp_df.seconds_elapsed

    return pbp_df

def calc_rebound(pbp_df):
    '''
    This function calculates whether the corsi event was generated off of a
    goalie rebound by looking at the time difference between the current event
    and the last event and checking that last even was a shot as well

    Input:
    pbp_df - play by play dataframe

    Output:
    pbp_df - play by play dataframe with rebound calculated
    '''

    pbp_df.loc[:, ('is_rebound')] = np.where((pbp_df.time_diff < 4) &
                                             ((pbp_df.event.isin(['SHOT', 'GOAL'])) &
                                              (pbp_df.event.shift(1) == 'SHOT') &
                                              (pbp_df.ev_team == pbp_df.ev_team.shift(1))),
                                             1, 0)

    return pbp_df

def calc_rush_shot(pbp_df):
    '''
    This function calculates whether the corsi event was generated off the rush
    by looking at the time difference between the last even and whether the last
    event occured in the neutral zone

    Input:
    pbp_df - play by play dataframe

    Output:
    pbp_df - play by play dataframe with is_rush calculated
    '''

    pbp_df.loc[:, ('is_rush')] = np.where((pbp_df.time_diff < 4) &
                                          (pbp_df.event.isin(['SHOT', 'MISS', 'BLOCK', 'GOAL'])) &
                                          (abs(pbp_df.xc.shift(1)) < 26),
                                          1, 0)

    return pbp_df

def calc_prior_coords(pbp_df):
    '''
    this function calculates the coordiantes of the preceeding event and then
    the distance from the
    '''

    pbp_df['prior_x_coords'] = pbp_df['xc'].shift(1)
    pbp_df['prior_y_coords'] = pbp_df['yc'].shift(1)

    pbp_df['prior_x_coords'] = pbp_df['prior_x_coords'].fillna(0)
    pbp_df['prior_y_coords'] = pbp_df['prior_y_coords'].fillna(0)

    return pbp_df

def calc_prior_distance(pbp_df):
    '''
    this function calculates the distance from the current event to the
    event prior
    '''
    pbp_df['dist_to_prior'] = np.sqrt(((pbp_df['xc'] - pbp_df['prior_x_coords'])**2 + (pbp_df['yc'] - pbp_df['prior_y_coords'])**2))

    return pbp_df

def calc_shooter_strength(pbp_df):
    '''
    Function calculates the team strength of the shooter such as 5v5, 5v4,
    etc. This is done by subtracting the home and away skaters based on who is
    shooting the puck.

    Input:
    pbp_df - play by play dataframe

    Output:
    pbp_df - play by play dataframe with shooter strength calculated
    '''

#calculates shooter strength based on who's shooting
    pbp_df.loc[:, ('shooter_strength')] = \
            np.where((pbp_df.ev_team == pbp_df.home_team),
                     pbp_df.home_players - pbp_df.away_players,
                     pbp_df.away_players - pbp_df.home_players)

#handle empty net situations this time for the home team
    pbp_df.loc[:, ('shooter_strength')] = \
            np.where((pbp_df.ev_team == pbp_df.home_team) &
                     (pbp_df.home_goalie.isnull()),
                     pbp_df['shooter_strength'] + 1,
                     pbp_df['shooter_strength'])

#away team empty net situations
    pbp_df.loc[:, ('shooter_strength')] = \
            np.where((pbp_df.ev_team == pbp_df.away_team) &
                     (pbp_df.away_goalie.isnull()),
                     pbp_df['shooter_strength'] + 1,
                     pbp_df['shooter_strength'])

    return pbp_df

def calc_rebound_angle(pbp_df):
    '''
    Function calculates the angle between two shots if the second shot
    is flagged as a rebound else the value is zero

    Input:
    pbp_df - play by play dataframe

    Output:
    pbp_df - play by play dataframe
    '''
    pbp_df.loc[:, ('rebound_angle')] = \
            np.where(pbp_df.is_rebound == 1,
                     pbp_df.angle + pbp_df.angle.shift(1), 0)

    return pbp_df

def calc_shot_metrics(pbp_df):
    '''
    function to calculate whether an event is a corsi or fenwick event

    Inputs:
    pbp_df - play by play dataframe

    Outputs:
    pbp_df - play by play dataframe with corsi and fenwick columns calculated
    '''

    corsi = ['SHOT', 'BLOCK', 'MISS', 'GOAL']
    fenwick = ['SHOT', 'MISS', 'GOAL']
    shot = ['SHOT', 'GOAL']

    pbp_df.loc[:, ('is_corsi')] = np.where(pbp_df.event.isin(corsi), 1, 0)
    pbp_df.loc[:, ('is_fenwick')] = np.where(pbp_df.event.isin(fenwick), 1, 0)
    pbp_df.loc[:, ('is_shot')] = np.where(pbp_df.event.isin(shot), 1, 0)
    pbp_df.loc[:, ('is_goal')] = np.where(pbp_df.event == 'GOAL', 1, 0)

    return pbp_df

def calc_is_home(pbp_df):
    '''
    Function determines whether event was taken by the home team or not

    Inputs:
    pbp_df - play by play dataframe

    Outputs:
    pbp_df - play by play dataframe
    '''

    pbp_df.loc[:, ('is_home')] = np.where(pbp_df.ev_team == pbp_df.home_team,
                                          1, 0)

    return pbp_df

def calc_score_diff(pbp_df):
    '''
    Function to calculate score differential for score adjustment caps at
    +/- 3 due to Micah Blake McCurdy's (@Ineffectivemath on Twitter) adjustment
    method.

    Input:
    pbp_df - play by play df

    Output:
    pbp_df - play by play df with score diff calculated
    '''

    pbp_df.loc[:, ('score_diff')] = pbp_df.home_score - pbp_df.away_score

    pbp_df.loc[:, ('score_diff')] = np.where(pbp_df.score_diff < -3, -3,
                                             pbp_df.score_diff)

    pbp_df.loc[:, ('score_diff')] = np.where(pbp_df.score_diff > 3, 3,
                                             pbp_df.score_diff)

    return pbp_df

def calc_is_hit(pbp_df):
    '''
    determines whether event is a hit or not

    Input:
    pbp_df - play by play df

    Output:
    pbp_df - play by play df with is_hit column calculated
    '''
    pbp_df['is_hit'] = np.where(pbp_df.event == 'HIT', 1, 0)

    return pbp_df

def calc_is_penalty(pbp_df):
    '''
    calculates whether an event is a penalty

    Input:
    pbp_df - play by play df

    Output:
    pbp_df - play by play df with is_penalty column created
    '''

    pbp_df.loc[:, 'is_penalty'] = np.where((pbp_df.event == 'PENL'), 1, 0)
    pbp_df.loc[:, 'is_penalty'] = np.where((pbp_df.event == 'PENL') &
                                    (pbp_df.description.str.contains('fighting', regex=False, case=False)), 0, pbp_df.is_penalty)
    pbp_df.loc[:, 'is_penalty'] = np.where((pbp_df.event == 'PENL') &
                                    (pbp_df.description.str.contains('maj', regex=False, case=False)), 0, pbp_df.is_penalty)
    pbp_df.loc[:, 'is_penalty'] = np.where((pbp_df.event == 'PENL') &
                                    (pbp_df.description.str.contains('misconduct', regex=False, case=False)), 0, pbp_df.is_penalty)
    pbp_df.loc[:, 'is_penalty'] = np.where((pbp_df.event == 'PENL') &
                                    (pbp_df.description.str.\
                                           contains('double minor',
                                                    case=False)),
                                    2, pbp_df.is_penalty)

    return pbp_df

def calc_is_goal(pbp_df):
    '''
    determines whether an event is a goal or not
    '''
    pbp_df['is_goal'] = np.where(pbp_df.event == 'GOAL', 1, 0)

    return pbp_df

def calc_season(pbp_df):
    '''
    this function calculates the season from the date of the game

    Inputs:
    pbp_df - pbp_df without a season column

    Outputs - pbp_df with the season of the game calculated
    '''


    pbp_df.loc[:, ('season')] = np.where(pbp_df.date.dt.month.isin([10, 11, 12]),
                                        pbp_df.date.dt.year + 1, pbp_df.date.dt.year)
    return pbp_df

def fix_game_id(pbp_df):
    '''
    this fixes the game_id by putting it in the format that the NHL uses

    Input:
    pbp_df - pbp_df with out game_id fixed

    Output:
    pbp_df - pbp_df with game_id fixed
    '''

    new_season = pbp_df['season'] - 1
    pbp_df.loc[:, ('game_id')] =  new_season.map(str)+ '0' + pbp_df['game_id'].map(str)
    pbp_df.loc[:, ('game_id')] = pbp_df.loc[:, ('game_id')].astype(float).astype(int)

    return pbp_df

def calc_xg(pbp_df):
    '''
    this function calculates the xg value of each shot, miss or block and then
    joins it back to the main pbp_df

    Inputs:
    pbp_df - play by play data frame without xg calculations made

    Outputs:
    pbp_df_xg - play by play data frame with xg probabilities calculated
    '''
    with open('gbm_model', 'rb') as model:
        gbm_model = pickle.load(model)

    feature_columns = ['seconds_elapsed', 'xc', 'yc', 'time_diff', 'score_diff',
                       'prior_x_coords', 'prior_y_coords', 'dist_to_prior',
                       'distance', 'angle', 'is_rebound', 'rebound_angle',
                       'is_rush', 'shooter_strength', 'type_BACKHAND',
                       'type_DEFLECTED', 'type_SLAP SHOT', 'type_SNAP SHOT',
                       'type_TIP-IN', 'type_WRIST SHOT']

    fenwick_pbp = pd.get_dummies(pbp_df[pbp_df.event.isin(['SHOT', 'GOAL', 'MISS'])],
                                     columns=['type'])

    for column in feature_columns:
        if column not in fenwick_pbp.columns:
            fenwick_pbp[column] = 0

    fenwick_pbp_nona = fenwick_pbp[~fenwick_pbp[feature_columns].isnull().any(axis=1)]

    fenwick_pbp_nona['xg'] = gbm_model.predict_proba(fenwick_pbp_nona[feature_columns])[:, 1]

    fenwick_pbp = fenwick_pbp.merge(fenwick_pbp_nona[['xg']], how = 'outer', left_index=True, right_index=True)

    fenwick_pbp['xg'] = fenwick_pbp['xg'].fillna(fenwick_pbp['xg'].mean())

    pbp_df_xg = pbp_df.merge(fenwick_pbp[['xg']], how='outer',left_index=True, right_index=True)

    pbp_df_xg['xg'] = pbp_df_xg['xg'].fillna(0)

    return pbp_df_xg



def create_stat_features(pbp_df):
    '''
    this function cleans the pbp_df and casts columns as the proper variable
    type and calculates all the neccesary columns needed to calculate xG, on
    ice stats, and individual stats

    Input:
    pbp_df - uncleaned pbp_df

    Output:
    pbp_df - pbp dataframe cleaned and ready for further processing
    '''

    pbp_df.loc[:, ('date')] = pbp_df.date.astype('datetime64[ns]')
    pbp_df = switch_block_shots(pbp_df)
    pbp_df = calc_time_diff(pbp_df)
    pbp_df = calc_event_length(pbp_df)
    pbp_df = calc_shot_metrics(pbp_df)
    pbp_df = calc_season(pbp_df)
    pbp_df = fix_game_id(pbp_df)
    pbp_df = calc_score_diff(pbp_df)
    pbp_df = calc_is_home(pbp_df)
    pbp_df = calc_is_penalty(pbp_df)
    pbp_df = calc_is_hit(pbp_df)
    pbp_df = calc_prior_coords(pbp_df)
    pbp_df = calc_prior_distance(pbp_df)
    pbp_df = calc_is_goal(pbp_df)
    pbp_df = calc_distance(pbp_df)
    pbp_df = calc_angle(pbp_df)
    pbp_df = calc_rebound(pbp_df)
    pbp_df = calc_rebound_angle(pbp_df)
    pbp_df = calc_rush_shot(pbp_df)
    pbp_df = calc_shooter_strength(pbp_df)
    pbp_df = calc_xg(pbp_df)

    return pbp_df


def main():

    return

if __name__ == '__main__':
    main()
