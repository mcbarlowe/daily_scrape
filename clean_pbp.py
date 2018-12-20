'''
This script will be used to clean up the resultant dataframe from merging the
shifts df and the pbp df such as filling NaN values and calculating proper
strength states and whether goalies where on the ice
'''
import pandas as pd
import numpy as np

def final_pbp_clean(pbp):
    '''
    this is the final cleaning step for the pbp before stats are calculated
    and the pbp is inserted into the sql database

    Input:
    pbp - pbp dataframe to be cleaned

    Output:
    pbp - play by play that has been cleaned and is ready for SQL insertion
    '''

    pbp[['date']] = pbp[['date']].astype('datetime64[ns]')

    pbp[['game_id', 'description', 'time_elapsed', 'strength',
                   'ev_zone', 'type', 'ev_team', 'home_zone', 'away_team',
                   'home_team', 'p1_name', 'p2_name', 'p3_name', 'awayplayer1',
                   'awayplayer2', 'awayplayer3', 'awayplayer4', 'awayplayer5',
                   'awayplayer6', 'homeplayer1', 'homeplayer2', 'homeplayer3',
                   'homeplayer4', 'homeplayer5', 'homeplayer6',
                   'home_coach', 'away_coach' ]] = \
    pbp[['game_id', 'description', 'time_elapsed', 'strength',
                   'ev_zone', 'type', 'ev_team', 'home_zone', 'away_team',
                   'home_team', 'p1_name', 'p2_name', 'p3_name', 'awayplayer1',
                   'awayplayer2', 'awayplayer3', 'awayplayer4', 'awayplayer5',
                   'awayplayer6', 'homeplayer1', 'homeplayer2', 'homeplayer3',
                   'homeplayer4', 'homeplayer5', 'homeplayer6',
                    'home_coach', 'away_coach' ]].astype(str)

    pbp[['adj_corsi', 'adj_fenwick', 'adj_xg']] = \
            pbp[['adj_corsi', 'adj_fenwick', 'adj_xg']].astype(float)

#filter out weird occurances where rows are all zeros sometimes probably
#comes from filling nas with zero and some rows are all nan
    pbp = pbp[pbp.date != 0]

    return pbp


def clean_pbp(new_pbp):
    '''
    this function cleans the new_pbp and gets it ready for xg and stat
    calculation
    '''
#fills na values in the coordinate with zeros
    new_pbp['xc'] = new_pbp['xc'].replace('', '0', regex=False)
    new_pbp['yc'] = new_pbp['yc'].replace('', '0', regex=False)
    new_pbp.loc[:, ('xc')] = new_pbp.loc[:, ('xc')].fillna(0).astype(int)
    new_pbp.loc[:, ('yc')] = new_pbp.loc[:, ('yc')].fillna(0).astype(int)
#fills na values with the names of the appropriate coaches
    new_pbp.loc[:, ('away_coach')] = new_pbp.loc[:, ('away_coach')].fillna(new_pbp.away_coach.unique()[0])
    new_pbp.loc[:, ('home_coach')] = new_pbp.loc[:, ('home_coach')].fillna(new_pbp.home_coach.unique()[0])
#fills na values with the names of the appropriate teams
    new_pbp.loc[:, ('away_team')] = new_pbp.loc[:, ('away_team')].fillna(new_pbp.away_team.unique()[0])
    new_pbp.loc[:, ('home_team')] = new_pbp.loc[:, ('home_team')].fillna(new_pbp.home_team.unique()[0])
#calculates new running scores to fill in the NaNs
    new_pbp.loc[:, ('away_score')] = np.where((new_pbp.event == 'GOAL') & (new_pbp.ev_team == new_pbp.away_team.unique()[0]), 1, 0).cumsum()
    new_pbp.loc[:, ('home_score')] = np.where((new_pbp.event == 'GOAL') & (new_pbp.ev_team == new_pbp.home_team.unique()[0]), 1, 0).cumsum()

    #clean home and away goalies
    new_pbp = new_pbp.apply(clean_goalie,
                            args=(new_pbp.away_goalie.unique(),
                                  new_pbp.away_goalie_id.unique(),
                                  new_pbp.home_goalie.unique(),
                                  new_pbp.home_goalie_id.unique()),
                            axis=1)

    #clean home and away skaters
    new_pbp = new_pbp.apply(clean_skaters, axis=1)
#cast columns to the appropirate values

#added fillna here to catch any na in seconds elapsed from a shift or something
#I don't know what
    new_pbp['seconds_elapsed'] = new_pbp['seconds_elapsed'].fillna(0).astype(int)
    new_pbp['game_id'] = new_pbp['game_id'].fillna(new_pbp.game_id.unique()[pd.notna(new_pbp.game_id.unique())][0])
    new_pbp['game_id'] = new_pbp['game_id'].astype(int)
#added in because some shifts are only 0 to 0 seconds and come in before the period start
#and cause a date NaN
    new_pbp['date'] = new_pbp['date'].fillna(new_pbp.date.unique()[pd.notna(new_pbp.date.unique())][0])
    new_pbp.loc[:, ('p1_id')] = new_pbp.loc[:, ('p1_id')].replace('', 0).fillna(0).astype(int)
    new_pbp.loc[:, ('p2_id')] = new_pbp.loc[:, ('p2_id')].replace('', 0).fillna(0).astype(int)
    new_pbp.loc[:, ('p3_id')] = new_pbp.loc[:, ('p3_id')].replace('', 0).fillna(0).astype(int)

    return new_pbp

def main():
    return

def clean_goalie(row, away_goalie, away_goalie_id, home_goalie, home_goalie_id):
    '''
    This checks to make sure the goalie for each team is on the ice and if so
    fills the NaN's from the shift/pbp merge with the goalie's name/id and if
    not leave it NaN

    Input:
    row - row of the new_pbp_df
    away_goalie    - list of away goalies in the game
    away_goalie_id - list of away goalie's player ids
    home_goalie    - same as away but for the home team
    home_goalie_id - see above

    Output:
    row - row with Goalie on ice calculated
    '''
#get rid of empty strings
    away_goalie_id[away_goalie_id == ''] = 0
    home_goalie_id[home_goalie_id == ''] = 0
    away_goalie_id[away_goalie_id == 'NA'] = 0
    home_goalie_id[home_goalie_id == 'NA'] = 0

#not sure why this is here but am leaving it here for now as I don't want
#to break anything
    if row['away_goalie_id'] == '':
        row['away_goalie_id'] = 0
    if row['home_goalie_id'] == '':
        row['home_goalie_id'] = 0

    away_goalie = away_goalie[~pd.isnull(away_goalie)]
    away_goalie_id = away_goalie_id[~pd.isnull(away_goalie_id)].astype(int)
    home_goalie = home_goalie[~pd.isnull(home_goalie)]
    home_goalie_id = home_goalie_id[~pd.isnull(home_goalie_id)].astype(int)

    away_goalie = [x for x in away_goalie if x != '']
    home_goalie = [x for x in home_goalie if x != '']

    for goalie, goalie_id in zip(away_goalie, away_goalie_id):
        if np.where(row[['awayplayer1', 'awayplayer2', 'awayplayer3', 'awayplayer4', 'awayplayer5', 'awayplayer6']].isin([goalie]), 1, 0).sum() > 0:
            row.loc[('away_goalie', 'away_goalie_id')] = goalie, int(goalie_id)

    for goalie, goalie_id in zip(home_goalie, home_goalie_id):
        if np.where(row[['homeplayer1', 'homeplayer2', 'homeplayer3', 'homeplayer4', 'homeplayer5', 'homeplayer6']].isin([goalie]), 1, 0).sum() > 0:
            row.loc[('home_goalie', 'home_goalie_id')] = goalie, int(goalie_id)

    return row

def clean_skaters(row):
    '''
    this function looks at the number of players that are on the ice and counts
    them to return the number of skaters for each team for each event in the
    pbp.

    Inputs:
    row - one row of the pbp dataframe

    Outputs:
    row - row with amount of skaters for each team calculated
    '''
    away_players = row[['awayplayer1', 'awayplayer2', 'awayplayer3',
                        'awayplayer4', 'awayplayer5', 'awayplayer6']]

    home_players = row[['homeplayer1', 'homeplayer2', 'homeplayer3',
                        'homeplayer4', 'homeplayer5', 'homeplayer6']]

    if row.event in ['OFF', 'ON']:
        row.away_players = len(away_players[away_players.nonzero()[0]])
        row.home_players = len(home_players[home_players.nonzero()[0]])

    return row

if __name__ == '__main__':
    main()
