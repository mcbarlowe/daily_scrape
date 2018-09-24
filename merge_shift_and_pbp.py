import pandas as pd
import numpy as np


def return_pbp_w_shifts(pbp_df, shifts_df):
    '''
    This funciton combines the other functions in this scripts to return a
    dataframe that contains all the pbp events along with the line changes as
    well

    Inputs:
    pbp_df - play by play dataframe
    shifts_df - dataframe of player shifts

    Outputs:
    pbp_w_shifts - play by play dataframe with the line changes incorporated
    '''

    pbp_df.columns = map(str.lower, pbp_df.columns)
    shifts_df.columns = map(str.lower, shifts_df.columns)
#creates the player dict of players on and off the ice for every second of the
#game
    player_matrix = player_onice_matrix(shifts_df)
    line_change_df = create_shifts_df(player_matrix, pbp_df.home_team.unique()[0],
                                     pbp_df.away_team.unique()[0])

    pbp_w_shifts_df = merge_pbp_and_shifts(line_change_df, pbp_df)

    return pbp_w_shifts_df

def merge_pbp_and_shifts(line_change_df, pbp_df):
    '''
    function to merge the shift changes and the pbp_df into one data frame
    like Emmanuel Perry's scraper

    Trash this function just calculate TOI from the shifts_df and calculate
    strength state there as well
    '''

    def label_priority(row):
        if row.event in ['TAKE', 'GIVE', 'MISS', 'HIT', 'SHOT', 'BLOCK']:
            return 1
        elif row.event == "GOAL":
            return 2
        elif row.event == "STOP":
            return 3
        elif row.event == "PENL":
            return 4
        elif row.event == "OFF":
            return 5
        elif row.event == 'ON':
            return 6
        elif row.event == 'FAC':
            return 7
        else:
            return 0

    def add_cols_to_shifts(line_change_df, pbp_df):
        '''
        This function adds in extra columns to help make the joins cleaner
        between the line change dataframe and the pbp dataframe

        Input:
        pbp_df - play by play dataframe
        line_change_df - data frame of line changes

        Output:
        line_change_df - dataframe with extra columns added
        '''
        line_change_df['home_team'] = pbp_df['home_team'].unique()[0]
        line_change_df['away_team'] = pbp_df['away_team'].unique()[0]

#change the on shifts with the same seconds elapsed as the period start/ends
#are moved to the next period so they can be sorted properly once the data
#frames are merged
        period_ends = []

        if int(pbp_df.game_id.unique()[0]) > 30000:
            for x in range(pbp_df.period.max()):
                period_ends.append((x+1) * 1200)
        else:
            period_ends = [1200, 2400, 3600]

        line_change_df['period'] = np.ceil(line_change_df.seconds_elapsed / 1200)

        for seconds in period_ends:
            line_change_df.loc[(line_change_df.seconds_elapsed == seconds) &
                                (line_change_df.event == 'ON'), 'period'] = \
                               line_change_df.loc[(line_change_df.seconds_elapsed == seconds)
                                       & (line_change_df.event == 'ON'), 'period'] + 1

        return line_change_df

    line_change_df = add_cols_to_shifts(line_change_df, pbp_df)

    line_change_df[['awayplayer1_id', 'awayplayer2_id', 'awayplayer3_id',
                                'awayplayer4_id', 'awayplayer5_id',
                                'awayplayer6_id', 'homeplayer1_id',
                                'homeplayer2_id','homeplayer3_id',
                                'homeplayer4_id','homeplayer5_id',
                                'homeplayer6_id']] = line_change_df[['awayplayer1_id', 'awayplayer2_id', 'awayplayer3_id',
                                'awayplayer4_id', 'awayplayer5_id',
                                'awayplayer6_id', 'homeplayer1_id',
                                'homeplayer2_id','homeplayer3_id',
                                'homeplayer4_id','homeplayer5_id',
                                'homeplayer6_id']].astype(float)

    pbp_df[['awayplayer1_id', 'awayplayer2_id', 'awayplayer3_id',
                                'awayplayer4_id', 'awayplayer5_id',
                                'awayplayer6_id', 'homeplayer1_id',
                                'homeplayer2_id','homeplayer3_id',
                                'homeplayer4_id','homeplayer5_id',
                                'homeplayer6_id']] = pbp_df[['awayplayer1_id', 'awayplayer2_id', 'awayplayer3_id',
                                'awayplayer4_id', 'awayplayer5_id',
                                'awayplayer6_id', 'homeplayer1_id',
                                'homeplayer2_id','homeplayer3_id',
                                'homeplayer4_id','homeplayer5_id',
                                'homeplayer6_id']].replace('', np.nan).astype(float)
#merge pbp_df and line_change_df into one dataframe
    pbp_w_shifts = pd.merge(pbp_df, line_change_df, how='outer',
                            on=['seconds_elapsed', 'event', 'awayplayer1_id', 'period',
                                'home_team', 'away_team',
                                'awayplayer1', 'awayplayer2_id', 'awayplayer2',
                                'awayplayer3', 'awayplayer3_id', 'awayplayer4',
                                'awayplayer4_id', 'awayplayer5', 'awayplayer5_id',
                                'awayplayer6', 'awayplayer6_id', 'homeplayer1_id',
                                'homeplayer1', 'homeplayer2_id', 'homeplayer2',
                                'homeplayer3', 'homeplayer3_id', 'homeplayer4',
                                'homeplayer4_id', 'homeplayer5', 'homeplayer5_id',
                                'homeplayer6', 'homeplayer6_id'])

#applying priority to events so that they will be properly ordered in the dataframe
    pbp_w_shifts['priority'] = pbp_w_shifts.apply(label_priority, axis=1)

#sorts the dataframe by elapsed seconds and then index as shift changes often
#take place at the same time by the play by play data
    pbp_w_shifts = pbp_w_shifts.sort_values(['seconds_elapsed','period', 'priority'],
                                            kind='mergesort',
                                            na_position='first').reset_index(drop=True)


#cleaning up and the NaNs with appropriate values
    pbp_w_shifts.game_id = pbp_w_shifts.game_id.fillna(pbp_w_shifts.game_id.values[0])
    pbp_w_shifts.date = pbp_w_shifts.date.fillna(pbp_w_shifts.date.values[0])

    return pbp_w_shifts

def create_shifts_df(player_matrix, home_team, away_team):
    '''
    function to transform on ice matrix into a shifts dataframe to join
    with the play by play dataframe

    Inputs:
    player_matrix - on ice dictionary
    home_team - home team for this game
    away_team - away team for this game

    Outputs:
    line_change_df - dataframe of just line changes
    '''
    line_change_list = []
    line_change_df_list = []

#loops through player onice matrix and parses the players on and off for each
#second where there is an actual line change i.e. players in the 'Off' part
#of the player_matrix
    for seconds in range(len(player_matrix)):
        if player_matrix[seconds][home_team]['Off'] == {} and player_matrix[seconds][away_team]['Off'] == {}:
            continue
        else:
#pulls out the player name and player id for each team and puts them in a list
#where each entry is either a tuple of player id and name if player was on the
#ice or a tuple of zeros if the team did not have the full compliment of players
#on the ice
            away_off = [(value, key) for key, value
                        in player_matrix[seconds][away_team]['On'].items()
                        if key in player_matrix[seconds-1][away_team]['On'].keys()
                        and player_matrix[seconds][away_team]['On'].keys()]

            away_on = [(value, key) for key, value
                       in player_matrix[seconds][away_team]['On'].items()]

            home_off = [(value, key) for key, value
                        in player_matrix[seconds][home_team]['On'].items()
                        if key in player_matrix[seconds-1][home_team]['On'].keys()
                        and player_matrix[seconds][home_team]['On'].keys()]

            home_on = [(value, key) for key, value
                       in player_matrix[seconds][home_team]['On'].items()]

#checks to see if there were total 6 players on the ice for each team and if not
#pads the lists with (0,0) tuple to form the dataframe
            if len(away_off) < 6:
                away_off.extend([(0,0)] * (6-len(away_off)))

            if len(home_off) < 6:
                home_off.extend([(0,0)] * (6-len(home_off)))

            if len(away_on) < 6:
                away_on.extend([(0,0)] * (6-len(away_on)))

            if len(home_on) < 6:
                home_on.extend([(0, 0)] * (6-len(home_on)))

#combines the home/away off and on to form one row of a shift change
            off_shift = away_off[:6] + home_off[:6]
            on_shift = away_on[:6] + home_on[:6]

#adding the event types showing whether players are coming on or leaving the ice
            off_shift.insert(0, 'OFF')
            on_shift.insert(0, 'ON')

#adding eplased_seconds to help facilitate the join
            off_shift.insert(0, seconds)
            on_shift.insert(0, seconds)

            line_change_list.append(off_shift)
            line_change_list.append(on_shift)

#pulling out the tuples to form a list of lists to create the on ice shift
#dataframe to join to the play by play
    for line in line_change_list:
        shift_line = []
        for x in range(len(line)):
            if type(line[x]) == tuple:
                shift_line.append(line[x][0])
                shift_line.append(line[x][1])
            else:
                shift_line.append(line[x])
        line_change_df_list.append(shift_line)

    columns = ['seconds_elapsed', 'event', 'awayplayer1', 'awayplayer1_id',
               'awayplayer2', 'awayplayer2_id','awayplayer3', 'awayplayer3_id',
               'awayplayer4', 'awayplayer4_id','awayplayer5', 'awayplayer5_id',
               'awayplayer6', 'awayplayer6_id',
               'homeplayer1', 'homeplayer1_id',
               'homeplayer2', 'homeplayer2_id','homeplayer3', 'homeplayer3_id',
               'homeplayer4', 'homeplayer4_id','homeplayer5', 'homeplayer5_id',
               'homeplayer6', 'homeplayer6_id']

    line_change_df = pd.DataFrame(line_change_df_list, columns=columns)

    return line_change_df

def player_onice_matrix(shift_df):
    '''
    This function creates a player on ice matrix showing which players were
    on the ice at the same time from the shift report

    Inputs:
    shift_df - one game shift dataframe with positions added

    Outputs:
    player_matrix - a list of lists breaking down players ice time
    '''

    shift_df.columns = map(str.lower, shift_df.columns)

    teams = list(shift_df.team.unique())
    game = shift_df.game_id.unique()[0]

    player_matrix = get_game_length(shift_df, game, teams)

    for row in range(shift_df.shape[0]):
        player_matrix = add_toi(shift_df.iloc[row, :], player_matrix)

    return player_matrix

def add_toi(row, onice_matrix):
    '''
    function that takes a shift from the shifts_df and applies it to the on
    ice matrix

    Inputs:
    shift_df - dataframe of the shifts of a game
    onice_matrix - empty dict where each index is a second in the game and lists
                   the players moving on and off the ice

    Outputs:
    onice_matrix - list filled with players moving on and off the ice
    '''

    start = row['start']
    end = row['end']
    team = row['team']

#this checks for OT shifts that have an end point of 0 or 1200 which is common
#for shifts at the end of OT which fucks everything up and changes them to the
#end value of OT itself
    if row['period'] == 4 and (end == 0.0 or end == 1200.0):
        end = 300

    if row['period'] != 1:
        start += (1200 * (int(row['period']) - 1))
        end += (1200 * (int(row['period']) - 1))

    for x in range(int(start), int(end) + 1):
        if x == end:
            shift_type = 'Off'
        else:
            shift_type = 'On'

        onice_matrix[x][team][shift_type][str(row.player_id)] = row.player

    return onice_matrix


def get_game_length(game_df, game, teams):
    """
    Gets a list with the length equal to the amount of seconds in that game.
    This code written by Harry Shomer.

    :param game_df: DataFrame with shift info for game
    :param game: game_id
    :param teams: both teams in game

    :return: list
    """
    # Start off with the standard 3 periods (1201 because start at 0)
    seconds = list(range(0, 1201)) * 3

    # If the last shift was an overtime shift, then extend the list of seconds by how fair into OT the game went
    if int(game_df['period'].iloc[game_df.shape[0] - 1]) == 4:
#again this checks for shifts that end in zero at the end of OT and replaces
#them with 300 and extends the matrix by that much because the NHL is stupid
#and doesn't assert values and allows TOI greater than 5 minutes or actually
#negative minutes
        if int(game_df['end'][game_df.shape[0] - 1]) == 0:
            seconds.extend(list(range(0, 301)))
        else:
            seconds.extend(list(range(0, game_df['end'][game_df.shape[0] - 1].astype(int) + 1)))

    # For Playoff Games
    # If the game went beyond 4 periods tack that on to
    if int(game) >= 30000:
        # Go from beyond period 4 because done above
        for i in range(game_df['period'].iloc[game_df.shape[0] - 1] - 4):
            seconds.extend(list(range(0, 1201)))

        seconds.extend(list(range(0, game_df['end'][game_df.shape[0] - 1] + 1)))

    # Create dict for seconds_list
    # On = Players on Ice at that second
    # Off = Players who got off ice at that second
    for i in range(len(seconds)):
        seconds[i] = {
            teams[0]: {'On': {}, 'Off': {}},
            teams[1]: {'On': {}, 'Off': {}},
        }

    return seconds
