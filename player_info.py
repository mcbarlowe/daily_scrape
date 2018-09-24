"""
Only for shift files that do not have player positions yet!!!

All runs through - fill_shifts_with_positions
"""

import os
import pandas as pd
from sqlalchemy import create_engine
#from db_info import *


def get_player(row):
    """
    Subset of id and position
    """
    return [row['id'], row['position']]


def get_position(player_id, players):
    try:
        return players[str(player_id)]
    except KeyError:
        print("Player id " + str(player_id) + " not found")
        return ''


# TODO: This functions shouldn't be in this file
def check_missing_shifts(shifts_df):
    """
    Checks for missing shifts for Goalies (others can have but those would be a lot more work to find)
    Adds them in if they have any

    :param shifts_df: Shifts DataFrame

    :return: Shifts DataFrame with some (or none) extra shifts included
    """
    games_list = list(set(shifts_df['game_id'].tolist()))

    for game in games_list:
        game_df = shifts_df[shifts_df['game_id'] == game]
        game_df = game_df.reset_index(drop=True)

        # Get last period in game and time (really only matters for non-regulation games)
        if len(set(game_df['period'].tolist())) > 3:
            last_period = game_df['period'][game_df.shape[0] - 1]
            last_time = list(range(0, int(game_df['end'][game_df.shape[0] - 1]) + 1))
            game_end = {'period': last_period, 'time': last_time}
        else:
            game_end = {'period': 3, 'time': 1200}

        for team in list(set(game_df['team'].tolist())):
            goalie_df = game_df[(game_df.team == team) & (game_df.position == 'G')]  # Subset goalies from team
            goalie_periods = list(set(goalie_df['period'].tolist()))

            # Get periods missing from shifts for goalies
            missing_periods = list(set(range(1, game_end['period']+1))-set(goalie_periods))

            # Just use the first goalie in the game...if it's not him I don't care. I don't give a fuck at this point
            goalie = dict(goalie_df.iloc[0])

            # Add Goalie Data in for missing periods
            for period in missing_periods:
                missing = goalie
                missing['period'] = period
                missing['start'] = 0
                missing['end'] = 1200
                missing['duration'] = 1200

                shifts_df = shifts_df.append(missing, ignore_index=True)

    return shifts_df


def fill_shifts_with_positions(shifts_df):
    """
    Given a DataFrame filled with the shift info for a collection of games it fills in the positions for each player

    :param shifts_df: DataFrame of Shifts

    :return: Same DataFrame with positions
    """

    shifts_df.columns = map(str.lower, shifts_df.columns)

    # Get Players table from db
    engine = create_engine(os.environ.get('DEV_DB_CONNECT'))
    players_df = pd.read_sql_table('nhl_players', engine, schema='nhl_tables')

    # Get list of all players and positions
    players_series = players_df.apply(lambda row: get_player(row), axis=1)
    players_set = set(tuple(x) for x in players_series.tolist())
    players_list = [list(x) for x in players_set]

    # Dict of players -> ID is key
    players = dict()
    for p in players_list:
        players[str(p[0])] = p[1]

    # Get rid of shifts that are fucked up
    shifts_df = shifts_df[~shifts_df['player_id'].isnull()]

    # Fill in positions
    shifts_df['position'] = shifts_df.apply(lambda row: get_position(int(row['player_id']), players), axis=1)

    # Sometimes goalies are missing shifts...add them in here
    shifts_df = check_missing_shifts(shifts_df)

    return shifts_df[['game_id', 'date', 'period', 'team', 'player', 'player_id', 'position', 'start', 'end', 'duration']]




