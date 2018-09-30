import os
import sys
import requests
import datetime
import logging
import hockey_scraper
import process_players
import xg_prepare as xg
import merge_shift_and_pbp as oi_matrix
import clean_pbp
import calc_adjusted_stats
import parse_players
from sqlalchemy import create_engine
from calc_all_sits_ind_stats import calc_ind_metrics, calc_adj_ind_metrics
from calc_all_sits_onice_stats import calc_onice_stats, calc_adj_onice_stats
from calc_goalie_stats import calc_goalie_metrics
from calc_team_stats import calc_team_metrics

def sched_insert(df, table_name):

    print('Inserting DataFrame to the Database')
    engine = create_engine(os.environ.get('DEV_DB_CONNECT'))
    df.to_sql(table_name, schema='nhl_tables', con=engine,
              if_exists='append', index=False)

#close connection to the database
    engine.dispose()

def get_yest_games(date):
    '''
    This function will return a list of game ids of NHL games played on the
    date given

    Input:
    date - the date on which the nhl games returned was played

    Output:
    game_ids - a list of NHL game ids played on the date given
    '''

    game_ids = []

    api_url = ('https://statsapi.web.nhl.com/api/v1/schedule?'
               'date={}').format(date)

    req = requests.get(api_url)
    schedule_dict = req.json()

#tests to see if any games were played and if not returns None to let the main
#script know that there where no games played and to stop running
    if schedule_dict['totalGames'] == 0:
        return None

    for date in schedule_dict['dates']:
        for game in date['games']:
            game_ids.append(game['gamePk'])


    return game_ids

def scrape_daily_games(game_id_list):
    '''
    this function scrapes the game ids provided in the list and returns a dict
    with all the pbp and shift data for each game with game_id serving as the
    key. It will also return a error_games list of the game where an error
    occured in the scraping

    Inputs:
    game_id_list - a list of game ids to scrape

    Outputs:
    game_dict - dictionary containing shift and pbp data for each game
    error_games - a list of game_ids where the scraper encountered errors
    '''

#setup games dictionary to store the pbp of the games scraped from the day
#before
    games_dict = {}

#create list to add games that errored out to write to file
    error_games = []

    for game in game_id_list:
        scraped_data = hockey_scraper.scrape_games([game],
                                                   True, data_format='Pandas')

#pull pbp, and shifts from the scraped data and write errors to the log
        games_dict[str(game)] = {}
        games_dict[str(game)]['pbp'] = scraped_data['pbp']
        games_dict[str(game)]['shifts'] = scraped_data['shifts']

#if an error occurs from scraping the game, log the errors and save the game id
#to rescrape latter. Delete the key from the dictionary so the data is not
#mistakenly added to the database
        if scraped_data['errors']:
            error_games.append(game)

    logging.info("All games scrapped")
    logging.debug(games_dict.keys())
    logging.info(f"Errors are {scraped_data['errors']}")

    return games_dict, error_games

def main():
    '''
    This script pulls the NHL games played from the day before parses the data
    to calculated individual, on-ice, and relative stats along with team and
    goalie stats and then will insert them into an PostgreSQL Database.

    This will use an NHL scraper built by Harry Shomer using the Hockey-Scraper
    package found at https://github.com/HarryShomer/Hockey-Scraper.
    '''


#setup logger to write out stuff to log file for debugging/warnings
#TODO changing logging level to info once script is ready
    logging.basicConfig(filename='daily_nhl_scraper.log',
                        format="%(asctime)s:%(levelname)s:%(message)s",
                        level=logging.INFO)

#getting yesterday's date and formatting it into the form that the NHL API
#accepts
    date = datetime.datetime.now() - datetime.timedelta(1)
    date = date.strftime('%Y-%m-%d')

#TODO remove test date once script is fully functional
    #test_date = "2018-01-09"
    game_ids = get_yest_games(date)

    #game_ids = [2017020001]
    if game_ids == None:
        logging.info("No games played today")
        return
    else:
        logging.info("Game Ids succesfully scraped")
        logging.info(f"{date} NHL games: {game_ids}")

    games_dict, error_games = scrape_daily_games(game_ids)

    for key, value in games_dict.items():

        print(key)
        try:
#pulling pbp and shifts data for each game out of the dictionary
            pbp_df = value['pbp']
            shifts_df = value['shifts']

#change all columns to lower case
            pbp_df.columns = map(str.lower, pbp_df.columns)
            shifts_df.columns = map(str.lower, shifts_df.columns)

#fixing the seconds elapsed column
            pbp_df = xg.fixed_seconds_elapsed(pbp_df)

#merging the shifts and pbp dataframes
            new_pbp_df = oi_matrix.return_pbp_w_shifts(pbp_df, shifts_df)

        #clean the pbp and fix block shots and calc columns to be used to calc
        #other stats
            new_pbp_df = clean_pbp.clean_pbp(new_pbp_df)

        #calc xg features and xg values for each fenwick envent
            new_pbp_df = xg.create_stat_features(new_pbp_df)

        #calc all adjusted stat columns for corsi, fenwick and xg
            new_pbp_df = new_pbp_df.apply(calc_adjusted_stats.calc_adjusted_columns,
                                          axis=1)

#insert pbp into the sql database
            sched_insert(new_pbp_df, 'master_pbp')

#insert new players into the player database
            process_players.process_players(shifts_df)

#calculating all situation stats
            print(f'Calculating {key} player stats')
            as_ind_stats = calc_ind_metrics(new_pbp_df)
            as_onice_stats = calc_onice_stats(new_pbp_df)
            as_adj_ind_stats = calc_adj_ind_metrics(new_pbp_df)
            as_adj_onice_stats = calc_adj_onice_stats(new_pbp_df)

            as_df = as_onice_stats.merge(as_ind_stats,
                                          on=['season', 'game_id', 'date',
                                              'player_id', 'player_name'],
                                          how='left')

            as_df = as_df.fillna(0)

            as_adj_df = as_adj_onice_stats.merge(as_adj_ind_stats,
                                                  on=['season', 'game_id', 'date',
                                                      'player_id', 'player_name'],
                                                  how='left')

            as_adj_df = as_adj_df.fillna(0)
#calculate all strength states for players
            stats_5v5, stats_5v5_adj = parse_players.get_player_dfs(new_pbp_df, 6, 6, list(as_df.columns))
            stats_4v4, stats_4v4_adj = parse_players.get_player_dfs(new_pbp_df, 5, 5, list(as_df.columns))
            stats_3v3, stats_3v3_adj = parse_players.get_player_dfs(new_pbp_df, 4, 4, list(as_df.columns))
            stats_5v4, stats_5v4_adj = parse_players.get_player_dfs(new_pbp_df, 6, 5, list(as_df.columns))
            stats_4v5, stats_4v5_adj = parse_players.get_player_dfs(new_pbp_df, 5, 6, list(as_df.columns))
            stats_5v3, stats_5v3_adj = parse_players.get_player_dfs(new_pbp_df, 6, 4, list(as_df.columns))
            stats_3v5, stats_3v5_adj = parse_players.get_player_dfs(new_pbp_df, 4, 6, list(as_df.columns))
            stats_4v3, stats_4v3_adj = parse_players.get_player_dfs(new_pbp_df, 5, 4, list(as_df.columns))
            stats_3v4, stats_3v4_adj = parse_players.get_player_dfs(new_pbp_df, 4, 5, list(as_df.columns))

            data = [stats_3v3, stats_3v3_adj, stats_3v4, stats_3v4_adj, stats_3v5,
                    stats_3v5_adj, stats_4v3, stats_4v3_adj, stats_4v4, stats_4v4_adj,
                    stats_4v5, stats_4v5_adj, stats_5v3, stats_5v3_adj, stats_5v4,
                    stats_5v4_adj, stats_5v5, stats_5v5_adj, as_df, as_adj_df]

            tables = ['player_3v3', 'player_3v3_adj', 'player_3v4', 'player_3v4_adj',
                      'player_3v5', 'player_3v5_adj', 'player_4v3', 'player_4v3_adj',
                      'player_4v4', 'player_4v4_adj', 'player_4v5', 'player_4v5_adj',
                      'player_5v3', 'player_5v3_adj', 'player_5v4', 'player_5v4_adj',
                      'player_5v5', 'player_5v5_adj', 'player_allsits',
                      'player_allsits_adj']

#insert player stats into the database
            for df, table in zip(data, tables):
                if df['toi'].sum() > 0:
                    df.columns = list(map(str.lower, df.columns))
                    sched_insert(df[df.toi > 0], table)
            logging.info(f'{key} player stats calculated and inserted')

        #calculating all team stats for all strengths adjusted/unadjusted

            team_allsits = calc_team_metrics(new_pbp_df, [6,5,4,3], [6,5,4,3])
            team_5v5 = calc_team_metrics(new_pbp_df, [6], [6])
            team_4v4 = calc_team_metrics(new_pbp_df, [5], [5])
            team_3v3 = calc_team_metrics(new_pbp_df, [4], [4])
            team_5v4 = calc_team_metrics(new_pbp_df, [6], [5])
            team_4v5 = calc_team_metrics(new_pbp_df, [5], [6])
            team_5v3 = calc_team_metrics(new_pbp_df, [6], [4])
            team_3v5 = calc_team_metrics(new_pbp_df, [4], [6])
            team_4v3 = calc_team_metrics(new_pbp_df, [5], [4])
            team_3v4 = calc_team_metrics(new_pbp_df, [4], [5])

            team_tables = ['team_3v3', 'team_3v3_adj', 'team_3v4', 'team_3v4_adj',
                           'team_3v5', 'team_3v5_adj', 'team_4v3', 'team_4v3_adj',
                           'team_4v4', 'team_4v4_adj', 'team_4v5', 'team_4v5_adj',
                           'team_5v3', 'team_5v3_adj', 'team_5v4', 'team_5v4_adj',
                           'team_5v5', 'team_5v5_adj', 'team_allsits',
                           'team_allsits_adj']

            team_data = [team_3v3, team_3v4, team_3v5,
                         team_4v3, team_4v4, team_4v5,
                         team_5v3, team_5v4, team_5v5,
                         team_allsits]

#inserting team tables into the database
            for df, table in zip(team_data, team_tables):
                if df.toi.sum() > 0:
                    df.columns = list(map(str.lower, df.columns))
                    sched_insert(df[df.toi > 0], table)

            logging.info(f'{key} team stats calculated and inserted')
#calculate all the goalie stats for all strength states and then inserting
#them into the table
            goalie_allsits = calc_goalie_metrics(new_pbp_df, [6,5,4,3], [6,5,4,3])
            goalie_5v5 = calc_goalie_metrics(new_pbp_df, [6], [6])
            goalie_4v4 = calc_goalie_metrics(new_pbp_df, [5], [5])
            goalie_3v3 = calc_goalie_metrics(new_pbp_df, [4], [4])
            goalie_5v4 = calc_goalie_metrics(new_pbp_df, [6], [5])
            goalie_4v5 = calc_goalie_metrics(new_pbp_df, [5], [6])
            goalie_5v3 = calc_goalie_metrics(new_pbp_df, [6], [4])
            goalie_3v5 = calc_goalie_metrics(new_pbp_df, [4], [6])

            goalie_tables = ['goalie_3v3', 'goalie_3v5',
                             'goalie_4v4', 'goalie_4v5',
                             'goalie_5v3', 'goalie_5v4',
                             'goalie_5v5',  'goalie_allsits']

            goalie_data = [goalie_3v3, goalie_3v5, goalie_4v4, goalie_4v5,
                           goalie_5v3, goalie_5v4, goalie_5v5,  goalie_allsits]

#inserting goalie stats into the database
            for df, table in zip(goalie_data, goalie_tables):
                if df.toi.sum() > 0:
                    df.columns = list(map(str.lower, df.columns))
                    sched_insert(df[df.toi > 0], table)

            logging.info(f'{key} goalie stats calculated and inserted')

        except (AttributeError, ValueError) as e:
            logging.exception(f'Game Id {key} did not scrape')


    #TODO write code to write all the games with erros to a file that another
    #script will rescrape periodically until all data is clean


if __name__ == '__main__':
    main()
