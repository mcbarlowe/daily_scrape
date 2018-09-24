"""
Process any new players and add to db
Code written by Harry Shomer with some additions by Matthew Barlowe

All runs through - process_players
"""

import json
import time
import os
import numpy as np
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from sqlalchemy import create_engine
import logging
logger = logging.getLogger(__name__)

def get_page(url):
    """
    Get the page

    :param url: given url

    :return: page
    """
    response = requests.Session()
    retries = Retry(total=10, backoff_factor=.1)
    response.mount('http://', HTTPAdapter(max_retries=retries))
    response = response.get(url, timeout=5)
    response.raise_for_status()

    #time.sleep(1)

    return json.loads(response.text)


def get_player_info(player_id):
    """
    Scrape info for a given player

    :param player_id: id per NHL

    :return: player info
    """
    url = 'http://statsapi.web.nhl.com/api/v1/people/{}'.format(player_id)
    response = get_page(url)

    print("Scraping a new player...  " + response['people'][0]['fullName'])
    p = dict()

    p['id'] = response['people'][0]['id']
    p['name'] = response['people'][0]['fullName']

    try:
        p['birth_date'] = response['people'][0]['birthDate']
    except KeyError:
        p['birth_date'] = "Na"

    try:
        p['nationality'] = response['people'][0]['nationality']
    except KeyError:
        p['nationality'] = "Na"

    try:
        p['height'] = response['people'][0]['height']
    except KeyError:
        p['height'] = "Na"

    try:
        p['weight'] = response['people'][0]['weight']
    except KeyError:
        p['weight'] = 0

    try:
        p['active'] = response['people'][0]['active']
    except KeyError:
        p['active'] = 0

    try:
        p['shoots_catches'] = response['people'][0]['shootsCatches']
    except KeyError:
        p['shoots_catches'] = "Na"

    try:
        p['position'] = response['people'][0]['primaryPosition']['abbreviation']
    except KeyError:
        p['position'] = "Na"

    return p


def process_players(df):
    """
    Get player info for each player and store in db
    """
    # Make columns lowercase...so it's in line with the db
    df.columns = map(str.lower, df.columns)

#TODO need to change this to prod database once everything is working
    engine = create_engine(os.environ.get('DEV_DB_CONNECT'))
    player_df = pd.read_sql_table('nhl_players', engine, schema='nhl_tables')

    # Get list of id's for players in scraped games
    df = df[df['player_id'].notnull()]
    scraped_players = set(x for x in df['player_id'].tolist())
    scraped_players = [int(x) for x in scraped_players if x != np.nan]

    # Get list of id's already in database
    players = set(x for x in player_df['id'].tolist())
    players = [int(x) for x in players]

    # Scrape info for any new players
    players_info = [get_player_info(player_id) for player_id in scraped_players if player_id not in players]

    # If any new players append to the database
    if len(players_info) != 0:
        print('\n')
        df = pd.DataFrame(players_info)

        df.to_sql('nhl_players', engine, schema='nhl_tables',
                  if_exists='append', index=False)
        logger.info(f"{players_info} added to players table")
    else:
        logger.info(f"No new players added to db from game {df.game_id.unique()}")



def main():
    '''
    passing all the shift dataframes to populater the player table
    '''

    years = ['20102011', '20112012', '20122013', '20132014', '20142015',
             '20152016', '20162017', '20172018']

    for year in years:

        shifts_df = pd.read_csv(f'scraped_files/nhl_shifts{year}.csv')

        process_players(shifts_df)

    return

if __name__ == '__main__':
    main()





