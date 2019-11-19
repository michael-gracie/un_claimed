import pandas as pd
import config as config
from time import sleep
import logging
import import urllib.parse

logging.getLogger().addHandler(logging.StreamHandler())
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

def pull_properties(search):
    """Iteratively pull properties

    Parameters
    ----------
    search : str
        Term to search

    Returns
    -------
    pd.DataFrame
        Dataframe with the properties

    """
    counter = 1
    formatted_df = pd.DataFrame()
    url = create_url(search, counter)
    df = pull_table(url)
    while df is not None:
        formatted_df = pd.concat((format_table(df), formatted_df))
        counter += 1
        url = create_url(search, counter)
        sleep(5)
        logger.info(f'Pulling info for term: {search}, page: {counter}')
        df = pull_table(url)
    return formatted_df

def create_url(search, pg):
    """Create the url

    Parameters
    ----------
    search : str
        The string to search
    pg : int
        The page number to return

    Returns
    -------
    str
        URL to search
    """
    url_search = urllib.parse.quote(search)
    url = f'https://ubmswww.bank-banque-canada.ca/en/Property/SearchIndex?page={pg}&searchType=Person&lastName={url_search}&propertyId=0'
    return url

def pull_table(url):
    """Pulls data from unclaims website into unformatted pandas dataframe

    Parameters
    ----------
    url : str
        Url for uncliams data base

    Returns
    -------
    pd.DataFrame
        Unformatted pandas data frame

    """
    df_list = pd.read_html(url)
    if len(df_list) > 3:
        return df_list[3]
    else:
        return None

def format_table(df):
    """Formats the html table that was returned

    Parameters
    ----------
    df : pd.DataFrame
        Pandas dataframe form the Unclaims website

    Returns
    -------
    pd.DataFrame
        Formatted dataframe
    """
    columns = [
        'Orignating Account Number',
        'Payee',
        'Payer Address',
        'Amount',
        'Account Co Owner',
        'Payer FI',
        'Type of Payment',
        'Date of Payment',
        'Date of BOC Transfer',
    ]
    base_df = pd.DataFrame(columns = columns)
    for row in range(int(len(df)/4)):
        strt = row*4
        insert_row = list(
            df.iloc[strt][1:]) + [
            df.iloc[strt+2][0].replace('Reported By: ','')] + list(
            config.bal_row_reg.search(df.iloc[strt+3][0]).groups()
            )
        try:
            base_df.loc[row] = insert_row
        except IndexError:
            raise Exception(f'Row is not right length {insert_row}')
    return base_df
