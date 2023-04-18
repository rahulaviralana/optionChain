import logging
import requests


"""
This module is for all interactions with NSE India public API
"""

# Global variables defined
MAIN_URL = 'https://www.nseindia.com/'
URL_OP_CHAIN_SYMBOL = 'https://www.nseindia.com/api/option-chain-indices?symbol='
HEADERS = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)'
                  ' Chrome/80.0.3987.149 Safari/537.36',
    'accept-language': 'en,gu;q=0.9,hi;q=0.8',
    'accept-encoding': 'gzip, deflate, br'
}

logger = logging.getLogger(__name__)
sess = requests.Session()


def set_cookie():
    """
    This function tries to set the cookies by visiting the main NSE India website
    :return: None
    """
    try:
        request = sess.get(MAIN_URL, headers=HEADERS, timeout=30)
        cookies = dict(request.cookies)
        return cookies

    except (requests.exceptions.RequestException, Exception) as e:
        logger.error('Error connecting to NSE India: %s', e)
        raise


def get_data(symbol='NIFTY'):
    """
    This function gets the option chain data for the supplied symbol
    :param symbol: Supply a valid NSE India symbol e.g {NIFTY | FINNIFTY | BANKNIFTY | NIFTYMID50 | MIDCPNIFTY}
    :return:
    """
    try:
        cookies = set_cookie()
        response = sess.get(URL_OP_CHAIN_SYMBOL+symbol, headers=HEADERS, cookies=cookies)
        if response.status_code == 401:
            cookies = set_cookie()
            response = sess.get(URL_OP_CHAIN_SYMBOL+symbol, headers=HEADERS, cookies=cookies)
        if response.status_code == 404:
            logger.error('No data from NSE. Got 404!')
            pass
        if response.status_code == 200:
            return response.json()

        logger.error('Error fetching data from NSE India: %s', response.text)
        response.raise_for_status()

    except (requests.exceptions.RequestException, Exception) as e:
        logger.error('Error fetching data from NSE India: %s', e)
        raise
