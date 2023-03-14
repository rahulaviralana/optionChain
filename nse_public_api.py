import requests


"""
This module is for all interactions with NSE India public API
"""

# Global variables defined
main_url = 'https://www.nseindia.com/'
url_op_chain_symbol = 'https://www.nseindia.com/api/option-chain-indices?symbol='
headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)'
                         ' Chrome/80.0.3987.149 Safari/537.36', 'accept-language': 'en,gu;q=0.9,hi;q=0.8',
           'accept-encoding': 'gzip, deflate, br'}
sess = requests.Session()
cookies = dict()


def set_cookie():
    """
    This function tries to set the cookies by visiting the main NSE India website
    :return: None
    """
    try:
        request = sess.get(main_url, headers=headers, timeout=30)
        cookies = dict(request.cookies)

    except (SystemExit, AssertionError, MemoryError, KeyboardInterrupt, Exception) as e:
        print('There was an error connecting to NSE India ', e)
        return e


def get_data(symbol='NIFTY'):
    """
    This function gets the option chain data for the supplied symbol
    :param symbol: Supply a valid NSE India symbol e.g {NIFTY | FINNIFTY | BANKNIFTY | NIFTYMID50 | MIDCPNIFTY}
    :return:
    """
    try:
        set_cookie()
        response = sess.get(url_op_chain_symbol+symbol, headers=headers, cookies=cookies)
        if response.status_code == 401:
            set_cookie()
            response = sess.get(url_op_chain_symbol+symbol, headers=headers, cookies=cookies)
        if response.status_code == 200:
            return response.json()

    except (SystemExit, AssertionError, MemoryError, KeyboardInterrupt, Exception) as e:
        print('There was an error fetching data connecting to NSE India ', e)
        return e

# print(get_data('BANKNIFTY'))
