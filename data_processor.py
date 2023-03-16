import numpy as np
import pandas as pd
import logging
import nse_public_api


logging.basicConfig(level=logging.INFO)
global timestamp

def convert_dataframe(raw_json_data):
    """
    Convert the supplied data to Pandas DataFrame.
    """
    try:
        timestamp = raw_json_data['records']['timestamp']
        rawdata = pd.DataFrame(raw_json_data)
        rawop = pd.DataFrame(rawdata['filtered']['data']).fillna(0)
        return rawop
    except Exception as e:
        logging.error(f"Error occurred in convert_dataframe: {e}")
        raise e


def process_dataframe(rawop):
    """
    This function processes the Pandas DataFrame to extract relevant data
    :param rawop: Pandas DataFrame
    :return: Processed Pandas DataFrame
    """
    try:
        data = []
        for i in range(len(rawop)):
            call_oi, call_coi, call_ltp, call_vol, put_oi, put_coi, put_ltp, put_vol, spot_price = [0] * 9
            stp = rawop['strikePrice'][i]
            if rawop['CE'][i] != 0:
                call_oi = rawop['CE'][i]['openInterest']
                call_coi = rawop['CE'][i]['changeinOpenInterest']
                call_ltp = rawop['CE'][i]['lastPrice']
                call_vol = rawop['CE'][i]['totalTradedVolume']
                spot_price = rawop['CE'][i]['underlyingValue']

            if rawop['PE'][i] != 0:
                put_oi = rawop['PE'][i]['openInterest']
                put_coi = rawop['PE'][i]['changeinOpenInterest']
                put_ltp = rawop['PE'][i]['lastPrice']
                put_vol = rawop['PE'][i]['totalTradedVolume']
                if spot_price == 0:
                    spot_price = rawop['PE'][i]['underlyingValue']

            opdata = {'CALL_OI': call_oi, 'CALL_CHNG_OI': call_coi, 'CALL_LTP': call_ltp, 'CALL_VOLUME': call_vol,
                      'STRIKE_PRICE': stp, 'CURRENT_SPOT_PRICE': spot_price, 'PUT_OI': put_oi, 'PUT_CHNG_OI': put_coi,
                      'PUT_LTP': put_ltp, 'PUT_VOLUME': put_vol, }
            data.append(opdata)

        optionchain = pd.DataFrame(data)
        return optionchain
    except Exception as e:
        logging.exception(f'Error occurred in process_dataframe: {e}')
        raise


def calculate_OI(symbol='NIFTY'):
    try:
        rawop = convert_dataframe(nse_public_api.get_data(symbol))
        optionchain = process_dataframe(rawop)

        # Compute Call OI
        TotalCallOI = np.sum(optionchain['CALL_OI'])
        # Compute Put OI
        TotalPutOI = np.sum(optionchain['PUT_OI'])
        # Compute Call Change OI
        TotalCallChOI = np.sum(optionchain['CALL_CHNG_OI'])
        # Compute Put Change OI
        TotalPutChOI = np.sum(optionchain['PUT_CHNG_OI'])
        # Compute Call Volume
        TotalCallVol = np.sum(optionchain['CALL_VOLUME'])
        # Compute Put Volume
        TotalPutVol = np.sum(optionchain['PUT_VOLUME'])
        # Compute Current Spot Price
        Totalulatp = np.sum(optionchain['CURRENT_SPOT_PRICE'])
        # Average Spot Price
        avgc = len(optionchain['CURRENT_SPOT_PRICE'])
        # Call Put ratio
        if TotalPutOI:
            call_put_ratio = TotalCallOI / TotalPutOI
        else:
            call_put_ratio = 0

        message = (f'CALL_OI {TotalCallOI} PUT_OI {TotalPutOI} TotalChinCallOI {TotalCallChOI} '
                f'TotalChinPutOI {TotalPutChOI} oi_difference {TotalCallOI - TotalPutOI} '
                f'Call_Put_Ratio {call_put_ratio} TotalCallVol {TotalCallVol} TotalPutVol {TotalPutVol}'
                f' SPOT_PRICE {Totalulatp / avgc}')

        logging.info(message)

        return message
    except (SystemExit, AssertionError, KeyError, MemoryError, KeyboardInterrupt, TypeError) as e:
        logging.exception('An error occurred in calculate_OI')
        raise e
    except Exception as e:
        logging.exception('An unknown error occurred in calculate_OI')
        raise e
