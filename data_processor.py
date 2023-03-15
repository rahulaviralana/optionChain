import pandas as pd
import nse_public_api

global timestamp


def __convert_dataframe__(raw_json_data):
    """
    This function will convert the supplied data to Pandas Data Frame
    :return: This
    """
    try:
        global timestamp
        timestamp = raw_json_data['records']['timestamp'].split(" ")[1]
        rawdata = pd.DataFrame(raw_json_data)
        rawop = pd.DataFrame(rawdata['filtered']['data']).fillna(0)
        return rawop
    except Exception as e:
        return e


def __dataframe__(rawop):
    try:
        data = []
        for i in range(0, len(rawop)):
            calloi = callcoi = cltp = callvol = putoi = putcoi = pltp = putvol = ulatp = 0
            stp = rawop['strikePrice'][i]
            if rawop['CE'][i] == 0:
                calloi = callcoi = 0
            else:
                calloi = rawop['CE'][i]['openInterest']
                callcoi = rawop['CE'][i]['changeinOpenInterest']
                cltp = rawop['CE'][i]['lastPrice']
                callvol = rawop['CE'][i]['totalTradedVolume']
                ulatp = rawop['CE'][i]['underlyingValue']

            if rawop['PE'][i] == 0:
                putoi = putcoi = 0
            else:
                putoi = rawop['PE'][i]['openInterest']
                putcoi = rawop['PE'][i]['changeinOpenInterest']
                pltp = rawop['PE'][i]['lastPrice']
                putvol = rawop['PE'][i]['totalTradedVolume']

            opdata = {'CALL_OI': calloi, 'CALL_CHNG_OI': callcoi, 'CALL_LTP': cltp, 'CALL_VOLUME': callvol,
                      'STRIKE_PRICE': stp, 'CURRENT_SPOT_PRICE': ulatp, 'PUT_OI': putoi, 'PUT_CHNG_OI': putcoi,
                      'PUT_LTP': pltp, 'PUT_VOLUME': putvol, }

            data.append(opdata)

        optionchain = pd.DataFrame(data)
        return optionchain
    except (SystemExit, AssertionError, KeyError, MemoryError, KeyboardInterrupt, Exception) as e:
        print('There was an exception in __dataframe__ ')
        return e
    except:
        return


def __calculate_OI__(symbol='NIFTY'):
    try:
        rawop = __convert_dataframe__(nse_public_api.get_data(symbol))
        optionchain = __dataframe__(rawop)
        # Compute Call OI
        TotalCallOI = optionchain['CALL_OI'].sum()
        # Compute Put OI
        TotalPutOI = optionchain['PUT_OI'].sum()
        # Compute Call Change OI
        TotalCallChOI = optionchain['CALL_CHNG_OI'].sum()
        # Compute Put OI
        TotalPutChOI = optionchain['PUT_CHNG_OI'].sum()
        # Compute Call Volume
        TotalCallVol = optionchain['CALL_VOLUME'].sum()
        # Compute Put Volume
        TotalPutVol = optionchain['PUT_VOLUME'].sum()
        # Compute Current Spot Price
        Totalulatp = optionchain['CURRENT_SPOT_PRICE'].sum()
        # Average Spot Price
        avgc = len(optionchain['CURRENT_SPOT_PRICE'])
        # Call Put ratio
        if TotalPutOI:
            call_put_ratio = TotalCallOI / TotalPutOI
        else:
            call_put_ratio = 0

        return (
                f'CALL_OI {TotalCallOI} PUT_OI {TotalPutOI} TotalChinCallOI {TotalCallChOI} '
                f'TotalChinPutOI {TotalPutChOI} oi_difference {TotalCallOI - TotalPutOI} '
                f'Call_Put_Ratio {call_put_ratio} TotalCallVol {TotalCallVol} TotalPutVol {TotalPutVol}'
                f' SPOT_PRICE {Totalulatp / avgc} TimeStamp {timestamp}')
    except (SystemExit, AssertionError, KeyError, MemoryError, KeyboardInterrupt, TypeError, Exception) as e:
        print('There was an error in __calculate_OI__ ')
        return e
