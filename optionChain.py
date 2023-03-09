import requests
import pandas as pd
import time

def fetch_nse_data():
    headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36'
    }
    main_url = "https://www.nseindia.com/"
    url = "https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY"
    response = requests.get(main_url, headers=headers)
    # print(response.status_code)

    cookies = response.cookies
    # print(cookies)

    nifty_oi_data = requests.get(url, headers=headers, cookies=cookies).json()
    # print(nifty_oi_data)

    rawdata = pd.DataFrame(nifty_oi_data)
    rawop = pd.DataFrame(rawdata['filtered']['data']).fillna(0)
    response.close()
    # print(rawdata['filtered']['data'])
    # print(rawop)
    return rawop


def dataframe(rawop):
    data = []
    for i in range(0,len(rawop)):
        calloi = callcoi = cltp = callvol = putoi = putcoi = pltp = putvol = ulatp = 0
        stp = rawop['strikePrice'][i]
        if(rawop['CE'][i]==0):
            calloi = callcoi = 0
        else:
            calloi = rawop['CE'][i]['openInterest']
            callcoi = rawop['CE'][i]['changeinOpenInterest']
            cltp = rawop['CE'][i]['lastPrice']
            callvol = rawop['CE'][i]['totalTradedVolume']
            ulatp = rawop['CE'][i]['underlyingValue']

        if(rawop['PE'][i]==0):
            putoi = putcoi = 0
        else:
            putoi = rawop['PE'][i]['openInterest']
            putcoi = rawop['PE'][i]['changeinOpenInterest']
            pltp = rawop['PE'][i]['lastPrice']
            putvol = rawop['PE'][i]['totalTradedVolume']



        opdata = {
                'CALL OI': calloi, 'CALL CHNG OI': callcoi, 'CALL LTP': cltp, 'CALL VOLUME': callvol, 'STRIKE PRICE': stp, 'CURRENT SPOT PRICE': ulatp,
                'PUT OI': putoi, 'PUT CHNG OI': putcoi, 'PUT  LTP': pltp, 'PUT VOLUME': putvol,

        }

        data.append(opdata)

    optionchain = pd.DataFrame(data)
    return optionchain


def main():
    rawop = fetch_nse_data()
    optionchain = dataframe(rawop)
#    print(optionchain)
    TotalCallOI = optionchain['CALL OI'].sum()
#    print(TotalCallOI)
    TotalPutOI = optionchain['PUT OI'].sum()
#    print(TotalPutOI)
    TotalCallChOI = optionchain['CALL CHNG OI'].sum()
#    print(TotalCallChOI)
    TotalPutChOI = optionchain['PUT CHNG OI'].sum()
#    print(TotalPutChOI)
#    print(f'Call/Put Ratio: {TotalCallOI/TotalPutOI}')
#    print(f'oi difference: {TotalCallOI-TotalPutOI}')
    TotalCallVol = optionchain['CALL VOLUME'].sum()
    TotalPutVol = optionchain['PUT VOLUME'].sum()
#    print(TotalCallVol)
#    print(TotalPutVol)
    Totalulatp = optionchain['CURRENT SPOT PRICE'].sum()
#    print(Totalulatp)
    avgc = len(optionchain['CURRENT SPOT PRICE'])
#    print(f' SPOT PRICE: {Totalulatp/avgc}')
    print(f'CALL OI: {TotalCallOI}, PUT OI: {TotalPutOI}, TotalChinCallOI: {TotalCallChOI}, TotalChinPutOI: {TotalPutChOI},  oi difference: {TotalCallOI-TotalPutOI}, Call/Put Ratio: {TotalCallOI/TotalPutOI}, TotalCallVol: {TotalCallVol},TotalPutVol: {TotalPutVol}, SPOT_PRICE: {Totalulatp/avgc}')


while True:
    main()
    time.sleep(10)