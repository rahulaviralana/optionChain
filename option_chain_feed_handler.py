import time
from datetime import datetime
import data_processor
import sqlite_functions


if __name__ == '__main__':
    """
    Main function to run the NSE Option chain feed handler
    :return: 
    """
    symbol = 'NIFTY'
    db1 = sqlite_functions.create_db("C:\\Users\\admin\\projects\\optionChain\\" + symbol + "-"
                        + datetime.today().strftime('%Y-%m-%d-%H%M') + ".db")
    sqlite_functions.create_table(db1, 'oi_chain', '''CREATE TABLE oi_chain (CALL_OI bigint,
                                          PUT_OI bigint,
                                          TotalChinCallOI integer,
                                          TotalChinPutOI integer,
                                          "oi_difference" integer,
                                          Call_Put_Ratio real,
                                          TotalCallVol integer,
                                          TotalPutVol integer,
                                          SPOT_PRICE real,
                                          TimeStamp text)''', drop_first=True)
    while True:
        final_oi = data_processor.__calculate_OI__(symbol)
        word_list = final_oi.split()
        output_dict = {}
        for i in range(0, len(word_list), 2):
            output_dict[word_list[i]] = word_list[i + 1]
        sqlite_functions.sql_insert(db1, 'oi_chain', output_dict)
        time.sleep(25)  # NSE only publish data every 30 seconds
