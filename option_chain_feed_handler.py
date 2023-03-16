import time
import argparse
from datetime import datetime
import logging
import data_processor
import sqlite_functions


def main():
    """
    Main function to run the NSE Option chain feed handler
    :return:
    """
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    logging.info("Starting the NSE Option chain feed handler")

    parser = argparse.ArgumentParser(description='This is can we used to record to NSE India options chan data ')
    parser.add_argument("-ticker",
                        help="enter a valid ticker default is NIFTY "
                             "e.g {NIFTY | FINNIFTY | BANKNIFTY | NIFTYMID50 | MIDCPNIFTY}",
                        default="NIFTY")
    args = parser.parse_args()
    symbol = args.ticker

    db_file = f"C:\\Users\\admin\\projects\\optionChain\\{symbol}-{datetime.today().strftime('%Y-%m-%d-%H%M')}.db"

    with sqlite_functions.create_db(db_file) as db:
        sqlite_functions.create_table(db, 'oi_chain', '''CREATE TABLE oi_chain (CALL_OI bigint,
                                                                  PUT_OI bigint,
                                                                  TotalChinCallOI integer,
                                                                  TotalChinPutOI integer,
                                                                  "oi_difference" real,
                                                                  Call_Put_Ratio real,
                                                                  TotalCallVol integer,
                                                                  TotalPutVol integer,
                                                                  SPOT_PRICE real,
                                                                  TimeStamp datetime)''',
                                                                drop_first=True)

        output_dict1 = {}
        while True:
            try:
                final_oi = data_processor.calculate_OI(symbol)
                word_list = final_oi.split()
                output_dict = {}
                for i in range(0, len(word_list), 2):
                    if word_list[i] == 'TimeStamp':
                        # If it is, get the time stamp
                        output_dict['TimeStamp'] = ' '.join(word_list[-2:])
                        break
                    else:
                        output_dict[word_list[i]] = word_list[i + 1]

                if output_dict != output_dict1:
                    sqlite_functions.sql_insert(db, 'oi_chain', output_dict)
                    output_dict1 = output_dict
                    logging.info(f"Inserted new record for {symbol}")
                else:
                    logging.info(f"Skipping duplicate record for {symbol}")

                time.sleep(25)  # NSE only publish data every 30 seconds

            except Exception as e:
                logging.error(f"Error occurred while fetching or inserting data: {e}")


if __name__ == '__main__':
    main()
