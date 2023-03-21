import time
import argparse
import datetime
import pytz
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

    # Set the timezone
    tz = pytz.timezone('Asia/Kolkata')

    # Get the current time in India
    current_time = datetime.datetime.now(tz).time()
    today = datetime.datetime.now(tz).date()

    parser = argparse.ArgumentParser(description='This is can we used to record to NSE India options chan data ')
    parser.add_argument("-ticker", "-t",
                        help="enter a valid ticker default is NIFTY "
                             "e.g {NIFTY | FINNIFTY | BANKNIFTY | NIFTYMID50 | MIDCPNIFTY}",
                        default="NIFTY")
    args = parser.parse_args()
    symbol = args.ticker

    output_dict1 = {}
    if datetime.time(9, 15) <= current_time <= datetime.time(15, 30):
        db_file = f"{symbol}-{datetime.datetime.today().strftime('%Y-%m-%d-%H%M')}.db"

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
        while True:
            try:
                # Check if the current time is within the desired range
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

                # parse the timestamp from the dictionary
                timestamp_str = output_dict['TimeStamp']
                timestamp = datetime.datetime.strptime(timestamp_str, '%d-%b-%Y %H:%M:%S')

                # convert the timestamp to the timezone
                timestamp = timestamp.replace(tzinfo=tz)
                if timestamp.date() == today:
                    if output_dict != output_dict1:
                        sqlite_functions.sql_insert(db, 'oi_chain', output_dict)
                        logging.info(f"Inserting {output_dict}")
                        output_dict1 = output_dict
                        logging.info(f"Inserted new record for {symbol}")
                    else:
                        logging.info(f"Skipping duplicate record for {symbol}")

                time.sleep(25)  # NSE only publish data every 30 seconds

                now = datetime.datetime.now(tz)
                end_time = datetime.datetime(now.year, now.month, now.day, 15, 35, 0, tzinfo=now.tzinfo).time()

                if now.time() >= end_time:
                    break

            except KeyboardInterrupt:
                print('Keyboard Interrupted')
            except Exception as e:
                logging.error(f"Error occurred while fetching or inserting data: {e}")

    else:
        logging.info("Outside NSE trading hours")


if __name__ == '__main__':
    main()
