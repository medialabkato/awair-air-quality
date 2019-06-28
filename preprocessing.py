#! /usr/bin/python3
# coding: utf-8

import decimal
import pandas as pd
import sqlite3
import logging
import os
import sys
import argparse
pd.options.mode.chained_assignment = None
decimal.getcontext().rounding = decimal.ROUND_HALF_UP


class Awair:
    """Katowice air pollution analysis

    Based on data from Awair air pollution monitoring system
    (https://powietrze.katowice.eu/) through json API.
    Available indicators: PM2.5, PM10.

    Args:
        pm10 measurements database (sqlite3 db)
        pm10 measurements file (csv)
        air stations info (csv)

    Returns:
        pm10 api raw data (csv)
        pm10 hourly stats (csv)
        pm10 daily stats (csv)
        pm10 monthly stats (csv)
    """

    def __init__(self, input_file, log_level=logging.INFO):
        self.log = None
        self.data = None
        self.hourly = None
        self.daily = None
        self.monthly = None

        self.configure_logger(log_level)
        self.read_data(input_file)

    def configure_logger(self, log_level):
        """
        Configures logger
        """
        self.log = logging.getLogger('__main__')
        self.log.setLevel(log_level)
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(
            '%(asctime)s %(levelname)s %(message)s'))
        self.log.addHandler(handler)

    def read_data(self, input_file):
        if not os.path.exists(input_file):
            self.log.error(f"Input file does not exist")
            sys.exit(1)
        extension = os.path.splitext(input_file)[1].lower()
        if extension == '.db':
            self.data = self._read_sqlite(input_file)
        elif extension == '.csv':
            self.data = self._read_csv(input_file)
        else:
            self.log.error(f"Invalid source extension. Permitted extensions are .db for sqlite file or .csv")
            sys.exit(1)

    def _read_sqlite(self, db_path):
        """
        Reads sqllite database
        """
        conn = sqlite3.connect(db_path)
        data = pd.read_sql_query(
            "SELECT station_id, measure_time, value FROM pm10", conn)
        conn.close()
        self.log.info(f"Loaded database content")
        return data

    def _read_csv(self, csv_path):
        """
        Reads csv file
        """
        data = pd.read_csv(
            csv_path, names=['station_id', 'measure_time', 'value'], skiprows=1)
        self.log.info(f"Loaded csv file")
        return data

    def preprocess_data(self, output_dir, output_file='data.csv', sort_order=['station_id', 'measure_time'],
                        lower_band='2019-04-01', upper_band='2019-05-01'):
        """
        Makes preprocessing of data and dumps it to a csv file
        """
        assert self.data is not None

        self.convert_datetime()
        self.remove_duplicates()
        self.sort_values(sort_order)
        self.limit_time_range(lower_band, upper_band)
        self.export_to_csv(self.data, output_dir, output_file)

    def generate_hourly_stats(self, output_dir, output_file='hourly_stats.csv'):
        """
        Calculates hourly stats
        """
        assert self.data is not None

        self.hourly = self.hourly_stats()
        self.export_to_csv(self.hourly, output_dir, output_file)

    def generate_daily_stats(self, output_dir, output_file='daily_stats.csv'):
        """
        Calculates daily stats
        """
        assert self.hourly is not None

        self.daily = self.daily_stats()
        self.perc_of_norm()
        self.export_to_csv(self.daily, output_dir, output_file)

    def generate_monthly_stats(self, station_file, output_dir, output_file='monthly_stats.csv'):
        """
        Calculates monthly stats
        """
        assert self.daily is not None

        self.monthly = self.monthly_stats()
        self.add_station_info(station_file)
        self.monthly = self.monthly[['station_id', 'date', 'station_name', 'station_address',
                                     'district_id', 'district', 'lat', 'lon',
                                     'days_num', 'days_abv_norm', 'days_abv_200',
                                     'days_abv_300', 'max', 'mean']]
        self.export_to_csv(self.monthly, output_dir, output_file)

    def convert_datetime(self):
        """
        Converts inconsistent date formats
        """
        self.data['measure_time'] = pd.to_datetime(
            self.data['measure_time'], utc=True).dt.tz_convert('Europe/Vienna')
        self.log.info(f"Converted inconsistent date formats")

    def remove_duplicates(self):
        """
        Removes duplicated rows (based on timestamps)
        """
        len_old = len(self.data)
        self.data = self.data.drop_duplicates()
        diff = len_old - len(self.data)
        self.log.info(f"Removed {diff} duplicated rows")

    def sort_values(self, order):
        """
        Sorts values
        """
        self.data['station_id'] = self.data['station_id'].astype(int)
        self.data = self.data.sort_values(by=order)
        self.log.info(f"Sorted values by {order}")

    def limit_time_range(self, lower_band, upper_band):
        """
        Limits data to a specified time range
        """
        self.data = self.data[(self.data['measure_time'] >= lower_band) & (
            self.data['measure_time'] < upper_band)]
        self.log.info(f"Limited data to the time period between {lower_band} and {upper_band}")

    def export_to_csv(self, data, output_dir, output_file):
        """
        Exports data to a csv file
        """
        output_path = os.path.join(output_dir, output_file)
        data.to_csv(output_path, index=False)
        self.log.info(f"Exported data to the file {output_path}")

    def hourly_stats(self):
        """
        Calculates the hourly average
        """
        hourly = self.data[['station_id', 'value']]
        hourly['date'] = self.data['measure_time'].dt.date
        hourly['hour'] = self.data['measure_time'].dt.hour
        hourly = hourly.groupby(['station_id', 'date', 'hour'], as_index=False)[
            'value'].agg({'value': 'mean'})
        hourly['value'] = hourly['value'].map(self._round_values)
        self.log.info(f'Calculated the hourly average')
        return hourly

    def _round_values(self, value, precision="1."):
        """
        Converts float to Decimal and rounds according to official standards
        """
        return decimal.Decimal(str(value)).quantize(decimal.Decimal(precision))

    def daily_stats(self, min_hour=18):
        """
        Calculates daily average values (for days with minimum 75% hourly average values)
        """
        daily = self.hourly.groupby(['station_id', 'date']).filter(
            lambda x: x.hour.count() >= min_hour)
        daily['value'] = daily['value'].astype(float)
        daily = daily.groupby(['station_id', 'date'], as_index=False)['value'].agg({'min': 'min',
                                                                                    'max': 'max',
                                                                                    'mean': 'mean'})
        daily[['min', 'max', 'mean']] = daily[[
            'min', 'max', 'mean']].applymap(self._round_values)
        self.log.info(f"Calculated daily average values")
        return daily

    def monthly_stats(self):
        """
        Calculates monthly stats: mean value and number of days with exceeded thresholds
        """
        self.daily['mean'] = self.daily['mean'].astype(float)
        self.daily['date'] = self.daily['date'].astype('datetime64[M]')
        monthly = self.daily.groupby(['station_id', 'date']).agg({'mean':
                                                                  ['mean',
                                                                   ('days_num',
                                                                    'size'),
                                                                      ('days_abv_norm', lambda x: sum(
                                                                          x > 50)),
                                                                      ('days_abv_200', lambda x: sum(
                                                                          x > 200)),
                                                                      ('days_abv_300', lambda x: sum(
                                                                          x > 300))
                                                                   ],
                                                                  'max': 'max'
                                                                  })

        monthly.columns = monthly.columns.get_level_values(1)
        monthly.reset_index(inplace=True)
        monthly[['mean', 'max']] = monthly[[
            'mean', 'max']].applymap(self._round_values)
        monthly[['days_abv_norm', 'days_abv_200', 'days_abv_300']] = monthly[[
            'days_abv_norm', 'days_abv_200', 'days_abv_300']].astype(int)
        monthly.sort_values(['station_id', 'date'],
                            ascending=True, inplace=True)
        self.log.info(f"Calculated monthly statistics")
        return monthly

    def add_station_info(self, station_file):
        """
        Loads csv with stations information and adds it to monthly stats
        """
        if not os.path.exists(station_file):
            self.log.error(f"Station file does not exist")
            sys.exit(1)

        stations = pd.read_csv(
            station_file, float_precision='high', encoding='utf-8')
        self.monthly['station_id'] = self.monthly['station_id'].astype(int)
        self.monthly = self.monthly.merge(
            stations, left_on='station_id', right_on='id', how='left')
        self.monthly.drop('id', axis=1, inplace=True)
        self.log.info(f"Monthly stats merged with station info")

    def perc_of_norm(self, norm=50, precision="1."):
        """
        Calculates percent of an acceptable norm
        """
        self.daily['perc_of_norm'] = self.daily['mean'].apply(
            lambda x: decimal.Decimal(str(x / norm * 100)).quantize(decimal.Decimal(precision)))
        self.log.info("Calculated percent of an acceptable norm")


def main():
    # Parsing command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-input_file', '--input-file', required=True)
    parser.add_argument('-stations_file', '--stations_file', required=False)
    parser.add_argument('-output_dir', '--output-dir',
                        default='output', required=False)
    args = parser.parse_args()

    awair_analysis = Awair(args.input_file)
    awair_analysis.preprocess_data(args.output_dir)
    awair_analysis.generate_hourly_stats(args.output_dir)
    awair_analysis.generate_daily_stats(args.output_dir)
    awair_analysis.generate_monthly_stats(args.stations_file, args.output_dir)


if __name__ == '__main__':
    main()
