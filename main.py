import datetime
import gps_time
import requests
import logging
import csv
import gzip
import shutil
import os
import re
from dotenv import load_dotenv
import pandas as pd
from io import StringIO

# from mail_routine import send_email
from datetime import date, timedelta

load_dotenv()

EARTH_DATA_USERNAME = os.getenv('EARTH_DATA_USERNAME')
EARTH_DATA_PASSWORD = os.getenv('EARTH_DATA_PASSWORD')


class GpsTime(object):
    def __init__(self, days):
        self.epoch = date(1980, 1, 6)
        self.today = date.today()
        self.timedelta_buffer = None
        self.noWeeks = None
        self.total_days = None
        self.year = None
        self.gps_time = None
        self.days_count = days
        self.prefix = "IGS0OPSULT_"
        self.suffix = "_02D_15M_ORB.SP3.gz"
        self.hour_list = ['0000',
                          '0600',
                          '1200',
                          '1800']
        self.date_string_array = []

    def form_info(self):
        self.timedelta_buffer = (datetime.datetime.today() -
                                 datetime.timedelta(days=self.days_count))

        self.total_days = self.timedelta_buffer.strftime("%j")
        self.gps_time = gps_time.GPSTime.from_datetime(self.timedelta_buffer)
        self.noWeeks = self.gps_time.week_number
        self.year = self.timedelta_buffer.year

        for hour in self.hour_list:
            self.date_string_array.append(
                [self.prefix + str(self.year) + self.total_days +
                 hour + self.suffix, ]
            )

        return self


class IGU(object):

    def __init__(self, days):
        self.site = ['https://cddis.nasa.gov/archive/gnss/products/']
        self.days_count = days
        self.gps_info = GpsTime(self.days_count).form_info()
        self.user_name = EARTH_DATA_USERNAME
        self.password = EARTH_DATA_PASSWORD
        self.name_match_string = r'[0-9]{5}_[0-9]{2}'
        self.meta_data = None
        self.data = {"Date": self.gps_info.timedelta_buffer,
                     "Year": self.gps_info.year,
                     "Day of The Year": self.gps_info.total_days,
                     "GPS Week": self.gps_info.noWeeks}

    def get_file(self, filename):
        url = f'{self.site[0]}/{self.gps_info.noWeeks}/{filename}'
        with requests.Session() as session:
            session.auth = (self.user_name, self.password)
            r1 = session.request('get', url)
            r = session.get(r1.url, auth=(self.user_name, self.password))
            if r.ok:
                with open(filename, 'wb') as file:
                    file.write(r.content)
                    return True
            else:
                return False

    def get_metadata_info(self, filename):

        with open(filename, 'r') as f:
            data = [row[0] for row in csv.reader(f)]
            dates = [datetime.datetime.strptime(row[3:-12], '%Y %m %d %H %M') for row in data if re.search(r'^\*', row)]
            name_string = [re.search(self.name_match_string, row).group() for row in data if
                           re.search(self.name_match_string, row)]
            df = pd.DataFrame(dates, columns=['Date'])
            self.meta_data = {"name_string": name_string,
                              "date_start": df.Date.min(),
                              "date_end": df.Date.max(),
                              }

    def rename_file(self, filename):
        new_file_name = "igu" + self.meta_data['name_string'][0] + ".sp3"
        os.rename(filename, new_file_name)
        return new_file_name

    def compress_new_data(self, filename, meta_data):
        with open(filename, 'rb') as infile, gzip.open(os.path.join("igu_files", filename + ".Z"), "wb") as gzip_file:
            gzip_file.writelines(infile)
        print("Compressed")

    @staticmethod
    def uncompress(filename):
        sp3_name = filename.split('.gz')[0].lower()
        with gzip.open(filename, 'rb') as f_in:
            with open(sp3_name, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        return sp3_name


if __name__ == '__main__':
    igu_data = IGU(2)

    print(igu_data.data)

    #
    # print(igu_data.gps_info.timedelta_buffer,
    #       igu_data.gps_info.year,
    #       igu_data.gps_info.total_days,
    #       igu_data.gps_info.noWeeks)
    for file_ in igu_data.gps_info.date_string_array:

        if igu_data.get_file(file_[0]):

            print(file_[0], "...Done")
            uncompressed_file = igu_data.uncompress(file_[0])
            igu_data.get_metadata_info(uncompressed_file)
            print(igu_data.meta_data)
            if igu_data.meta_data['date_start'] < igu_data.gps_info.timedelta_buffer < igu_data.meta_data['date_end']:
                print('Date is relevant ')
                new_file_name = igu_data.rename_file(uncompressed_file)
                igu_data.compress_new_data(new_file_name, meta_data=igu_data.meta_data)
                pass
        else:
            print(file_[0], "...Not Available")
