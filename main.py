import datetime
import requests
import csv
import gzip
import shutil
import os
import re
import gnsscal
from dotenv import load_dotenv
import pandas as pd
from datetime import date
import argparse

load_dotenv()

EARTH_DATA_USERNAME = os.getenv('EARTH_DATA_USERNAME')
EARTH_DATA_PASSWORD = os.getenv('EARTH_DATA_PASSWORD')


class GpsTime(object):
    def __init__(self, days):
        self.today = date.today()
        self.timedelta_buffer = None
        self.no_weeks = None
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
        self.gps_time = gnsscal.date2gpswd(self.timedelta_buffer.date())
        self.no_weeks = self.gps_time[0]
        self.year = self.timedelta_buffer.year

        for hour in self.hour_list:
            self.date_string_array.append(
                [self.prefix + str(self.year) + self.total_days +
                 hour + self.suffix, ]
            )

        return self


class IGU(object):

    def __init__(self, days):
        self.site = ['https://cddis.nasa.gov/archive/gnss/products']
        self.days_count = days
        self.log_folder = 'log'
        self.gps_info = GpsTime(self.days_count).form_info()
        self.user_name = EARTH_DATA_USERNAME
        self.password = EARTH_DATA_PASSWORD
        self.name_match_string = r'[0-9]{5}_[0-9]{2}'
        self.meta_data = None
        self.igu_folder = 'igu_files'
        self.temp = 'tmp_files'
        self.data = {"Date": self.gps_info.timedelta_buffer,
                     "Year": self.gps_info.year,
                     "Day of The Year": self.gps_info.total_days,
                     "GPS Week": self.gps_info.no_weeks}
        self.folders = [self.igu_folder,
                        self.log_folder,
                        self.temp]

    def check_connection(self):
        url = self.site[0]
        with requests.Session() as session:
            session.auth = (self.user_name, self.password)
            r1 = session.request('get', url)
            r = session.get(r1.url, auth=(self.user_name, self.password))
            assert r.ok, "There is no connection!!! Check remote site address!!!"
            return True

    def check_folders(self):
        for folder in self.folders:
            if not os.path.exists(f'./{folder}'):
                os.mkdir(f'./{folder}')

    def get_file(self, filename):

        if self.check_connection():

            url = f'{self.site[0]}/{self.gps_info.no_weeks}/{filename}'
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
            self.meta_data = {"file_name": filename,
                              "name_string": name_string[0],
                              "date_start": df.Date.min(),
                              "date_end": df.Date.max(),
                              }

    def rename_file(self, filename):
        new_name = "igu" + self.meta_data['name_string'] + ".sp3"
        os.rename(filename, new_name)
        # shutil.move(filename, self.temp)
        return new_name

    def compress_new_data(self, filename):

        with open(filename, 'rb') as infile, \
                gzip.open(os.path.join(self.igu_folder, filename + ".Z"), "wb") as gzip_file:
            gzip_file.writelines(infile)

        shutil.move(os.path.join('./', filename), os.path.join(self.temp, filename))

    def uncompress(self, filename):
        sp3_name = filename.split('.gz')[0].lower()
        with gzip.open(filename, 'rb') as f_in:
            with open(sp3_name, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        shutil.move(os.path.join('./', filename), os.path.join(self.temp, filename))
        return sp3_name


if __name__ == '__main__':
    # day 1 yesterday
    # 2 days before
    # 3 days before
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', type=int, required=False)
    args = parser.parse_args()
    if args.d is None:
        day_to_look = 1
    else:
        day_to_look = args.d

    igu_data = IGU(day_to_look)
    igu_data.check_folders()
    res = pd.DataFrame(columns=['file_name',
                                'name_string',
                                'date_start',
                                'date_end',
                                'new_file_name',
                                'date_to_download',
                                'day_of_year',
                                'downloaded_at'])

    print(igu_data.data)

    for file_ in igu_data.gps_info.date_string_array:

        if igu_data.get_file(file_[0]):
            print(file_[0], "...Done")
            uncompressed_file = igu_data.uncompress(file_[0])
            igu_data.get_metadata_info(uncompressed_file)
            # res
            print(igu_data.meta_data)
            if igu_data.meta_data['date_start'].date() <= \
                    igu_data.gps_info.timedelta_buffer.date() <= \
                    igu_data.meta_data['date_end'].date():
                new_file_name = igu_data.rename_file(uncompressed_file)
                igu_data.compress_new_data(new_file_name)
                igu_data.meta_data['new_file_name'] = new_file_name
                igu_data.meta_data['date_to_download'] = igu_data.gps_info.timedelta_buffer.date()
                igu_data.meta_data['day_of_year'] = igu_data.gps_info.total_days
                igu_data.meta_data['downloaded_at'] = datetime.datetime.now()
                res = res.append(igu_data.meta_data, ignore_index=True)
            else:
                print('Date is not relevant !! check dates !! ')
        else:
            print(file_[0], "...Not Available")
    log_file = os.path.join(igu_data.log_folder, 'log_' + datetime.datetime.today().strftime("%Y%m%d") + '.log')
    res.to_csv(log_file, mode='a', header=not os.path.exists(log_file))
