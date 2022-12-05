import datetime
import ftplib
import os
import urllib.request
import gps_time
import requests
import logging
import os
from dotenv import load_dotenv

# from mail_routine import send_email
from datetime import date, timedelta

load_dotenv()

EARTH_DATA_USERNAME = os.getenv('EARTH_DATA_USERNAME')
EARTH_DATA_PASSWORD = os.getenv('EARTH_DATA_PASSWORD')


class GpsTime(object):
    def __init__(self, days):
        self.epoch = date(1980, 1, 6)
        self.today = date.today()
        self.epochMonday = None
        self.todayMonday = None
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
        timedelta_buffer = (datetime.datetime.today() -
                            datetime.timedelta(days=self.days_count))

        self.total_days = timedelta_buffer.strftime("%j")
        self.gps_time = gps_time.GPSTime.from_datetime(timedelta_buffer)
        self.noWeeks = self.gps_time.week_number
        self.year = timedelta_buffer.year

        for hour in self.hour_list:
            self.date_string_array.append(
                [self.prefix + str(self.year) + self.total_days +
                 hour + self.suffix, ]
            )

        return self


class IGU(object):

    def __init__(self):
        self.site = ['https://cddis.nasa.gov/archive/gnss/products/']
        self.gps_info = GpsTime(2).form_info()
        self.user_name = EARTH_DATA_USERNAME
        self.password = EARTH_DATA_PASSWORD

        print(self.gps_info.noWeeks)

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


if __name__ == '__main__':
    igu_data = IGU()

    for file_ in igu_data.gps_info.date_string_array:
        if igu_data.get_file(file_[0]):
            print(file_[0], "...Done")
        else:
            print(file_[0], "...Not Available")
