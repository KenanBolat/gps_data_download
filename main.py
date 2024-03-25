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
from lxml import html
import ftplib

load_dotenv()

EARTH_DATA_USERNAME = os.getenv('EARTH_DATA_USERNAME')
EARTH_DATA_PASSWORD = os.getenv('EARTH_DATA_PASSWORD')
FILE_AGE = 4
IONOSPHERE_RETRO_DATA = 5


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


class CDDIS(object):

    def __init__(self, days):
        self.site = ['https://cddis.nasa.gov/archive/gnss/products']
        self.days_count = days
        self.log_folder = 'log'
        self.ionex_folder = 'ionex_files'
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
                        self.temp,
                        self.ionex_folder,
                        ]

    def check_connection(self):
        url = self.site[0]
        with requests.Session() as session:
            session.auth = (self.user_name, self.password)
            r1 = session.request('get', url)
            r = session.get(r1.url, auth=(self.user_name, self.password))
            assert r.ok, f"No connection has been established!!! Check remote site address!!!, {r.reason}"
            return True

    def check_folders(self):
        for folder in self.folders:
            if not os.path.exists(f'./{folder}'):
                os.mkdir(f'./{folder}')

    def get_file(self, url, filename):
        file_ = f'{url}/{filename}'
        with requests.Session() as session:
            session.auth = (self.user_name, self.password)
            r1 = session.request('get', file_)
            r = session.get(r1.url, auth=(self.user_name, self.password))
            if r.ok:
                with open(filename, 'wb') as file:
                    file.write(r.content)
                    return True
            else:
                return False

    @staticmethod
    def empty_folder(folder, age=FILE_AGE):

        threshold = datetime.datetime.now() - datetime.timedelta(days=age)
        for file in os.listdir(folder):
            file_path = os.path.join(folder, file)
            try:

                if datetime.datetime.fromtimestamp(os.stat(file_path).st_mtime) <= threshold:
                    if os.path.isfile(file_path):
                        os.unlink(file_path)

            except Exception as e:

                print(e)

    def get_metadata_info(self, filename, type='igu'):
        if type == 'igu':

            with open(filename, 'r') as f:
                data = [row[0] for row in csv.reader(f)]
                dates = [datetime.datetime.strptime(row[3:-12], '%Y %m %d %H %M') for row in data if
                         re.search(r'^\*', row)]
                name_string = [re.search(self.name_match_string, row).group() for row in data if
                               re.search(self.name_match_string, row)]
                df = pd.DataFrame(dates, columns=['Date'])
                self.meta_data = {"file_name": filename,
                                  "name_string": name_string[0],
                                  "date_start": df.Date.min(),
                                  "date_end": df.Date.max(),
                                  }
        elif type == 'ionex':
            with open(filename, 'r') as f:
                data = [row[0] for row in csv.reader(f)]

                epoch_of_first_map = data[12].split()
                epoch_of_last_map = data[13].split()

                self.meta_data = {"file_name": filename,
                                  "name_string": data[0].split()[1],

                                  "date_start": datetime.datetime(int(epoch_of_first_map[0]),
                                                                  int(epoch_of_first_map[1]),
                                                                  int(epoch_of_first_map[2])),

                                  "date_end": datetime.datetime(int(epoch_of_last_map[0]),
                                                                int(epoch_of_last_map[1]),
                                                                int(epoch_of_last_map[2])),
                                  }

    def rename_file(self, filename, type='igu'):
        if type == 'igu':
            new_name = f"igu{self.meta_data['name_string']}.sp3"
        elif type == 'ionex':
            new_name = f"igrg{self.gps_info.total_days}0.{str(self.gps_info.year)[2:]}i"
        os.rename(filename, new_name)
        # shutil.move(filename, self.temp)
        return new_name

    def compress_new_data(self, filename, type='igu'):
        date_string = self.meta_data['date_start'].strftime("%Y%m%d")

        if type == 'igu':
            if not os.path.exists(f'./{self.igu_folder}/{date_string}'):
                os.mkdir(f'./{self.igu_folder}/{date_string}')

            fname = os.path.join(f'./{self.igu_folder}/{date_string}', filename + ".Z")

        elif type == 'ionex':
            if not os.path.exists(f'./{self.ionex_folder}/{date_string}'):
                os.mkdir(f'./{self.ionex_folder}/{date_string}')
            fname = os.path.join(f'./{self.ionex_folder}/{date_string}', filename + ".Z")

        with open(filename, 'rb') as infile, \
                gzip.open(fname, "wb") as gzip_file:
            gzip_file.writelines(infile)

        shutil.move(os.path.join('./', filename), os.path.join(self.temp, filename))

    def uncompress(self, filename):
        uncompressed = filename.split('.gz')[0].lower()
        with gzip.open(filename, 'rb') as f_in:
            with open(uncompressed, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        shutil.move(os.path.join('./', filename), os.path.join(self.temp, filename))
        return uncompressed


class BULLETIN(object):
    def __init__(self):
        # Earth orientation parameters
        self.bulletin_a_folder = 'bulletin_a/'
        self.bulletin_b_folder = 'bulletin_b/'
        self.bulletin_c_folder = 'bulletin_c/'
        self.bulletin_d_folder = 'bulletin_d/'
        self.bulletins = {
            'bulletin_a': 'a',
            'bulletin_b': 'b',
            'bulletin_c': 'c',
            'bulletin_D': 'D',
        }
        self.folders = {"a": self.bulletin_a_folder,
                        "b": self.bulletin_b_folder,
                        "c": self.bulletin_c_folder,
                        "D": self.bulletin_d_folder}
        self.eop_link = {'a': 'https://datacenter.iers.org/availableVersions.php?id=6',
                         'b': 'https://datacenter.iers.org/availableVersions.php?id=207',
                         'c': 'https://datacenter.iers.org/availableVersions.php?id=16',
                         'D': 'https://datacenter.iers.org/availableVersions.php?id=17',
                         }
        self.xpath_tags = {
            'eop_date': '//*[@id="content"]/table/tbody/tr[2]/td[2]/span',
            'eop_link': '/html/body/div[1]/div/table/tbody/tr[2]/td[5]/a/@href',
        }

    def check_folders(self):
        for folder in self.folders.values():
            if not os.path.exists(f'./{folder}'):
                os.mkdir(f'./{folder}')

    @staticmethod
    def get_response(url):
        response = requests.get(url)
        tree = html.fromstring(response.content)
        return tree

    @staticmethod
    def get_match(tree, tag):
        return tree.xpath(tag)

    def get_data(self, bulletin_code):
        self.check_folders()
        tree = self.get_response(self.eop_link[bulletin_code])
        element_date = self.get_match(tree, tag=self.xpath_tags['eop_date'])
        element_link = self.get_match(tree, tag=self.xpath_tags['eop_link'])
        print('date::', element_date[0].text, "::link::", element_link[0])
        data = [element_date, element_link[0]]
        assert None not in data, "Does not match any result in a row"
        response = requests.get(element_link[0])

        # If the request is successful
        if response.status_code == 200:
            # Get the content of the response (the XML file)
            xml_content = response.content

            # Save the XML file to a local file
            with open(os.path.join('./', self.folders[bulletin_code]) + element_link[0].split('/')[-1], "wb") as f:
                f.write(xml_content)

        else:
            print("Failed to download XML file")
        return data


class SolarData():
    def __init__(self):
        self.ftp_adress = "ftp.swpc.noaa.gov"
        self.RSGA_location = "/pub/forecasts/RSGA"
        self.folders = {"rsga": "RSGA"}
        self.days_range = 4
        self.dates = None
        self.ftp = None
        self.ftp_user = "anonymous"
        self.ftp_password = "anonymous"

    def check_folders(self):
        for folder in self.folders.values():
            if not os.path.exists(f'./{folder}'):
                os.mkdir(f'./{folder}')

    def calculate_dates(self):
        dates = []
        for days in range(self.days_range):
            dates.append(datetime.datetime.now() - datetime.timedelta(days=days))
        self.dates = dates

    def get_data(self):

        self.calculate_dates()
        self.check_folders()

        self.ftp = ftplib.FTP(self.ftp_adress)
        self.ftp.login(self.ftp_user, self.ftp_password)

        # Change to the desired directory
        self.ftp.cwd(self.RSGA_location)

        # List all files in the directory
        files = self.ftp.nlst()
        for day in self.dates:
            day_to_download = day.strftime('%m%d')
            new_name = day.strftime('%Y%m%d')
            print(f"{new_name} is being searched")
            for file in files:
                if file.endswith(f'{day_to_download}RSGA.txt'):
                    print(f"{file} :: Available")
                    local_file_path = os.path.join(self.folders["rsga"], f'{new_name}_RSGA.txt')
                    with open(local_file_path, 'wb') as local_file:
                        self.ftp.retrbinary('RETR ' + file, local_file.write)


if __name__ == '__main__':
    # day 1 yesterday
    # 2 days before
    # 3 days before
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', required=False)

    parser.add_argument('-a', '--bulletin-a', action="store_true", required=False)
    parser.add_argument('-b', '--bulletin-b', action="store_true", required=False)
    parser.add_argument('-c', '--bulletin-c', action="store_true", required=False)
    parser.add_argument('-D', '--bulletin-D', action="store_true", required=False)
    parser.add_argument('-R', '--solar-data', action="store_true", required=False)
    parser.add_argument('-I', '--ionex-data', action="store_true", required=False)

    args = parser.parse_args()

    if args.d is None:
        start = 1
        end = 1
    else:
        if ':' in str(args.d):
            start, end = map(int, args.d.split(':'))
        else:  # if only one value is given
            start, end = int(args.d), int(args.d)  # end is not included

    for day_to_look in range(start, end + 1):
        igs_data = CDDIS(day_to_look)

        assert igs_data.check_connection(), "There is no connection!!! Check remote site address!!!"

        igs_data.check_folders()

        igs_data.empty_folder(igs_data.temp, age=0)
        igs_data.empty_folder(igs_data.igu_folder)
        igs_data.empty_folder(igs_data.ionex_folder)

        res = pd.DataFrame(columns=['file_name',
                                    'name_string',
                                    'date_start',
                                    'date_end',
                                    'new_file_name',
                                    'date_to_download',
                                    'day_of_year',
                                    'downloaded_at'])

        print(igs_data.data)

        for file_ in igs_data.gps_info.date_string_array:
            url = f'{igs_data.site[0]}/{igs_data.gps_info.no_weeks}'

            if igs_data.get_file(url, file_[0]):
                print(file_[0], "...Done")
                uncompressed_file = igs_data.uncompress(file_[0])
                igs_data.get_metadata_info(uncompressed_file)
                # res
                print(igs_data.meta_data)
                if igs_data.meta_data['date_start'].date() <= \
                        igs_data.gps_info.timedelta_buffer.date() <= \
                        igs_data.meta_data['date_end'].date():
                    new_file_name = igs_data.rename_file(uncompressed_file)
                    igs_data.compress_new_data(new_file_name)
                    igs_data.meta_data['new_file_name'] = new_file_name
                    igs_data.meta_data['date_to_download'] = igs_data.gps_info.timedelta_buffer.date()
                    igs_data.meta_data['day_of_year'] = igs_data.gps_info.total_days
                    igs_data.meta_data['downloaded_at'] = datetime.datetime.now()
                    # res = res.append(igu_data.meta_data, ignore_index=True)
                    res = pd.concat([res, pd.DataFrame([igs_data.meta_data])], ignore_index=True)
                else:
                    print('Date is not relevant !! check dates !! ')
            else:
                print(file_[0], "...Not Available")
        log_file = os.path.join(igs_data.log_folder, 'log_' + datetime.datetime.today().strftime("%Y%m%d") + '.log')
        res.to_csv(log_file, mode='a', header=not os.path.exists(log_file))

    # Bulletin data download section
    bulletin = BULLETIN()

    for bulletin_arg, bulletin_value in bulletin.bulletins.items():
        if getattr(args, bulletin_arg):
            print(f'Getting the bulletin : {bulletin_value}   data')
            bulletin.get_data(bulletin_value)

    # Solar Data download section
    if args.solar_data:
        print("Solar data is being requested")
        solar = SolarData()
        solar.get_data()

    # Ionosphere  Data download section
    if args.ionex_data:

        for day_ in range(IONOSPHERE_RETRO_DATA):

            ionosphere = CDDIS(day_)
            ionosphere.check_folders()
            print(ionosphere.data)

            ionosphere_data_url = f"{ionosphere.site[0]}/ionex/{ionosphere.gps_info.year}/{ionosphere.gps_info.total_days}"
            ionosphere_data_file = f"IGS0OPSRAP_{ionosphere.gps_info.year}{ionosphere.gps_info.total_days}0000_01D_02H_GIM.INX.gz"

            if ionosphere.get_file(ionosphere_data_url, ionosphere_data_file):
                uncompressed = ionosphere.uncompress(ionosphere_data_file)
                renamed = ionosphere.rename_file(uncompressed, type='ionex')
                ionosphere.get_metadata_info(renamed, type='ionex')

                ionosphere.compress_new_data(renamed, type='ionex')
                print(ionosphere_data_file, "...Done")
            else:
                print(ionosphere_data_file, "...Not Available")
            del ionosphere
