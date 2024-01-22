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
                        self.ionex_folder]

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

    def empty_folder(self, folder, age=0):

        threshold = datetime.datetime.now() - datetime.timedelta(days=age)
        for file in os.listdir(folder):
            file_path = os.path.join(folder, file)
            try:

                if datetime.datetime.fromtimestamp(os.stat(file_path).st_mtime) <= threshold:
                    if os.path.isfile(file_path):
                        os.unlink(file_path)

            except Exception as e:

                print(e)

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

    def rename_file(self, filename, type='igu'):
        if type == 'igu':
            new_name = f"igu{self.meta_data['name_string']}.sp3"
        elif type == 'ionex':
            new_name = f"igrg{self.gps_info.total_days}0.{str(self.gps_info.year)[2:]}i"
        os.rename(filename, new_name)
        # shutil.move(filename, self.temp)
        return new_name

    def compress_new_data(self, filename, type='igu'):
        if type == 'igu':
            fname = os.path.join(self.igu_folder, filename + ".Z")
        elif type == 'ionex':
            fname = os.path.join(self.ionex_folder, filename + ".Z")

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


if __name__ == '__main__':
    # day 1 yesterday
    # 2 days before
    # 3 days before
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', type=int, required=False)
    parser.add_argument('-a', '--bulletin-a', action="store_true", required=False)
    parser.add_argument('-b', '--bulletin-b', action="store_true", required=False)
    parser.add_argument('-c', '--bulletin-c', action="store_true", required=False)
    parser.add_argument('-D', '--bulletin-D', action="store_true", required=False)
    args = parser.parse_args()
    if args.d is None:
        day_to_look = 1
    else:
        day_to_look = args.d

    igs_data = CDDIS(day_to_look)

    assert igs_data.check_connection(), "There is no connection!!! Check remote site address!!!"

    igs_data.check_folders()

    igs_data.empty_folder(igs_data.temp)
    igs_data.empty_folder(igs_data.igu_folder, age=7)
    igs_data.empty_folder(igs_data.ionex_folder, age=7)

    res = pd.DataFrame(columns=['file_name',
                                'name_string',
                                'date_start',
                                'date_end',
                                'new_file_name',
                                'date_to_download',
                                'day_of_year',
                                'downloaded_at'])

    print(igs_data.data)

    ionex_data_url = ionex_data_url = f"{igs_data.site[0]}/ionex/{igs_data.gps_info.year}/{igs_data.gps_info.total_days}"
    ionex_data_file = f"IGS0OPSRAP_{igs_data.gps_info.year}{igs_data.gps_info.total_days}0000_01D_02H_GIM.INX.gz"

    if igs_data.get_file(ionex_data_url, ionex_data_file):
        uncompressed = igs_data.uncompress(ionex_data_file)
        renamed = igs_data.rename_file(uncompressed, type='ionex')
        igs_data.compress_new_data(renamed, type='ionex')

        print(ionex_data_file, "...Done")
    else:
        print(ionex_data_file, "...Not Available")

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
    bul = BULLETIN()
    if args.bulletin_a:
        bul.get_data('a')
    if args.bulletin_b:
        bul.get_data('b')
    if args.bulletin_c:
        bul.get_data('c')
    if args.bulletin_D:
        bul.get_data('D')
