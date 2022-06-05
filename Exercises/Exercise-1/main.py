import requests
import os
from zipfile import ZipFile
import shutil

download_uris = [
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip'
]


def main():
    # your code here
    cwd = os.getcwd()
    new_directory='downloads'
    save_path=os.path.join(cwd,new_directory)
    try:
        os.mkdir(save_path)
    except:
        print('directory already exists')

    def download_url(url, save_path, chunk_size=256):
        r = requests.get(url, stream=True)
        lfilename=url.split('/')[-1]
        zfile_path=os.path.join(save_path,lfilename)
        with open(zfile_path, 'wb') as fd:
            for chunk in r.iter_content(chunk_size=chunk_size):
                fd.write(chunk)
    def extract_files(zfile_path):
        with ZipFile(zfile_path) as file:
            for filename in file.namelist():
                if not str(filename).startswith('__MACOSX/'):
                    file.extract(filename, save_path)
        os.remove(zfile_path)

    for url in download_uris:
        try:
            download_url(url,save_path)
        except:
            print(url,' cannot be downloaded')
    for lfilename in os.listdir(save_path):
        zfile_path = os.path.join(save_path, lfilename)
        try:
            extract_files(zfile_path)
        except:
            print(lfilename, 'cannot be extracted or already exists')
            os.remove(zfile_path)

if __name__ == '__main__':
    main()
