import requests
import pandas as pd
from bs4 import BeautifulSoup


def main():
    site_url='https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/'
    r=requests.get(site_url)
    soup=BeautifulSoup(r.text, 'html.parser')
    tags=soup.find_all('tr')
    for tag in tags:
        if '2022-02-07 14:03 ' in tag.get_text():
            filename=tag.a['href']
            break
    csv_url=site_url+filename

    df=pd.read_csv(csv_url,header=0)
    value=df['HourlyDryBulbTemperature'].max()
    result_df=df.loc[df['HourlyDryBulbTemperature']==value]
    print('\nResult dataframe :\n',result_df)
    pass

if __name__ == '__main__':
    main()

