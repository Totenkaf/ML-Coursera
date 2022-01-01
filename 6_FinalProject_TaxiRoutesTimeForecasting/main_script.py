# Main system library importing
import os

# Main modelling libraries importing
import numpy as np
import pandas as pd
import datetime
import urllib3

# Main statistical library importing
from scipy import stats as sts

# Main visual library importing
# Standard visualize
import matplotlib.image as pltimg
from matplotlib import pylab as plt

# Dynamical visualize importing
from chart_studio import plotly as py
import plotly.graph_objs as go
from plotly.offline import iplot

os.chdir(r"~/ML_coursera/MIPT_YANDEX_FINAL_TAXI")

# Получение списка количества поездок по координатам.
def get_trips(x, y):
    west = -74.25559 
    east = -73.70001
    south = 40.49612
    north = 40.91553
    counts = sts.binned_statistic_2d(x, y, None, statistic='count', bins=50, 
                        range=[[west, east], [south, north]])
    return counts.statistic.ravel()

# Получение списка часов в месяце.
def get_hours(date_string):
    now_date = datetime.datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S')
    now_date = now_date.replace(day=1, hour=0, minute=0, second=0)
    target_month = now_date.month
    now_month = now_date.month
    res_list = []
    while now_month == target_month:
        res_list.append(now_date.strftime('%Y-%m-%d %H:%M:%S'))
        now_date = now_date + datetime.timedelta(hours=1)
        now_month = now_date.month
    return res_list

# Считаем поездки. Модифицирует датафрейм.
def count_trips(count_data, clean_filename, chunksize=1e6):
    
    for chunk in pd.read_csv(clean_filename, sep=',', header=0, chunksize=chunksize):
        
        # Добавляем столбцы часов в датафрейм.
        if count_data.shape[1] < 10:
            hours = get_hours(chunk['tpep_pickup_datetime'][0])
            for hour in hours:
                values = np.zeros((count_data.shape[0],))
                count_data[hour] = values
        
        for hour in chunk['tpep_pickup_datetime'].unique():
            h_chunk = chunk[chunk['tpep_pickup_datetime'] == hour]
            x = np.array(h_chunk['pickup_longitude'])
            y = np.array(h_chunk['pickup_latitude'])
            count_data[hour] = np.array(count_data[hour]) + get_trips(x, y)


regions_filename = os.path.join(os.path.dirname(os.path.abspath('file')), 'regions.csv')
regions = pd.read_csv(regions_filename, sep=';', header=0)
regions.head()


clean_files = os.listdir(dir_with_clean_data)
dir_with_count_data = os.path.join(os.path.dirname(os.path.abspath('file')), 
                             'count_data')
clean_files.sort()


# Считаем, записываем.
CHUNKSIZE = 1e6

for file in clean_files:
    print(file+'...', end='')
    count_data = regions.copy(deep=True)
    clean_filename = os.path.join(dir_with_clean_data, file)
    count_filename = os.path.join(dir_with_count_data, 'count_' + file)
    
    #Если файл существует, удаляем его.
    if os.path.exists(count_filename):
        os.remove(count_filename)
    
    num_rows = count_trips(count_data, clean_filename, CHUNKSIZE)
    count_data.to_csv(count_filename, sep=',', header=True)
    print('DONE')