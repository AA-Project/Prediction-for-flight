#!/usr/bin/env python
# coding: utf-8

# In[ ]:


def download(source_blob_name, destination_file_name):
    storage_client = storage.Client()
    bucket_name = 'high-resolution-rapid-refresh'
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)


from google.cloud import storage
import os
import datetime
import pygrib
import pickle
import numpy as np

cur_location = os.getcwd()
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = cur_location + "/splendid-petal-342115-1abb6da43d9d.json"

# setting
idx_cloudtop = 2
idx_VIL = 3
idx_cycle = 49
lat_range = [273, 373]
long_range = [862, 962]

time_zone_diff = 5 #6pm CDT = 11pm UTC

# logfile setting

start_date = datetime.date(2019, 3, 1)
end_date = datetime.date(2019, 10, 1)
time_between_dates = end_date - start_date
days_between_dates = time_between_dates.days

# counter = 0

for i in range(days_between_dates):
    cloudtop = {}
    VIL = {}
    names = []
    
    for loc_hour in range(0,24):
        #cur_hour, setting with corresponding hour and date
        cur_hour = loc_hour + time_zone_diff
        if cur_hour >= 24:
            cur_hour = cur_hour - 24
            current_date = start_date + datetime.timedelta(days= i+1)
        else:
            current_date = start_date + datetime.timedelta(days=i)
            
        str_date = "{}{:0>2}{:0>2}".format(current_date.year, current_date.month, current_date.day)
        file_name = "hrrr.{}/conus/hrrr.t{:0>2}z.wrfsubhf00.grib2".format(str_date, cur_hour)

        try:
            download(file_name, cur_location + "/00.grib2")
        except:
            with open(cur_location + "/hrrrdata/logfile_{}-{}.txt".format(start_date, end_date), 'a') as logfile:
                logfile.write("Not found file: {}".format(file_name))
                logfile.write("\n")
            continue
        grbs = pygrib.open(cur_location + "/00.grib2")
        grb = grbs()[idx_cloudtop - 1]
        mat = grb.values
        mmat = mat[lat_range[0]:lat_range[1], long_range[0]: long_range[1]]
        cloudtop[loc_hour * 100 + 0] = mmat
        names.append(grb.name)

        grb = grbs()[idx_VIL - 1]
        mat = grb.values
        mmat = mat[lat_range[0]:lat_range[1], long_range[0]: long_range[1]]
        VIL[loc_hour * 100 + 0] = mmat
        names.append(grb.name)

        os.remove(cur_location + "/00.grib2")
        ############### 15/30/45
        file_name = "hrrr.{}/conus/hrrr.t{:0>2}z.wrfsubhf01.grib2".format(str_date, cur_hour)
        try:
            download(file_name, cur_location + "/00.grib2")
        except:
            with open(cur_location + "/hrrrdata/logfile_{}-{}.txt".format(start_date, end_date), 'a') as logfile:
                logfile.write("Not found file: {}".format(file_name))
                logfile.write("\n")
            continue

        grbs = pygrib.open(cur_location + "/00.grib2")
        for mul in [0, 1, 2]:
            try:
                grb = grbs()[idx_cloudtop - 1 + mul * idx_cycle]
            except:
                print(cur_hour)
            mat = grb.values
            mmat = mat[lat_range[0]:lat_range[1], long_range[0]: long_range[1]]
            cloudtop[loc_hour * 100 + (mul + 1) * 15] = mmat
            names.append(grb.name)

        for mul in [0, 1, 2]:
            try:
                grb = grbs()[idx_VIL - 1 + mul * idx_cycle]
            except:
                print(cur_hour)
            mat = grb.values
            mmat = mat[lat_range[0]:lat_range[1], long_range[0]: long_range[1]]
            VIL[loc_hour * 100 + (mul + 1) * 15] = mmat
            names.append(grb.name)
        os.remove(cur_location + "/00.grib2")
    
    local_date = start_date + datetime.timedelta(days= i)
    with open(cur_location + '/hrrrdata/hrrr{}.dat'.format(local_date), 'wb') as f:
        pickle.dump([cloudtop, VIL, names], f)

