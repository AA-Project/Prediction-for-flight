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

cur_location = os.getcwd()
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = cur_location + "/splendid-petal-342115-1abb6da43d9d.json"

# setting
idx_cloudtop = 2
idx_VIL = 3
idx_cycle = 43
delta_time = 60  # currently only dealing with either 30 mins or 60 mins prediction
lat_range = [413, 520]
long_range = [860, 965]

# logfile setting

start_date = datetime.date(2020, 3, 1)
end_date = datetime.date(2020, 10, 1)
time_between_dates = end_date - start_date
days_between_dates = time_between_dates.days

output = {}
counter = 0

print('output_{}-{}_predict_in_{}mins.dat'.format(start_date,end_date,60))
for i in range(days_between_dates):
    current_date = start_date + datetime.timedelta(days=i)
    str_date = "{}{:0>2}{:0>2}".format(current_date.year, current_date.month, current_date.day)
    # real result
    for cur_hour in range(0):
        file_name = "hrrr.{}/conus/hrrr.t{:0>2}z.wrfsubhf00.grib2".format(str_date, cur_hour)
        try:
            download(file_name, cur_location + "/00.grib2")
        except:
            with open("logfile_{}-{}".format(start_date, end_date), 'r') as logfile:
                logfile.write("Not found file: {}".format(file_name))
                logfile.write("\n")
            continue
        grbs = pygrib.open(cur_location + "/00.grib2")
        grb = grbs()[idx_cloudtop - 1]
        mat = grb.values
        mmat = mat[lat_range[0]:lat_range[1], long_range[0]: long_range[1]]
        output["{}_{:0>2}_cloudtop_real".format(str_date, cur_hour)] = mmat

        grb = grbs()[idx_VIL - 1]
        mat = grb.values
        mmat = mat[lat_range[0]:lat_range[1], long_range[0]: long_range[1]]
        output["{}_{:0>2}_VIL_real".format(str_date, cur_hour)] = mmat
        os.remove(cur_location + "/00.grib2")

    # predict result
    multiple = delta_time // 15 - 1
    for cur_hour in range(24):
        file_name = "hrrr.{}/conus/hrrr.t{:0>2}z.wrfsubhf01.grib2".format(str_date, cur_hour)
        try:
            download(file_name, cur_location + "/00.grib2")
        except:
            with open("logfile_{}-{}".format(start_date, end_date), 'r') as logfile:
                logfile.write("Not found file: {}".format(file_name))
                logfile.write("\n")
            continue
        grbs = pygrib.open(cur_location + "/00.grib2")
        grb = grbs()[idx_cloudtop - 1 + multiple * idx_cycle]
        mat = grb.values
        mmat = mat[lat_range[0]:lat_range[1], long_range[0]: long_range[1]]
        output["{}_{:0>2}_cloudtop_predict".format(str_date, cur_hour)] = mmat

        grb = grbs()[idx_VIL - 1 + multiple * idx_cycle]
        mat = grb.values
        mmat = mat[lat_range[0]:lat_range[1], long_range[0]: long_range[1]]
        output["{}_{:0>2}_VIL_predict".format(str_date, cur_hour)] = mmat
        os.remove(cur_location + "/00.grib2")
        counter +=1
if counter % 30 == 0:
    with open('output_{}-{}_predict_in_{}mins.dat'.format(start_date,end_date,delta_time), 'wb') as f:
        pickle.dump(output, f)
print('output_{}-{}_predict_in_{}mins.dat'.format(start_date,end_date,delta_time))