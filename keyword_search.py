from google.cloud import storage
import os
import random
import datetime
import pygrib

cur_location = os.getcwd()
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=cur_location+"/splendid-petal-342115-1abb6da43d9d.json"

def random_date():
    start_date = datetime.date(2014, 9, 18)
    end_date = datetime.date(2021, 1, 1)

    time_between_dates = end_date - start_date
    days_between_dates = time_between_dates.days
    random_number_of_days = random.randrange(days_between_dates)
    random_date = start_date + datetime.timedelta(days=random_number_of_days)
    return "{}{:0>2}{:0>2}".format(random_date.year, random_date.month, random_date.day)


def download(source_blob_name, destination_file_name):
    storage_client = storage.Client()
    bucket_name = 'high-resolution-rapid-refresh'
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)
    # print('Blob {} downloaded to {}.'.format(source_blob_name, destination_file_name))


storage_client = storage.Client()
tot = 1
s = set()

output = open(cur_location + "/result.txt", "w")
for i in range(tot):
    date = random_date()
    files_list = []
    for blob in storage_client.list_blobs('high-resolution-rapid-refresh', prefix='hrrr.' + date + '/conus'):
        files_list += [blob.name]

    file = random.choice(files_list)
    while not file.endswith(".grib2"):
        file = random.choice(files_list)
    # print(file)
    output.write(file + '\n')

    download(file, cur_location + "/00.grib2")

    grbs = pygrib.open(cur_location + "/00.grib2")

    for grb in grbs():
        s.add(str(grb).split(":")[1])
    os.remove(cur_location + "/00.grib2")

output.write("##########Keywords##########\n")

for element in s:
    output.write(element + '\n')
output.close()