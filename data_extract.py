#only needs to be run once for now
import requests
import os

def download_file(url, destination):
    response = requests.get(url, stream=True)
    with open(destination, 'wb') as file:
        for chunk in response.iter_content(chunk_size=8192):
            file.write(chunk)
url = 'https://raw.githubusercontent.com/darshilparmar/uber-etl-pipeline-data-engineering-project/main/data/uber_data.csv'
destination = 'data/tlc_data.csv'
download_file(url,destination)



#todo: create function to download data directly from tlc website and store it as csv