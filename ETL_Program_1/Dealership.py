# ETL on used Cars Data
import glob                         # this module helps in selecting files 
import pandas as pd                 # this module helps in processing CSV files
import xml.etree.ElementTree as ET  # this module helps in processing XML files.
from datetime import datetime

from pandas.core.frame import DataFrame

targetfile= 'targetfile.csv'
##loggingfile= 'log.txt'

#### defining functions for csv, json, xml file
def csv_ext(csv_file):
    dataframe= pd.read_csv(csv_file)
    return dataframe

def json_ext(json_file):
    dataframe= pd.read_json(json_file, lines=True)
    return dataframe  

def xml_ext(xml_file):
    dataframe= pd.DataFrame(columns= ['car_model', 'year_of_manufacture', 'price', 'fuel'])
    d= ET.parse(xml_file)
    r= d.getroot()
    for item in r:
        car_model= item.find('car_model').text
        year_of_manufacture= item.find('year_of_manufacture').text
        price= float(item.find('price').text)
        fuel= item.find('fuel').text
        dataframe.append({'car_model': car_model, 'year_of_manufacture': year_of_manufacture , 'price': price, 'fuel': fuel}, ignore_index=True )

    return dataframe    

### defining extraction function 

def xtract():
    extracted_data= pd.DataFrame(columns= ['car_model', 'year_of_manufacture', 'price', 'fuel'])
    for c in glob.glob('*.csv'):
        extracted_data= extracted_data.append(csv_ext(c), ignore_index=True)

    for j in glob.glob('*.json'):
        extracted_data= extracted_data.append(json_ext(j), ignore_index=True)

    for x in glob.glob('*.xml'):       
        extracted_data= extracted_data.append(xml_ext(x), ignore_index=True)

    return extracted_data    

### transformation of data

def transform(data):
    data['price']= round(data.price, 2)
    return data

### loading of data
def loading(targetfile, data):
    data.to_csv(targetfile)    

def logging(msg):
    timestamp_fmt= '%Y-%h-%d-%H:%M:%S'
    now= datetime.now()
    timestamp= now.strftime(timestamp_fmt)
    with open('log.txt', 'a') as f:
        f.write(timestamp + ',' + msg + '\n')


logging('ETL process started')
# print(logfile)


# Log that you have started the Extract step
logging('Extraction')

# Call the Extract function
dff= xtract()

# Log that you have completed the Extract step
logging('Extraction done')

# Log that you have started the Transform step
logging('Tranformation is started')

# Call the Transform function
dff1= transform(dff)

# Log that you have completed the Transform step
logging('Tranformation is done')


# Log that you have started the Load step
logging('loading is started')

# Call the Load function
loading(targetfile, dff1)


# Log that you have completed the Load step
logging('loading is done')


# Log that you have completed the ETL process
logging('ETL process is done')



