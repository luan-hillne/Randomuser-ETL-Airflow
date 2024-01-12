import requests
import json
import time
import pandas as pd
import re
from kafka import KafkaProducer
import logging
# Creates the results JSON from the random user API call
def get_data():
    # Request nationality US
    url = "https://randomuser.me/api/?results=1&nat=US"
    
    response = requests.get(url)
    data = response.json()
    results = data["results"]
    return results

data = get_data()
# print(data)

def return_dataframe(data):
    if len(data) == 0:
        print("No random user exist...")
        return None
    
    #user_id = []
    first_name = []
    last_name = []
    gender = []
    dateofbirth = []
    street_name = []
    city = []
    latitude = []
    longtitude = []
    email = []
    
    for results in data:
        #user_id.append(results["info"]["seed"][0])
        first_name.append(results["name"]["first"])
        last_name.append(results["name"]["last"])
        gender.append(results["gender"])
        dateofbirth.append(results["dob"]["date"])
        street_name.append(results["location"]["street"]["name"])
        city.append(results["location"]["city"])
        latitude.append(results["location"]["coordinates"]["latitude"])
        longtitude.append(results["location"]["coordinates"]["longitude"])
        email.append(results["email"])

    results_dict = {
        "first_name": first_name,
        "last_name": last_name,
        "gender": gender,
        "dateofbirth": dateofbirth,
        "street_name": street_name,
        "city": city,
        "latitude": latitude,
        "longtitude": longtitude,
        "email": email
    }

    results_df = pd.DataFrame(results_dict, columns = ['first_name','last_name','gender','dateofbirth',
                                                    'street_name', 'city','latitude','longtitude', 'email'])
    return results_df
# rel = return_dataframe(data)
# print(rel)

# Set of Data Quality Checks Needed to Perform Before Loading
def Data_Quality(kafka_data):
    # Checking Whether the DataFrame is empty
    if kafka_data.empty:
        print('No Songs Extracted')
        return False

    # Enforcing Primary keys since we don't need duplicates
    if pd.Series(kafka_data['full_name']).is_unique:
        pass
    else:
        # The Reason for using an exception is to immediately terminate the program and avoid further processing
        raise Exception("Primary Key Exception, Data Might Contain duplicates")

    # Checking for Nulls in our data frame
    if kafka_data.isnull().values.any():
        raise Exception("Null values found")

def remove_parentheses(text):
    return re.sub(r'\([^)]*\)', '', text).strip()

def Transform_df(results):
    results['full_name'] = results['first_name'] + ' ' + results['last_name']
    results['dateofbirth'] = results['dateofbirth'].apply(lambda x: x[:x.find('T')])
    results['latitude'] = results['latitude'].str[:5].astype(float)
    results['longtitude'] = results['longtitude'].str[:5].astype(float)
    results.rename(columns={'street_name': 'address'}, inplace=True)

    transform_df = results[['full_name','gender','dateofbirth','address', 'city','latitude','longtitude', 'email']]
    json_df = transform_df.to_json()
    return json_df 
# extract = return_dataframe(data)
# res = Transform_df(extract)
# print(res)
    
# def create_kafka_producer():
#     #C reates the Kafka producer object
#     return KafkaProducer(bootstrap_servers=['kafka1:19092', 'kafka2:19093', 'kafka3:19094'])


def start_streaming():
    # Writes the API data every 10 seconds to Kafka topic random_names

    producer = KafkaProducer(bootstrap_servers = ['broker:29092'], max_block_ms=5000)
    cur_time = time.time()

    while True: 
        if time.time() > cur_time + 60: # 1 min
            break
        try: 
            data = get_data()
            extract = return_dataframe(data)
            kafka_data = Transform_df(extract)

            producer.send('users_created', json.dumps(kafka_data).encode('utf-8'))
            time.sleep(10)
        except Exception as e: 
            logging.error(f'An error occured: {e}')
            continue


if __name__ == "__main__":
    start_streaming()
    
