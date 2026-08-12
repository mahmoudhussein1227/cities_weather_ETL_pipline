
# this script is to extract weather information from the onpenweather api


import json
import csv
from datetime import datetime
import requests
import pandas as pd



#1st we must get the long , lat and city name from geocoding-api
def get_city_weather(city:str , count:int , country : str) -> dict : 
    url = f"https://geocoding-api.open-meteo.com/v1/search?name={city}&count={count}&language=en&format=json"

    if(requests.get(url=url).status_code != 200):
        return None
    elif('results' not in requests.get(url=url).json().keys()):
        return None

    geocoding_api_resp = requests.get(url=url).json()

    api_country = geocoding_api_resp['results'][0]['country']

    if(country != api_country.lower()):
        print(f"The city : {city} you look for does not belong to this country : {country}")
        return None
    else:
        latitude  = geocoding_api_resp['results'][0]['latitude']
        longitude = geocoding_api_resp['results'][0]['longitude']

        # 2nd : we need to get the weather using the info above

        url = f"https://api.open-meteo.com/v1/forecast?latitude={latitude}&longitude={longitude}&daily=weather_code,temperature_2m_min,temperature_2m_max&forecast_days=1"
        weather_info = requests.get(url=url).json()
        time     = weather_info['daily']['time'][0]
        max_temp = weather_info['daily']['temperature_2m_max'][0]
        min_temp = weather_info['daily']['temperature_2m_min'][0]

        weather_data = {
            'city'     : city,
            'country'  : api_country,
            'time'     : time,
            'max_temp' : max_temp,
            'min_temp' : min_temp,
        }

        return weather_data

def run(file : str , country :str , dest_file:str):
    with open(file , 'r') as f:
        cities = [city.strip() for city in f.readlines()]
        weather_data =  []
        for city in cities:
            city_info = get_city_weather(city=city , count=1 , country=country)
            if city_info is None:
                continue
            weather_data.append(city_info)
            
        df = pd.DataFrame(weather_data)

        df.to_csv(dest_file , index=False )

        

        

