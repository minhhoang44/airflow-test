import json
from datetime import datetime, UTC,timedelta
import pandas as pd
import requests

city_name = "Hanoi"
base_url = "https://api.openweathermap.org/data/2.5/weather?q="

with open("credentials.txt", 'r') as f:
    api_key = f.read()

full_url = base_url + city_name + "&APPID=" + api_key

def kelvin_to_celsius(temp_in_kelvin):
    temp_in_celsius = (temp_in_kelvin - 273.15)
    return temp_in_celsius

def etl_weather_data(url):
    r = requests.get(url)
    data = r.json()
    # print(data)


    city = data["name"]
    weather_description = data["weather"][0]['description']
    temp = kelvin_to_celsius(data["main"]["temp"])
    feels_like= kelvin_to_celsius(data["main"]["feels_like"])
    min_temp = kelvin_to_celsius(data["main"]["temp_min"])
    max_temp = kelvin_to_celsius(data["main"]["temp_max"])
    pressure = data["main"]["pressure"]
    humidity = data["main"]["humidity"]
    wind_speed = data["wind"]["speed"]
    timezone_offset = timedelta(seconds=data['timezone'])
    time_of_record = datetime.fromtimestamp(data['dt'], UTC) + timezone_offset
    sunrise_time = datetime.fromtimestamp(data['sys']['sunrise'], UTC) + timezone_offset
    sunset_time = datetime.fromtimestamp(data['sys']['sunset'], UTC) + timezone_offset


    transformed_data = {"City": city,
                        "Description": weather_description,
                        "Temperature (C)": temp,
                        "Feels Like (C)": feels_like,
                        "Minimun Temp (C)":min_temp,
                        "Maximum Temp (C)": max_temp,
                        "Pressure": pressure,
                        "Humidty": humidity,
                        "Wind Speed": wind_speed,
                        "Time of Record": time_of_record,
                        "Sunrise (Local Time)":sunrise_time,
                        "Sunset (Local Time)": sunset_time                        
                        }

    transformed_data_list = [transformed_data]
    df_data = pd.DataFrame(transformed_data_list)
    # print(df_data)

    df_data.to_csv("current_weather_data_hanoi.csv", index = False)

if __name__ == '__main__':
    etl_weather_data(full_url)
