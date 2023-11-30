from abc import ABC, abstractmethod
from datetime import datetime
import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()

class WeatherBuilder(ABC):
    def __init__(self):
        super.__init__()
    
    @abstractmethod
    def __select_url(cls, category):
        pass
    
    @abstractmethod
    def __build_record(self, record):
        pass
    
    @abstractmethod
    def __build_coords(vals):
        pass
    
    @abstractmethod
    def __build_weather(cls, vals):
        pass
    
    @abstractmethod
    def __build_sys(cls, vals):
        pass

    @abstractmethod
    def __build_date(vals, key):
        pass

    @abstractmethod
    def __process_records(self, resp):
        pass
    
    @abstractmethod
    def __request_data(self):
        pass

    @abstractmethod
    def process(self):
        pass

    @abstractmethod
    def request_data(self):
        pass

class IQAirBuilder(WeatherBuilder):
    _icon_url_template = 'https://airvisual.com/images/{icon}.png'
    _countries_url = 'http://api.airvisual.com/v2/countries?key={key}'
    _states_url = 'http://api.airvisual.com/v2/states?country={country}&key={key}'
    _cities_url = 'http://api.airvisual.com/v2/cities?state={state}&country={country}&key={key}'
    _data_url = 'http://api.airvisual.com/v2/city?city={city}&state={state}&country={country}&key={key}'
    _key = os.environ['IQ_AIR_KEY']
    
    def __init__(self, category):
        self.response = None
        self.url = self.__select_url(category)
    
    def process(self, resp_data=None):
        _data = self.response.text if self.response is not None else resp_data
        assert _data is not None, 'Response data must be provided.'
        data = json.loads(_data)
        self._s_data = self.__build_sys(data)
        # self.proc_data = self.__process_records(data)
        return self.__process_records(data)
    
    @classmethod
    def __select_url(cls, category):
        match category:
            case 'countries':
                return cls._countries_url
            case 'states':
                return cls._states_url
            case 'cities':
                return cls._cities_url
            case 'data':
                return cls._data_url
            case _:
                raise Exception('Category not set. Please set category.')
    
    def __build_record(self, record):
        return {
            'weather': self.__build_weather(record.get('current', None)),
            'pollution': self.__build_pollution(record.get('current', None)),
            'sys': self._s_data
        }

    @staticmethod
    def __build_coords(vals):
        coords = vals.get('coordinates', None)
        if coords is not None:
            return {'lat': coords[0], 'lon': coords[-1]}
        else:
            return {'lat': None, 'lon': None}
        
    @classmethod
    def __build_pollution(cls, vals):
        pollution = vals.get('pollution', None)
        if pollution is not None:
            return {
                'pollution_ts': cls.__build_date(pollution, 'ts'),
                'aqi_us_epa': pollution.get('aqius', None), 
                'aqi_china_mep': pollution.get('aqicn', None), 
                'main_pollutant_us': pollution.get('mainus', None),
                'main_pollutant_cn': pollution.get('maincn', None)
            }
        else:
            return {'lat': None, 'lon': None}

    @classmethod
    def __build_weather(cls, vals):
        weather = vals.get('weather', None)
        if weather is not None:
            return {
                'weather_ts': cls.__build_date(weather, 'ts'),
                'temp_c': weather.get('tp'),
                'pressure_hpa': weather.get('pr', None),
                'humidity_perc': weather.get('hu', None),
                'wind_speed_m_s': weather.get('ws', None),
                'wind_direction_deg': weather.get('wd', None),
                'icon': cls._icon_url_template.format(icon=weather['ic'])
            }
        
    @classmethod
    def __build_sys(cls, vals):
        if vals is not None:
            return {
                'country': vals.get('country', None),
                'state': vals.get('state', None),
                'city': vals.get('city', None),
                'coord': cls.__build_coords(vals.get('location', None))
            }
        
    @staticmethod
    def __build_date(vals, key):
        date_val = vals.get(key, None)
        match date_val:
            case int():
                return datetime.fromtimestamp(date_val)
            case str():
                date_val_2 = date_val.split('.')[0].replace('T', ' ')
                return datetime.strptime(date_val_2, '%Y-%m-%d %H:%M:%S')
            case _:
                return date_val
                    
    def __process_records(self, resp):
        tmp = resp.get('list', None)
        if tmp is not None:
            return [self.__build_record(data) for data in tmp]
        else:
            return self.__build_record(resp)
    
    def request_data(self, headers={}, payload={}, files={}, **url_params):
        _url = self.url.format(key=self._key, **url_params)
        self.response = requests.request("GET", _url, headers=headers, data=payload, files=files)
        
class OpenWeatherBuilder(WeatherBuilder):
    _icon_url_template = 'https://openweathermap.org/img/wn/{icon}@2x.png'
    _geo_url = 'http://api.openweathermap.org/geo/1.0/direct?q={city}&limit={limit}&appid={key}'
    _forecast_url = 'https://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&appid={key}&units=metric'
    _current_url = 'https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={key}&units=metric'
    _key = os.environ['OPEN_WEATHER_KEY']
    
    def __init__(self, category):
        self.response = None
        self.category = category
        self.url = self.__select_url(category)
        
    def process(self, resp_data=None, geoloc_idx=0):
        _data = self.response.text if self.response is not None else resp_data
        assert _data is not None, 'Response data must be provided.'
        data = json.loads(_data)
        
        if self.category == 'geoloc':
            self.proc_data = self.__process_records(data)
            _res = self.proc_data[geoloc_idx]
            self.selected_lat = _res['coord']['lat']
            self.selected_lon = _res['coord']['lon']
        elif self.category in ['forecast', 'current']:
            self._tz_shift = data.get('timezone', None)
            self._s_data = self.__build_sys(data)
            self.proc_data = self.__process_records(data)
        else:
            raise Exception('Category selected does not have an allocated process')
        
    
    @staticmethod
    def __build_local_names(cls, record):
        _local_names = record.get('local_names', None)
        return {
            'en': None if _local_names is None else _local_names.get('en', None), 
            'zh': None if _local_names is None else _local_names.get('zh', None)
        }

    def __build_record(self, record):
        if self.category == 'geoloc':
            return {
                'name': record.get('name', None),
                'country': record.get('country', None),
                'local_names': self.__build_local_names(record),
                'state': record.get('state', None),
                'coord': self.__build_coords(record)
            }        
        else:
            return {
                'main': self.__build_main(record['main']),
                # 'coord': self.__build_coords(record.get('coords', None)),
                'visibility_metres': record['visibility'],
                'weather': self.__build_weather(record['weather']),
                'wind': self.__build_wind(record['wind']),
                'cloudiness_perc': self.__build_clouds(record['clouds']),
                'wind': self.__build_snow_rain(record.get('wind', None)),
                'rain': self.__build_snow_rain(record.get('rain', None)),
                'sys': self._s_data,
                'date': self.__build_date(record, 'dt'),
                'forecasted_date': self.__build_date(record, 'dt_txt'),
                'timezone_shift_s': self._tz_shift
            }
        
    @classmethod
    def __select_url(cls, category):
        match category:
            case 'forecast':
                return cls._forecast_url
            case 'current':
                return cls._current_url
            case 'geoloc':
                return cls._geo_url
            case _:
                raise Exception('Category not set. Please set category.')
    
    @staticmethod
    def __build_coords(vals):
        return {
            'lat': vals['lat'] if vals is not None else None,
            'lon': vals['lon'] if vals is not None else None
        }
        
    @staticmethod
    def __build_main(vals):
        return {
          'temp_c': vals['temp'],
          'feels_like_c': vals['feels_like'],
          'temp_min_c': vals['temp_min'],
          'temp_max_c': vals['temp_max'],
          'pressure_hpa': vals['pressure'],
          'humidity_perc': vals['humidity'],
          'sea_level_hpa': vals.get('sea_level', None),
          'grnd_level_hpa': vals.get('sea_level', None)
        }
    
    @classmethod
    def __build_weather(cls, vals):
        weather_data = []
        for wd in vals:
            tmp_wd = {
                'main': wd['main'],
                'description': wd['description'],
                'icon': cls._icon_url_template.format(icon=wd['icon'])
            }
            weather_data.append(tmp_wd)
        return weather_data
    
    @staticmethod
    def __build_clouds(vals):
        return vals['all']
    
    @staticmethod
    def __build_wind(vals):
        return {
            'speed_m_s': vals['speed'],
            'deg': vals['deg'],
            'gust_m_s': vals.get('gust', None)
        }
    
    @staticmethod
    def __build_snow_rain(vals):
        return {
            '1h_mm': vals.get('1h', None) if vals is not None else None, 
            '3h_mm': vals.get('3h', None) if vals is not None else None
        }
    
    @staticmethod
    def __build_date(vals, key):
        date_val = vals.get(key, None)
        match date_val:
            case int():
                return datetime.fromtimestamp(date_val)
            case str():
                return datetime.strptime(date_val, '%Y-%m-%d %H:%M:%S')
            case _:
                return date_val
    
    @classmethod
    def __build_sys(cls, vals):
        if vals.get('sys', None) is None:
            tmp_vals = vals['city']
        else:
            tmp_vals = vals['sys']

        return {
            'country': tmp_vals.get('country', None),
            'sunrise': cls.__build_date(tmp_vals, 'sunrise'),
            'sunset': cls.__build_date(tmp_vals, 'sunset'),
            'part_of_day': tmp_vals.get('pod', None),
            'coord': cls.__build_coords(tmp_vals.get('coords', None))
        }
    
    def __process_records(self, resp):
        _check_dict = isinstance(resp, dict)
        _check_list = isinstance(resp, list)
        
        _data = resp if _check_list else None
        if _check_dict:
            _data = resp.get('list', None)
        
        if _data is not None:
            return [self.__build_record(data) for data in _data]
        else:
            return self.__build_record(resp)
        
    def request_data(self, headers={}, payload={}, files={}, **url_params):
        _url = self.url.format(key=self._key, **url_params)
        self.response = requests.request("GET", _url, headers=headers, data=payload, files=files)