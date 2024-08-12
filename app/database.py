import json


def insert_weather(cursor, date_downloads, time_downloads, r):
    cursor.execute(
        "INSERT INTO public.weather (date_downloads,time_downloads,coord,weather,base,main,visibility,wind,clouds,dt,sys,timezone,name)"
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (date_downloads, time_downloads,
         json.dumps(r["coord"]), json.dumps(r["weather"]), r["base"], json.dumps(r["main"]), r["visibility"],
         json.dumps(r["wind"]), json.dumps(r["clouds"]), r["dt"], json.dumps(r["sys"]), r["timezone"], r["name"]))
    count_weather = cursor.rowcount
    return count_weather


def insert_city_name(cursor):
    cursor.execute("""
        INSERT INTO public.city_name (weather_id, city)
        SELECT  w.id, w.name
		FROM public.weather w
		WHERE w.id > (select max(weather_id) FROM public.city_name)
		""")
    count_city_name = cursor.rowcount
    return count_city_name


def insert_date_time_downloads(cursor):
    cursor.execute("""
        INSERT INTO public.date_time_downloads (weather_id, date_downloads, time_downloads)
		SELECT  w.id, w.date_downloads, w.time_downloads
		FROM public.weather w
		WHERE w.id > (select max(weather_id) FROM public.date_time_downloads)
		""")
    count_date_time_downloads = cursor.rowcount
    return count_date_time_downloads


def insert_sun_light(cursor):
    cursor.execute("""
        INSERT INTO public.sun_light (weather_id, sunrise, sunset)
		SELECT  w.id,
				(select * from json_to_record(w.sys) as x(sunrise int)) as sunrise,
				(select * from json_to_record(w.sys) as x(sunset int)) as sunset
		FROM public.weather w
		where w.id > (select max(weather_id) from public.sun_light)
		""")
    count_sun_light = cursor.rowcount
    return count_sun_light


def insert_weather_temperature_params(cursor):
    cursor.execute("""
        INSERT INTO public.weather_temperature_params (weather_id, temperature, feels_like, pressure, humidity)
		SELECT  w.id,
				(select * from json_to_record(w.main) as x(temp float)) as temperature,
				(select * from json_to_record(w.main) as x(feels_like float)) as feels_like,
				(select * from json_to_record(w.main) as x(pressure int)) as pressure,
				(select * from json_to_record(w.main) as x(humidity int)) as humidity
		FROM public.weather w
		where w.id > (select max(weather_id) from public.weather_temperature_params)
		""")
    count_weather_temperature_params = cursor.rowcount
    return count_weather_temperature_params


def insert_weather_wind_clouds_params(cursor):
    cursor.execute("""
        INSERT INTO public.weather_wind_clouds_params (weather_id, wind_speed, wind_deg, weather_parameters, description, clouds)
		SELECT  w.id,
				(select * from json_to_record(w.wind) as x(speed int)) as speed,
				(select * from json_to_record(w.wind) as x(deg int)) as deg,
				(select * from json_to_recordset(w.weather) as x(main text)) as weather_parameters,
				(select * from json_to_recordset(w.weather) as x(description text)) as description,
				(select * from json_to_record(w.clouds) as x("all" int)) as clouds
		FROM public.weather w
		where w.id > (select max(weather_id) from public.weather_wind_clouds_params)
		""")
    count_weather_wind_clouds_params = cursor.rowcount
    return count_weather_wind_clouds_params
