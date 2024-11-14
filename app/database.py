import json


def insert_weather(cursor, date_downloads, time_downloads, req_json):
    cursor.execute(
        """insert into dwh.weather (date_downloads,time_downloads,coord,weather,base,main,visibility,wind,clouds,
        dt,sys,timezone,name) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
        (date_downloads, time_downloads,
         json.dumps(req_json["coord"]), json.dumps(req_json["weather"]), req_json["base"], json.dumps(req_json["main"]),
         req_json["visibility"], json.dumps(req_json["wind"]), json.dumps(req_json["clouds"]), req_json["dt"],
         json.dumps(req_json["sys"]), req_json["timezone"], req_json["name"]))
    count_weather = cursor.rowcount
    return count_weather


def insert_dim_coordinates(cursor):
    cursor.execute("""
    insert into dwh.dim_coordinates (longitude, latitude)
    select src.longitude, src.latitude
    from (select (select * from json_to_record(w.coord) as x(lon real)) as longitude,
        (select * from json_to_record(w.coord) as x(lat real)) as latitude
        from dwh.weather  w
        group by longitude, latitude
    ) as src
    left join dwh.dim_coordinates as dco 
        on (dco.longitude, dco.latitude) = (src.longitude, src.latitude)
    where dco.longitude is null""")
    count_dim_coordinates = cursor.rowcount
    return count_dim_coordinates


def insert_dim_date(cursor):
    cursor.execute("""
    insert into dwh.dim_date (full_date, initial_date, "year", "month", month_text, "day", "quarter", 
                                number_week, day_week, week_txt,  day_year)
    select src.full_date, src.initial_date, src."year", src."month", src.month_text, src."day", 
            src."quarter", src.number_week , src.day_week, src.week_txt, src.day_year
    from (select (select to_char(w.date_downloads, 'YYYYMMDD')::integer) as full_date,
            w.date_downloads as initial_date,
            (select extract(year from w.date_downloads)) as "year",
            (select extract(month from w.date_downloads)) as "month",
            (select to_char(w.date_downloads, 'month')) as month_text,
            (select extract(day from w.date_downloads)) as "day",
            (select extract(quarter from w.date_downloads)) as "quarter",
            (select extract(week from w.date_downloads)) as number_week,
            (select extract(isodow from w.date_downloads)) as day_week,
            (select to_char(w.date_downloads, 'day')) as week_txt,
            (select extract(doy from w.date_downloads)) as day_year
            from dwh.weather w
            group by date_downloads
    ) as src
    left join dwh.dim_date as dd 
        on dd.initial_date = src.initial_date
    where dd.initial_date is null
    order by initial_date""")
    count_dim_date = cursor.rowcount
    return count_dim_date


def insert_dim_main(cursor):
    cursor.execute("""
    insert into dwh.dim_main
    (temperature, feels_like, temp_min, temp_max, pressure, humidity, sea_level, grnd_level, visibility)
    select tempr."temp" as temperature, fl.feels_like, temp_mn.temp_min, temp_mx.temp_max, pr.pressure, 
            hum.humidity, sea.sea_level, grnd.grnd_level, src.visibility::smallint
    from dwh.weather as src
    cross join lateral json_to_record(src.main) as tempr("temp" real)
    cross join lateral json_to_record(src.main) as fl(feels_like real)
    cross join lateral json_to_record(src.main) as temp_mn(temp_min real)
    cross join lateral json_to_record(src.main) as temp_mx(temp_max real)
    cross join lateral json_to_record(src.main) as pr(pressure smallint)
    cross join lateral json_to_record(src.main) as hum(humidity smallint)
    cross join lateral json_to_record(src.main) as sea(sea_level smallint)
    cross join lateral json_to_record(src.main) as grnd(grnd_level smallint)
    where not exists (
    	select main_id from dwh.dim_main group by main_id
    	having main_id >= src.id)""")
    count_dim_main = cursor.rowcount
    return count_dim_main


def insert_dim_sun_light(cursor):
    cursor.execute("""
    insert into dwh.dim_sun_light (sunrise, sunset, sunrise_unix, sunset_unix)
    select src.sunrise, src.sunset, src.sunrise_unix, src.sunset_unix
    from (select (select * from json_to_record(w.sys) as x(sunrise bigint)) as sunrise,
        (select * from json_to_record (w.sys) as x(sunset bigint)) as sunset,
        (select timestamp with time zone 'epoch' + (select * from json_to_record (w.sys) as x(sunrise bigint)) 
            * interval '1 second') as sunrise_unix,
        (select timestamp with time zone 'epoch' + (select * from json_to_record (w.sys) as x(sunset bigint)) 
            * interval '1 second') as sunset_unix
        from dwh.weather w
        group by sunrise, sunset, sunrise_unix, sunset_unix
    ) as src
    left join dwh.dim_sun_light as dsl
        on (dsl.sunrise, dsl.sunset) = (src.sunrise, src.sunset)
    where dsl.sunrise is null
    order by sys_ts""")
    count_dim_sun_light = cursor.rowcount
    return count_dim_sun_light


def insert_dim_time(cursor):
    cursor.execute("""
    insert into dwh.dim_time (full_time, initial_time, "hour", "minute", time_day)
    select src.full_time, src.initial_time, src."hour", src."minute", src.time_day
    from (select (select to_char(w.time_downloads, 'HH24MISS')::integer) as full_time,
        w.time_downloads as initial_time,
        (select extract(hour from w.time_downloads)) as "hour",
        (select extract(minute from w.time_downloads)) as "minute",
        case when w.time_downloads between '00:00:00' and '05:59:59' then 'night' when
        w.time_downloads between '06:00:00' and '11:59:59' then 'morning' when
        w.time_downloads between '12:00:00' and '17:59:59' then 'afternoon' when
        w.time_downloads between '18:00:00' and '23:59:59' then 'evening' end as time_day
        from dwh.weather w
        group by time_downloads
    ) as src
        left join dwh.dim_time as dt
            on dt.initial_time = src.initial_time
    where dt.initial_time is null
    order by initial_time""")
    count_dim_time = cursor.rowcount
    return count_dim_time


def insert_dim_timezone(cursor):
    cursor.execute("""
    insert into dwh.dim_timezone (timezone)
    select w.timezone
    from dwh.weather w
    left join dwh.dim_timezone dtz
        on w.timezone = dtz.timezone 
    where dtz.timezone is null 
    group by w.timezone """)
    count_dim_timezone = cursor.rowcount
    return count_dim_timezone


def insert_dim_timezone_name(cursor):
    cursor.execute("""
    insert into dwh.dim_timezone_name ("name", country)
    select src."name", src.country
    from (select w."name",
        (select * from json_to_record(w.sys) as x(country text)) as country
        from dwh.weather w
        group by "name", country
    ) as src
    left join dwh.dim_timezone_name dtn
        on dtn."name" = src."name"
    where dtn."name" is null""")
    count_dim_timezone_name = cursor.rowcount
    return count_dim_timezone_name


def insert_dim_weather_descr(cursor):
    cursor.execute("""
    insert into dwh.dim_weather_descr (group_main_params, weather_condition_groups, cloudiness)
    select src.group_main_params, src.weather_condition_groups, src.cloudiness
    from (select (select * from json_to_recordset(w.weather) as x(main text)) as group_main_params,
        (select * from json_to_recordset(w.weather) as x(description text)) as weather_condition_groups,
        (select * from json_to_record(w.clouds) as x("all" smallint)) as cloudiness
        from dwh.weather w
    ) as src
    left join dwh.dim_weather_descr dwd
        on (dwd.group_main_params, dwd.weather_condition_groups, dwd.cloudiness) 
            = (src.group_main_params, src.weather_condition_groups, src.cloudiness)
    where (dwd.group_main_params, dwd.weather_condition_groups, dwd.cloudiness) is null
    group by src.group_main_params, src.weather_condition_groups, src.cloudiness, sys_ts
    order by sys_ts""")
    count_dim_weather_descr = cursor.rowcount
    return count_dim_weather_descr


def insert_dim_wind(cursor):
    cursor.execute("""
    insert into dwh.dim_wind (speed, "degree")
    select src.speed, src."degree"
    from (select (select * from json_to_record(w.wind) as x(speed real)) as speed,
        (select * from json_to_record(w.wind) as x(deg smallint)) as "degree"
        from dwh.weather  w
        group by speed, "degree"
    ) as src
    left join dwh.dim_wind as dw
        on (dw.speed, dw."degree") = (src.speed, src."degree")
    where dw.speed is null
    order by sys_ts""")
    count_dim_wind = cursor.rowcount
    return count_dim_wind


def insert_stage_fact_weather(cursor):
    cursor.execute("""
    insert into dwh.stage_fact_weather 
    (
    weather_id, hash, dim_date_id, date_downloads, dim_time_id, time_downloads, dim_coordinates_id, longitude, latitude,
    dim_sun_light_id, sun_l_country, sun_l_sunrise, sun_l_sunset, dim_timezone_id, timezone,
    dim_timezone_name_id, timezone_name, dim_weather_descr_id, weather_main, weather_description, 
    cloudiness, dim_wind_id, wind_speed, wind_direction, temperature, feels_like, 
    temp_min, temp_max, pressure, humidity, visibility, time_calculation
    )
    select distinct on (src.id) src.id as weather_id, 
        md5(src.id::text || dd.date_id::text || dt.time_id::text)::uuid as hash, dd.date_id as dim_date_id,
        src.date_downloads, dt.time_id as dim_time_id, src.time_downloads, dco.coord_id as dim_coordinates_id, 
        ll.lon as longitude, ll.lat as latitude, dsl.sun_light_id as dim_sun_light_id, sun_l.country, 
        sun_l.sunrise as sun_l_sunrise, sun_l.sunset as sun_l_sunset, dtz.timezone_id as dim_timezone_id, 
        dtz.timezone as timezone, dtn.timezone_name_id as dim_timezone_name_id,dtn."name" as timezone_name, 
        dwd.weather_descr_id as dim_weather_descr_id, weath.main as weather_main, 
        weath.description as weather_description, cloud."all" as cloudiness, dw.wind_id as dim_wind_id, 
        wd.speed as wind_speed, wd.deg as wind_direction, tempr."temp" as temperature, fl.feels_like, 
        temp_mn.temp_min, temp_mx.temp_max, pr.pressure, hum.humidity, src.visibility::smallint, src.dt as time_calculation
    from dwh.weather as src
    join dwh.dim_date as dd on dd.initial_date = src.date_downloads 
    join dwh.dim_time as dt on dt.initial_time = src.time_downloads
    cross join lateral json_to_record(src.coord) as ll (lon real, lat real)
    join dwh.dim_coordinates as dco on (dco.longitude, dco.latitude) = (ll.lon, ll.lat) 
    cross join lateral json_to_record(src.sys) as sun_l ("type" smallint, id integer, country text, sunrise bigint, sunset bigint)
    join dwh.dim_sun_light as dsl on (dsl.sunrise, dsl.sunset) = (sun_l.sunrise, sun_l.sunset)
    join dwh.dim_timezone as dtz on dtz.timezone = src.timezone 
    join dwh.dim_timezone_name as dtn on dtn."name" = src."name"
    cross join lateral json_to_recordset(src.weather) as weath (id smallint, main text, description text)
    join dwh.dim_weather_descr as dwd on (dwd.group_main_params, dwd.weather_condition_groups) = (weath.main, weath.description)
    cross join lateral json_to_record(src.clouds) as cloud ("all" smallint)
    join dwh.dim_weather_descr as dc on dc.cloudiness = cloud."all"
    cross join lateral json_to_record(src.wind) as wd (speed real, deg smallint)
    join dwh.dim_wind as dw on (dw.speed, dw."degree") = (wd.speed, wd.deg)
    cross join lateral json_to_record(src.main) as tempr("temp" real)
    cross join lateral json_to_record(src.main) as fl(feels_like real)
    cross join lateral json_to_record(src.main) as temp_mn(temp_min real)
    cross join lateral json_to_record(src.main) as temp_mx(temp_max real)
    cross join lateral json_to_record(src.main) as pr(pressure smallint)
    cross join lateral json_to_record(src.main) as hum(humidity smallint)
    where not exists (
    	select stage_fact_weather_id from dwh.stage_fact_weather group by stage_fact_weather_id
    	having stage_fact_weather_id >= src.id
    	)
    order by weather_id, dim_time_id, dc.sys_ts""")
    count_stage_fact_weather = cursor.rowcount
    return count_stage_fact_weather