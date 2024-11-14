CREATE SCHEMA IF NOT EXISTS dwh;

CREATE TABLE IF NOT EXISTS dwh.weather
(
	id integer not null generated always as identity,
	date_downloads date NULL,
	time_downloads time NULL,
	coord json NULL,
	weather json NULL,
	base text NULL,
	main json NULL,
	visibility text NULL,
	wind json NULL,
	clouds json NULL,
	dt integer NULL,
	sys json NULL,
	timezone integer NULL,
	name text NULL,
	constraint pk_weather PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS dwh.dim_coordinates
(
	coord_id integer generated always as identity,
	longitude real not null,
	latitude real not null,
	sys_ts timestamp(0) default now(),
	constraint pk_dim_coordinates primary key(coord_id)
);

CREATE TABLE IF NOT EXISTS dwh.dim_date 
(
	date_id integer generated always as identity,
	full_date integer not null,
	initial_date date not null,
	"year" smallint not null,
	"month" smallint not null,
	month_text text not null,
	"day" smallint not null,
	"quarter" smallint not null,
	number_week smallint not null,
	day_week smallint not null,
	week_txt text not null,
	day_year smallint not null,
	sys_ts timestamp(0) default now(),
	constraint pk_date primary key (date_id)
);

CREATE TABLE IF NOT EXISTS dwh.dim_main
(
	main_id integer generated always as identity,
	temperature real not null,
	feels_like real not null,
	temp_min real not null,
	temp_max real not null,
	pressure smallint not null,
	humidity smallint not null,
	sea_level smallint not null,
	grnd_level smallint not null,
	visibility smallint not null,
	sys_ts timestamp(0) default now(),
	constraint pk_main primary key (main_id)
);

CREATE TABLE IF NOT EXISTS dwh.dim_sun_light
(
	sun_light_id integer generated always as identity,
	sunrise bigint not null,
	sunset bigint not null,
	sunrise_unix timestamp not null,
	sunset_unix timestamp not null,
	sys_ts timestamp(0) default now(),
	constraint pk_dim_sun_light primary key(sun_light_id)
);

CREATE TABLE IF NOT EXISTS dwh.dim_time
(
	time_id integer generated always as identity,
	full_time integer not null,
	initial_time time not null,
	"hour" integer not null,
	"minute" integer not null,
	time_day text,
	sys_ts timestamp(0) default now(),
	constraint pk_dim_time primary key (time_id)
);

CREATE TABLE IF NOT EXISTS dwh.dim_timezone
(
	timezone_id integer generated always as identity,
	timezone smallint not null,
	sys_ts timestamp(0) default now(),
	constraint pk_dim_timezone primary key(timezone_id)
);

CREATE TABLE IF NOT EXISTS dwh.dim_timezone_name
(
	timezone_name_id integer generated always as identity,
	"name" text not null,
	country text not null,
	sys_ts timestamp(0) default now(),
	constraint pk_dim_timezone_name primary key(timezone_name_id)
);

CREATE TABLE IF NOT EXISTS dwh.dim_weather_descr
(
	weather_descr_id integer generated always as identity,
	group_main_params text not null,
	weather_condition_groups text not null,
	cloudiness smallint not null,
	sys_ts timestamp(0) default now(),
	constraint pk_dim_weather_descr primary key (weather_descr_id)
);

CREATE TABLE IF NOT EXISTS dwh.dim_wind
(
	wind_id integer generated always as identity,
	speed real not null,
	"degree" smallint not null,
	sys_ts timestamp(0) default now(),
	constraint pk_dim_wind primary key(wind_id)
);

CREATE TABLE IF NOT EXISTS dwh.stage_fact_weather
(
	stage_fact_weather_id integer generated always as identity,
	weather_id integer not null,
	hash uuid not null,
	dim_date_id integer not null,
	date_downloads date not null,
	dim_time_id integer not null,
	time_downloads time not null,
	dim_coordinates_id integer not null,
	longitude real not null,
	latitude real not null,
	dim_sun_light_id integer not null,
	sun_l_id integer not null,
	sun_l_country text not null,
	sun_l_sunrise bigint not null,
	sun_l_sunset bigint not null,
	dim_timezone_id smallint not null,
	timezone smallint not null,
	dim_timezone_name_id smallint not null,
	timezone_name text not null,
	dim_weather_descr_id smallint not null,
	weather_descr_id smallint not null,
	weather_main text not null,
	weather_description text not null,
	cloudiness smallint not null,
	dim_wind_id integer not null,
	wind_speed real not null,
	wind_direction smallint not null,
	temperature real not null,
	feels_like real not null,
	temp_min real not null,
	temp_max real not null,
	pressure smallint not null,
	humidity smallint not null,
	visibility smallint not null,
	time_calculation bigint not null,
	sys_ts timestamp(0) default now(),
	constraint pk_dstage_fact_weather primary key(stage_fact_weather_id)
);