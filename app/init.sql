
CREATE TABLE IF NOT EXISTS public.weather (
	id int4 NOT NULL GENERATED ALWAYS AS IDENTITY,
	date_downloads date NULL,
	time_downloads time NULL,
	coord json NULL,
	weather json NULL,
	base text NULL,
	main json NULL,
	visibility text NULL,
	wind json NULL,
	clouds json NULL,
	dt int4 NULL,
	sys json NULL,
	timezone int4 NULL,
	name text NULL,
	CONSTRAINT pk_weather PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.city_name (
	city_name_id int4 NOT NULL GENERATED ALWAYS AS IDENTITY,
	weather_id int4 NOT NULL,
	city text NOT NULL,
	CONSTRAINT pk_city_name PRIMARY KEY (city_name_id),
	CONSTRAINT fk_city_name_weather_id FOREIGN KEY (weather_id) REFERENCES public.weather(id) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE INDEX idx_city_name_weather_id ON public.city_name USING btree (weather_id);

CREATE TABLE IF NOT EXISTS public.date_time_downloads (
	date_time_downloads_id int4 NOT NULL GENERATED ALWAYS AS IDENTITY,
	weather_id int4 NOT NULL,
	date_downloads date NULL,
	time_downloads time NULL,
	CONSTRAINT pk_date_time_downloads PRIMARY KEY (date_time_downloads_id),
	CONSTRAINT fk_date_time_downloads_weather_id FOREIGN KEY (weather_id) REFERENCES public.weather(id) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE INDEX idx_dt_downloads_weather_id ON public.date_time_downloads USING btree (weather_id);

CREATE TABLE IF NOT EXISTS public.sun_light (
	sun_light_id int4 NOT NULL GENERATED ALWAYS AS IDENTITY,
	weather_id int4 NOT NULL,
	sunrise int4 NULL,
	sunset int4 NULL,
	CONSTRAINT pk_sun_light PRIMARY KEY (sun_light_id),
	CONSTRAINT fk_wind_clouds_weather_id FOREIGN KEY (weather_id) REFERENCES public.weather(id) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE INDEX idx_wind_clouds_weather_id ON public.sun_light USING btree (weather_id);

CREATE TABLE IF NOT EXISTS public.weather_temperature_params (
	temperature_params_id int4 NOT NULL GENERATED ALWAYS AS IDENTITY,
	weather_id int4 NOT NULL,
	temperature float8 NULL,
	feels_like float8 NULL,
	pressure int4 NULL,
	humidity int4 NULL,
	CONSTRAINT pk_weather_temperature_params PRIMARY KEY (temperature_params_id),
	CONSTRAINT fk_weather_temperature_params_weather_id FOREIGN KEY (weather_id) REFERENCES public.weather(id) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE INDEX idx_weather_temperature_params_weather_id ON public.weather_temperature_params USING btree (weather_id);

CREATE TABLE IF NOT EXISTS public.weather_wind_clouds_params (
	wind_clouds_params_id int4 NOT NULL GENERATED ALWAYS AS IDENTITY,
	weather_id int4 NOT NULL,
	wind_speed int4 NULL,
	wind_deg int4 NULL,
	weather_parameters text NULL,
	description text NULL,
	clouds int4 NULL,
	CONSTRAINT pk_weather_wind_clouds_params PRIMARY KEY (wind_clouds_params_id),
	CONSTRAINT fk_weather_wind_clouds_weather_id FOREIGN KEY (weather_id) REFERENCES public.weather(id) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE INDEX idx_weather_wind_clouds_weather_id ON public.weather_wind_clouds_params USING btree (weather_id);
