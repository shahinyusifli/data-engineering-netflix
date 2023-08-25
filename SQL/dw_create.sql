BEGIN;

CREATE TABLE public.country_dimension (
	id serial4 NOT NULL,
	country varchar(100) NULL,
	CONSTRAINT country_dimension_pkey PRIMARY KEY (id)
);


CREATE TABLE public.date_dimension (
	id date NOT NULL,
	"day" int4 NULL,
	"month" int4 NULL,
	month_name varchar(20) NULL,
	"year" int4 NULL,
	quarter int4 NULL,
	weekday int4 NULL,
	day_of_week_name varchar(20) NULL,
	is_weekend bool NULL,
	CONSTRAINT date_dimension_pkey PRIMARY KEY (id)
);


CREATE TABLE public.device_dimension (
	id serial4 NOT NULL,
	device varchar(50) NULL,
	CONSTRAINT device_dimension_pkey PRIMARY KEY (id)
);


CREATE TABLE public.gender_dimension (
	id serial4 NOT NULL,
	gender varchar(10) NULL,
	CONSTRAINT gender_dimension_pkey PRIMARY KEY (id)
);


CREATE TABLE public.subscription_dimension (
	id serial4 NOT NULL,
	"subscription" varchar(255) NULL,
	plan_duration int4 NULL,
	revenue int4 NULL,
	CONSTRAINT subscription_dimension_pkey PRIMARY KEY (id)
);


CREATE TABLE public.user_dimension (
	id serial4 NOT NULL,
	join_date date NULL,
	age int4 NULL,
	active_profiles int4 NULL,
	household_profile_ind int4 NULL,
	movies_watched int4 NULL,
	series_watched int4 NULL,
	gender_id int4 NULL,
	CONSTRAINT user_dimension_pkey PRIMARY KEY (id)
);
ALTER TABLE public.user_dimension ADD CONSTRAINT fk_gender FOREIGN KEY (gender_id) REFERENCES public.gender_dimension(id);


CREATE TABLE public.sales_fact (
	id serial4 NOT NULL,
	user_id int4 NULL,
	subscription_id int4 NULL,
	last_payment_date date NULL,
	country_id int4 NULL,
	device_id int4 NULL,
	CONSTRAINT sales_fact_pkey PRIMARY KEY (id)
);
ALTER TABLE public.sales_fact ADD CONSTRAINT fk_country FOREIGN KEY (country_id) REFERENCES public.country_dimension(id);
ALTER TABLE public.sales_fact ADD CONSTRAINT fk_date FOREIGN KEY (last_payment_date) REFERENCES public.date_dimension(id);
ALTER TABLE public.sales_fact ADD CONSTRAINT fk_device FOREIGN KEY (device_id) REFERENCES public.device_dimension(id);
ALTER TABLE public.sales_fact ADD CONSTRAINT fk_subscription FOREIGN KEY (subscription_id) REFERENCES public.subscription_dimension(id);
ALTER TABLE public.sales_fact ADD CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES public.user_dimension(id);

COMMIT;