CREATE TABLE public.countrydimension (
	id serial4 NOT NULL,
	country varchar(100) NULL,
	CONSTRAINT countrydimension_pkey PRIMARY KEY (id)
);


CREATE TABLE public.datedimension (
	id date NOT NULL,
	"day" int4 NULL,
	"month" int4 NULL,
	monthname varchar(20) NULL,
	"year" int4 NULL,
	quarter int4 NULL,
	weekday int4 NULL,
	dayofweekname varchar(20) NULL,
	isweekend bool NULL,
	CONSTRAINT datedimension_pkey PRIMARY KEY (id)
);

CREATE TABLE public.devicedimension (
	id serial4 NOT NULL,
	device varchar(50) NULL,
	CONSTRAINT devicedimension_pkey PRIMARY KEY (id)
);


CREATE TABLE public.genderdimension (
	id serial4 NOT NULL,
	gender varchar(10) NULL,
	CONSTRAINT genderdimension_pkey PRIMARY KEY (id)
);

-- public.salesfact definition

-- Drop table

-- DROP TABLE public.salesfact;

CREATE TABLE public.salesfact (
	salesid serial4 NOT NULL,
	userid int4 NULL,
	subscriptionid int4 NULL,
	last_payment_date date NULL,
	country_id int4 NULL,
	device_id int4 NULL,
	CONSTRAINT salesfact_pkey PRIMARY KEY (salesid)
);


-- public.salesfact foreign keys

ALTER TABLE public.salesfact ADD CONSTRAINT fk_country FOREIGN KEY (country_id) REFERENCES public.countrydimension(id);
ALTER TABLE public.salesfact ADD CONSTRAINT fk_date FOREIGN KEY (last_payment_date) REFERENCES public.datedimension(id);
ALTER TABLE public.salesfact ADD CONSTRAINT fk_device FOREIGN KEY (device_id) REFERENCES public.devicedimension(id);
ALTER TABLE public.salesfact ADD CONSTRAINT fk_subscription FOREIGN KEY (subscriptionid) REFERENCES public.subscriptiondimension(id);
ALTER TABLE public.salesfact ADD CONSTRAINT fk_user FOREIGN KEY (userid) REFERENCES public.userdimension(userid);

-- public.subscriptiondimension definition

-- Drop table

-- DROP TABLE public.subscriptiondimension;

CREATE TABLE public.subscriptiondimension (
	id serial4 NOT NULL,
	"subscription" varchar(255) NULL,
	plan_duration int4 NULL,
	revenue int4 NULL,
	CONSTRAINT subscriptiondimension_pkey PRIMARY KEY (id)
);

-- public.userdimension definition

-- Drop table

-- DROP TABLE public.userdimension;

CREATE TABLE public.userdimension (
	userid serial4 NOT NULL,
	joindate date NULL,
	age int4 NULL,
	activeprofiles int4 NULL,
	householdprofileind int4 NULL,
	movieswatched int4 NULL,
	serieswatched int4 NULL,
	gender_id int4 NULL,
	CONSTRAINT userdimension_pkey PRIMARY KEY (userid)
);


-- public.userdimension foreign keys

ALTER TABLE public.userdimension ADD CONSTRAINT fk_gender FOREIGN KEY (gender_id) REFERENCES public.genderdimension(id);