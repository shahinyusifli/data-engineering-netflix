CREATE table gold.dim_date (
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
