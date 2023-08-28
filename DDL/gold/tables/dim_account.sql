CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE gold.dim_account (
	id serial4 NOT NULL,
	join_date date NULL,
	age int4 NULL,
	gender varchar(20) NULL,
	country varchar(50) NULL,
	CONSTRAINT user_dimension_pkey PRIMARY KEY (id)
);
