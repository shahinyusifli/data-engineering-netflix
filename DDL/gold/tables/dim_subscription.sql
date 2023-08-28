CREATE TABLE gold.dim_subscription (
	id serial4 NOT NULL,
	subscription_type varchar(255) NULL,
	plan_duration int4 NULL,
	revenue int4 NULL,
	CONSTRAINT subscription_dimension_pkey PRIMARY KEY (id)
);