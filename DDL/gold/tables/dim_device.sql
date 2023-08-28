CREATE TABLE gold.dim_device (
	id serial4 NOT NULL,
	device varchar(50) NULL,
	CONSTRAINT device_dimension_pkey PRIMARY KEY (id)
);
