CREATE TABLE gold.fct_sales (
	id serial4 NOT NULL,
	account_id int4 NULL,
	subscription_id int4 NULL,
	last_payment_date date NULL,
	device_id int4 NULL,
	active_profiles int4 NULL,
	household_profile_ind int4 NULL,
	movies_watched int4 NULL,
	series_watched int4 NULL,
	CONSTRAINT sales_fact_pkey PRIMARY KEY (id)
);

ALTER TABLE gold.fct_sales ADD CONSTRAINT fk_date FOREIGN KEY (last_payment_date) REFERENCES gold.dim_date(id);
ALTER TABLE gold.fct_sales ADD CONSTRAINT fk_device FOREIGN KEY (device_id) REFERENCES gold.dim_device(id);
ALTER TABLE gold.fct_sales ADD CONSTRAINT fk_subscription FOREIGN KEY (subscription_id) REFERENCES gold.dim_subscription(id);
ALTER TABLE gold.fct_sales ADD CONSTRAINT fk_user FOREIGN KEY (account_id) REFERENCES gold.dim_account(id);