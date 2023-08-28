CREATE SCHEMA IF NOT EXISTS bronze;


CREATE TABLE bronze.netflix (
    user_id INT,
    subscription_type VARCHAR(50),
    monthly_revenue INT,
    join_date VARCHAR(50),
    last_payment_date VARCHAR(50),
    country VARCHAR(50),
    age INT,
    gender VARCHAR(10),
    device VARCHAR(50),
    plan_duration VARCHAR,
    active_profiles INT,
    household_profile_Ind INT,
    movies_watched INT,
    series_watched INT
);