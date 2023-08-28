CREATE SCHEMA IF NOT EXISTS silver;


CREATE TABLE silver.netflix (
    user_id INT PRIMARY KEY,
    subscription_type VARCHAR(50),
    monthly_revenue INT,
    join_date DATE,
    last_payment_date DATE,
    country VARCHAR(50),
    age INT,
    gender VARCHAR(10),
    device VARCHAR(50),
    plan_duration INT,
    active_profiles INT,
    household_profile_Ind INT,
    movies_watched INT,
    series_watched INT
);