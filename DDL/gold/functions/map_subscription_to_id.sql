CREATE OR REPLACE FUNCTION map_subscription_to_id(subscription_type text, monthly_revenue numeric) 
RETURNS integer AS $$
BEGIN
    RETURN CASE 
        WHEN subscription_type = 'Standard' AND monthly_revenue = 10 THEN 1
        WHEN subscription_type = 'Standard' AND monthly_revenue = 15 THEN 2
        WHEN subscription_type = 'Standard' AND monthly_revenue = 11 THEN 3
        WHEN subscription_type = 'Standard' AND monthly_revenue = 14 THEN 12
        WHEN subscription_type = 'Standard' AND monthly_revenue = 13 THEN 16
        WHEN subscription_type = 'Standard' AND monthly_revenue = 12 THEN 9

        WHEN subscription_type = 'Premium' AND monthly_revenue = 12 THEN 4
        WHEN subscription_type = 'Premium' AND monthly_revenue = 15 THEN 5
        WHEN subscription_type = 'Premium' AND monthly_revenue = 14 THEN 7
         WHEN subscription_type = 'Premium' AND monthly_revenue = 10 THEN 14
        WHEN subscription_type = 'Premium' AND monthly_revenue = 13 THEN 15
        WHEN subscription_type = 'Premium' AND monthly_revenue = 11 THEN 17
        
        WHEN subscription_type = 'Basic' AND monthly_revenue = 14 THEN 10
        WHEN subscription_type = 'Basic' AND monthly_revenue = 13 THEN 11
        WHEN subscription_type = 'Basic' AND monthly_revenue = 10 THEN 13
        WHEN subscription_type = 'Basic' AND monthly_revenue = 11 THEN 6
        WHEN subscription_type = 'Basic' AND monthly_revenue = 12 THEN 8
        WHEN subscription_type = 'Basic' AND monthly_revenue = 15 THEN 18
       
  
        ELSE NULL
    END;
END;
$$ LANGUAGE plpgsql;
