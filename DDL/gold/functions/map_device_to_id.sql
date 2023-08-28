CREATE OR REPLACE FUNCTION map_device_to_id(device_name text)
RETURNS integer AS $$
DECLARE
    device_id integer;
BEGIN
    SELECT id INTO device_id
    FROM gold.dim_device
    WHERE device = device_name;

    RETURN device_id;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN NULL;
END;
$$ LANGUAGE plpgsql;