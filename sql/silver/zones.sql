-- SILVER: clean the static taxi-zone reference data.
CREATE OR REPLACE TEMP TABLE zones_staged AS
SELECT
    CAST(LocationID AS INTEGER)                               AS zone_id,
    COALESCE(nullif(trim(Borough), 'N/A'), 'Unknown')         AS borough,
    COALESCE(nullif(trim(Zone), 'N/A'), 'Unknown')            AS zone_name,
    COALESCE(nullif(trim(service_zone), 'N/A'), 'Unknown')    AS service_zone,
    CAST(LocationID AS INTEGER) IN (1, 132, 138)              AS is_airport
FROM read_csv('$src', header = true, all_varchar = true)
QUALIFY row_number() OVER (PARTITION BY LocationID ORDER BY LocationID) = 1;
