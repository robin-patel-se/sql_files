CREATE OR REPLACE TABLE scratch.robinpatel.amenities
(
    hotel_name      VARCHAR,
    city            VARCHAR,
    country         VARCHAR,
    account_id      VARCHAR,
    snapshot_url    VARCHAR,
    facilities_list VARCHAR,
    facilities_json VARCHAR,
    deep_link       VARCHAR
);

USE SCHEMA scratch.robinpatel;

PUT file:///Users/robin/myrepos/sql_files/ad_hoc/jan_aminities/aminities.csv @%amenities;


COPY INTO scratch.robinpatel.amenities
    FILE_FORMAT = (
        TYPE = CSV
            FIELD_DELIMITER = ','
            SKIP_HEADER = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
            RECORD_DELIMITER = '\\n'
        );

SELECT a.hotel_name,
       a.city,
       a.country,
       a.account_id,
       a.snapshot_url,
       a.facilities_list,
       PARSE_JSON(a.facilities_json) AS faciilites_json,
       a.deep_link
FROM scratch.robinpatel.amenities a;

------------------------------------------------------------------------------------------------------------------------
-- to find all different types of amenities

SELECT PARSE_JSON(a.facilities_json) AS faciilites_json,
       keys.*,
       amenities.*
FROM scratch.robinpatel.amenities a,
     LATERAL FLATTEN(INPUT => PARSE_JSON(a.facilities_json), OUTER => TRUE) keys,
     LATERAL FLATTEN(INPUT => keys.value, OUTER => TRUE) amenities;


SELECT amenities.key,
       COUNT(*)
FROM scratch.robinpatel.amenities a,
     LATERAL FLATTEN(INPUT => PARSE_JSON(a.facilities_json), OUTER => TRUE) keys,
     LATERAL FLATTEN(INPUT => keys.value, OUTER => TRUE) amenities
WHERE amenities.value::VARCHAR IS DISTINCT FROM ''
GROUP BY 1

-- important_facilities
-- Outdoors
-- description
-- Activities
-- Internet
-- Parking
-- General
-- Languages Spoken
-- Bathroom
-- View
-- Kitchen
-- Room Amenities
-- Media & Technology
-- Food & Drink
-- Transportation
-- Services
-- Safety & security
-- Accessibility
-- Pets
-- Spa
-- Ski
-- Entertainment & Family Services
-- Business Facilities
-- 4 swimming pools
-- Common Areas
-- Swimming pool
-- Health & Wellness Facilities
-- Cleaning Services
-- Outdoor & View
-- Shops
-- 7 swimming pools
--
-- Transport
-- Business facilities
-- Languages spoken
-- 12 swimming pools
-- Wellness
-- Badkamer
-- Slaapkamer
-- Media & Technologie
-- Diensten receptie
-- Overige
-- 9 swimming pools
-- Bedroom
-- Living Area
-- Outdoor swimming pool
-- 3 swimming pools
-- Services & Extras
-- Pool and Spa
-- Front Desk Services
-- Indoor swimming pool
-- 2 swimming pools
-- Building Characteristics
-- Miscellaneous
-- 5 swimming pools
-- Pool  – outdoor (kids)
-- 6 swimming pools
-- Cleaning services
-- Pool  – indoor (kids)
-- 8 swimming pools
-- Kamerfaciliteiten
-- Zakelijke faciliteiten
-- Veiligheid
-- Gesproken talen
-- Parkeerplaats
-- Kenmerken gebouw
-- 16 swimming pools
-- Reception services
-- Entertainment and family services
-- Wellness facilities
-- Buiten
-- Woonruimte
-- Toegankelijkheid


------------------------------------------------------------------------------------------------------------------------