-- https://hoffa.medium.com/generating-all-the-holidays-in-sql-with-a-python-udtf-4397f190252b

CREATE OR REPLACE FUNCTION scratch.robinpatel.holidays(
    country_region string, years array
                                                      )
    RETURNS table (day date, holiday string)
    LANGUAGE python runtime_version = '3.8' imports = ( '@~/holidays.zip' ) packages = ( 'python-dateutil' , 'hijri-converter' , 'convertdate' , 'korean_lunar_calendar' ) handler = 'X'
AS
$$
from datetime import date
import holidays
class X:
    def __init__(self):
        pass
    def process(self, country_region, years):
        parsed_country_region = country_region.split('-')
        country = parsed_country_region[0]
        region = (
            parsed_country_region[1]
            if len(parsed_country_region) > 1
            else None
        )
        range_years = range(min(years), 1 + max(years))
        holidays_dict = holidays.country_holidays(
           country
           , subdiv=region
           , years=range_years
        )
        return(((k, v) for k, v in holidays_dict.items()))
    def end_partition(self):
            pass
$$;



CREATE OR REPLACE FUNCTION scratch.robinpatel.debug_holidays(
                                                            )
    RETURNS string
    LANGUAGE python runtime_version = '3.8' packages = ( 'holidays' ) handler = 'x'
AS
$$
import holidays
def x():
    return holidays.__version__
$$;

SELECT debug_holidays();
-- 0.11.3.1
SELECT scratch.robinpatel.debug_holidays();

SELECT *
FROM TABLE (holidays('US', [2022]));

SELECT *
FROM TABLE (holidays('US', [2022]));

USE SCHEMA scratch.robinpatel;

PUT file:///Users/robin/myrepos/holidays/python-holidays/holidays.zip @~/ AUTO_COMPRESS = FALSE OVERWRITE=TRUE;

CREATE OR REPLACE FUNCTION scratch.robinpatel.debug_holidays(
                                                            )
    RETURNS string
    LANGUAGE python runtime_version = '3.8' packages = ( 'python-dateutil' , 'hijri-converter' , 'convertdate' , 'korean_lunar_calendar' ) imports = ( '@~/holidays.zip' ) handler = 'x'
AS
$$
import holidays
def x():
    return holidays.__version__
$$;
SELECT debug_holidays();
#
ModuleNotFoundError: NO module named 'dateutil'  IN FUNCTION DEBUG_HOLIDAYS
WITH handler x


CREATE OR REPLACE FUNCTION scratch.robinpatel.holidays(
    country_region string, years array
                                                      )
    RETURNS table (day date, holiday string)
    LANGUAGE python runtime_version = '3.8' imports = ( '@~/holidays.zip' ) packages = ( 'python-dateutil' , 'hijri-converter' , 'convertdate' , 'korean_lunar_calendar' ) handler = 'X'
AS
$$
from datetime import date
import holidays
class X:
    def __init__(self):
        pass
    def process(self, country_region, years):
        parsed_country_region = country_region.split('-')
        country = parsed_country_region[0]
        region = (
            parsed_country_region[1]
            if len(parsed_country_region) > 1
            else None
        )
        range_years = range(min(years), 1 + max(years))
        holidays_dict = holidays.country_holidays(
           country
           , subdiv=region
           , years=range_years
        )
        return(((k, v) for k, v in holidays_dict.items()))
    def end_partition(self):
            pass
$$;

SELECT *
FROM TABLE (holidays('US', [2022]));
SELECT *
FROM TABLE (holidays('US-CA', [2023]));
SELECT *
FROM TABLE (holidays('UK', [2010, 2030]));

SELECT
    name
FROM se.data.se_country sc;


------------------------------------------------------------------------------------------------------------------------


SELECT *
FROM collab.hackdays.calendar_holidays
WHERE is_holiday = TRUE
  AND YEAR(date_value) = '2022'
ORDER BY date_value ASC;


-- SELECT
--     ssa.hotel_code,
--     ssa.posu_country,
--     IFF(ssa.posu_country IN ('England', 'Wales/Cymru', 'Scotland', 'Ireland'), 'UK', sc.code) AS country_code
--
-- FROM se.data.se_sale_attributes ssa
--     LEFT JOIN se.data.se_country sc ON ssa.posu_country = sc.name



WITH hotel_country AS (
    SELECT
        ssa.hotel_code,
        ANY_VALUE(IFF(ssa.posu_country IN ('England', 'Wales/Cymru', 'Scotland', 'Ireland'), 'UK', sc.code)) AS country_code
    FROM se.data.se_sale_attributes ssa
        LEFT JOIN se.data.se_country sc ON ssa.posu_country = sc.name
    GROUP BY 1
),
     agg_allocation AS (
         SELECT
             hscv.date,
             ht.country_code,
             SUM(hscv.no_total_rooms)     AS total_inventory,
             SUM(hscv.no_available_rooms) AS available_inventory
         FROM se.data.harmonised_hotel_rooms_and_rates hscv
             LEFT JOIN hotel_country ht ON hscv.hotel_code = ht.hotel_code
         GROUP BY 1, 2
     )
SELECT
    ch.date_value,
    ch.territory,
    ch.is_holiday,
    ch.holiday_description,
    al.total_inventory,
    al.available_inventory
FROM collab.hackdays.calendar_holidays ch
    LEFT JOIN agg_allocation al ON ch.date_value = al.date AND ch.territory = al.country_code
WHERE date_value BETWEEN CURRENT_DATE AND CURRENT_DATE + 365;

SELECT *
FROM collab.hackdays.calendar_holidays ch
WHERE ch.territory = 'US'
  AND (ch.is_holiday OR ch.holiday_description IS NOT NULL)
  AND YEAR(ch.date_value) = '2022';


SELECT *
FROM collab.hackdays.calendar_holidays ch
WHERE ch.is_holiday = FALSE
  AND ch.holiday_description IS NOT NULL
  AND YEAR(ch.date_value) = '2022';
;


SELECT * FROM latest_vault.cms_mongodb.users;

SELECT * FROM data_vault_mvp.dwh.iterable__user_profile iup;




