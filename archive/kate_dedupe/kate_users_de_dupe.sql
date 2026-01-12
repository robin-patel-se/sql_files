CREATE OR REPLACE TABLE collab.covid_pii.customer_sf
(
    firstname     VARCHAR,
    id            VARCHAR,
    lastname      VARCHAR,
    personalemail VARCHAR

);

USE SCHEMA collab.covid_pii;
USE WAREHOUSE pipe_xlarge;

PUT file:///Users/robin/sqls/pro-boner/extract.csv @%customer_sf;

COPY INTO collab.covid_pii.customer_sf
    FILE_FORMAT = (
        TYPE = CSV
            FIELD_DELIMITER = ','
            SKIP_HEADER = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
            RECORD_DELIMITER = '\\n'
            REPLACE_INVALID_CHARACTERS = TRUE
        );


SELECT SUM(accounts)
FROM (
         SELECT COALESCE(NULLIF(cs.personalemail, ''), IFF(cs.lastname LIKE '%@%', cs.lastname, NULL)) AS email,
                count(*)                                                                               AS accounts
         FROM collab.covid_pii.customer_sf cs
         GROUP BY 1
         HAVING COUNT(*) > 1
         ORDER BY 2 DESC
     );

SELECT *
FROM collab.covid_pii.customer_sf cs
WHERE cs.personalemail = '';

DROP TABLE collab.covid_pii.customer_sf;

GRANT OWNERSHIP ON VIEW collab.marketing.deal_live_status TO ROLE personal_role__kirstengrieve;

SELECT * FROM collab.marketing.deal_live_status

DROP VIEW collab.marketing.odm_ndm_saleid_mapping;
DROP VIEW collab.marketing.braze_reminders_ndm_linked;
DROP VIEW collab.marketing.braze_reminders;

