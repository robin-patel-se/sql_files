--   "AT":[4, 10, 12],
--       "BE":[7, 32, 39],
--       "CH":[4, 10, 12],
--       "DE":[4, 10, 12],
--       "DK":[13, 40, 46],
--       "ES":[2, 15, 42],
--       "FR":[18, 34],
--       "HK":[118, 122, 128, 141, 142, 150, 152],
--       "ID":[118, 122, 128, 141, 142, 150, 152],
--       "IT":[29],
--       "MY":[118, 122, 128, 141, 142, 150, 152],
--       "NL":[7, 32, 39],
--       "NO":[13, 40, 46],
--       "SE":[46],
--       "SG":[118, 122, 128, 141, 142, 150, 152],
--       "UK":[19, 20, 21, 27],
--       "US":[167, 190]
-- i.e if the territory = "AT" and CountryID = 4/10/12 then "Domestic", else "International"

--AT
SELECT 'AT',
       name AS country
FROM data_vault_mvp.cms_mysql_snapshots.country_snapshot cs
WHERE id IN (4, 10, 12);
--BE
SELECT 'BE',
       name AS country
FROM data_vault_mvp.cms_mysql_snapshots.country_snapshot cs
WHERE id IN (7, 32, 39);
--CH
SELECT 'CH',
       name AS country
FROM data_vault_mvp.cms_mysql_snapshots.country_snapshot cs
WHERE id IN (4, 10, 12);
--DE
SELECT 'DE',
       name AS country
FROM data_vault_mvp.cms_mysql_snapshots.country_snapshot cs
WHERE id IN (4, 10, 12);
--DK
SELECT 'DK',
       name AS country
FROM data_vault_mvp.cms_mysql_snapshots.country_snapshot cs
WHERE id IN (13, 40, 46);
--ES
SELECT 'ES',
       name AS country
FROM data_vault_mvp.cms_mysql_snapshots.country_snapshot cs
WHERE id IN (2, 15, 42);
--FR
SELECT 'FR',
       name AS country
FROM data_vault_mvp.cms_mysql_snapshots.country_snapshot cs
WHERE id IN (18, 34);
--HK
SELECT 'HK',
       name AS country
FROM data_vault_mvp.cms_mysql_snapshots.country_snapshot cs
WHERE id IN (118, 122, 128, 141, 142, 150, 152);
--ID
SELECT 'ID',
       name AS country
FROM data_vault_mvp.cms_mysql_snapshots.country_snapshot cs
WHERE id IN (118, 122, 128, 141, 142, 150, 152);
--IT
SELECT 'IT',
       name AS country
FROM data_vault_mvp.cms_mysql_snapshots.country_snapshot cs
WHERE id IN (29);
--MY
SELECT 'MY',
       name AS country
FROM data_vault_mvp.cms_mysql_snapshots.country_snapshot cs
WHERE id IN (118, 122, 128, 141, 142, 150, 152);
--NL
SELECT 'NL',
       name AS country
FROM data_vault_mvp.cms_mysql_snapshots.country_snapshot cs
WHERE id IN (7, 32, 39);
--NO
SELECT 'NO',
       name AS country
FROM data_vault_mvp.cms_mysql_snapshots.country_snapshot cs
WHERE id IN (13, 40, 46);
--SE
SELECT 'SE',
       name AS country
FROM data_vault_mvp.cms_mysql_snapshots.country_snapshot cs
WHERE id IN (46);
--SG
SELECT 'SG',
       name AS country
FROM data_vault_mvp.cms_mysql_snapshots.country_snapshot cs
WHERE id IN (118, 122, 128, 141, 142, 150, 152);
--UK
SELECT 'UK',
       name AS country
FROM data_vault_mvp.cms_mysql_snapshots.country_snapshot cs
WHERE id IN (19, 20, 21, 27);
--US
SELECT 'US',
       name AS country
FROM data_vault_mvp.cms_mysql_snapshots.country_snapshot cs
WHERE id IN (167, 190);


SELECT CASE
           WHEN 'posa_territory' = 'AT'
               AND 'posu_country' IN
                   (
                    'Austria', 'Switzerland', 'Germany'
                       )
               THEN 'Domestic'

           WHEN 'posa_territory' = 'BE'
               AND 'posu_country' IN
                   (
                    'Belgium', 'Netherlands', 'Luxemburg'
                       )
               THEN 'Domestic'

           WHEN 'posa_territory' IN ('CH', 'DE')
               AND 'posu_country' IN
                   (
                    'Austria', 'Switzerland', 'Germany'
                       )
               THEN 'Domestic'

           WHEN 'posa_territory' = 'DK'
               AND 'posu_country' IN
                   (
                    'Sweden', 'Denmark', 'Norway'
                       )
               THEN 'Domestic'

           WHEN 'posa_territory' = 'ES'
               AND 'posu_country' IN
                   (
                    'Portugal', 'Spain', 'Andorra'
                       )
               THEN 'Domestic'

           WHEN 'posa_territory' IN ('HK', 'ID', 'MY', 'SG')
               AND 'posu_country' IN
                   (
                    'Japan', 'Malaysia', 'Indonesia', 'Maldives', 'Thailand', 'Singapore', 'China'
                       )
               THEN 'Domestic'

           WHEN 'posa_territory' = 'IT'
               AND 'posu_country' = 'Italy'
               THEN 'Domestic'

           WHEN 'posa_territory' = 'NL'
               AND 'posu_country' IN
                   (
                    'Belgium', 'Netherlands', 'Luxemburg'
                       )
               THEN 'Domestic'

           WHEN 'posa_territory' = 'NO'
               AND 'posu_country' IN
                   (
                    'Sweden', 'Denmark', 'Norway'
                       )
               THEN 'Domestic'

           WHEN 'posa_territory' = 'SE'
               AND 'posu_country' = 'Sweden'
               THEN 'Domestic'

           WHEN 'posa_territory' = 'UK'
               AND 'posu_country' IN
                   (
                    'England', 'Wales/Cymru', 'Scotland', 'Ireland'
                       )
               THEN 'Domestic'

               WHEN 'posa_territory' = 'US'
               AND 'posu_country' IN
                   (
                    'Canada', 'USA'
                       )
               THEN 'Domestic'

           ELSE 'International'
           END AS travel_type



self_describing_task --include 'dv/dwh/transactional/se_sale.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
self_describing_task --include 'dv/dwh/transactional/tb_offer.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

self_describing_task --include 'se/data/dwh/tb_offer.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
self_describing_task --include 'se/data/dwh/se_sale_attributes.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
self_describing_task --include 'se/data/dwh/dim_sale.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'


/Users/robin/myrepos/one-data-pipeline/biapp/task_catalogue/se/data/dwh/tb_offer.py

/Users/robin/myrepos/one-data-pipeline/biapp/task_catalogue/dv/dwh/transactional/se_sale.py

SELECT * FROM data_vault_mvp_dev_robin.dwh.se_sale ss;
SELECT * FROM data_vault_mvp_dev_robin.dwh.tb_offer t;

SELECT DISTINCT POSA_TERRITORY FROM se.data.tb_offer t;

SELECT * FROM se_dev_robin.data.dim_sale ds;

SELECT * FROM data_vault_mvp.dwh.se_sale ss;

SELECT * FROM se.data.dim_sale ds