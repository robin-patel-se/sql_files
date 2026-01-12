CREATE OR REPLACE TABLE scratch.robinpatel.live_sales AS
WITH offer_allocation_live_offer_min_start_date AS (
    SELECT o.sale_id         AS offer_sale_id,
           MAX(a.start_date) AS allocation_start_date
    FROM data_vault_mvp.cms_mysql_snapshots.offer_snapshot o
             INNER JOIN data_vault_mvp.cms_mysql_snapshots.allocation_snapshot a
                        ON a.offer_id = o.id
    WHERE o.active = 1
    GROUP BY 1
),
     active_sale_territory AS (
         --list of odm sale ids that are live in at least one territory
         SELECT DISTINCT sale_id
         FROM data_vault_mvp.cms_mysql_snapshots.sale_territory_snapshot
         --additional where filter? or do we need to change ingest to snapshot, due to deletions
     )
-- regular (old data model) sales currently live
SELECT s.salesforce_opportunity_id AS sf_opp_id,
       s.id::VARCHAR               AS saleid,
       s.start_date,
       s.end_date,
       CASE
           WHEN s.active = 1
               AND s.start_date <= current_date
               AND s.end_date > current_date
               -- offers with an allocation available in the future
               AND o.allocation_start_date >= current_date
               THEN 1
           ELSE 0
           END                     AS is_live,
       'odm'::VARCHAR              AS source
FROM data_vault_mvp.cms_mysql_snapshots.sale_snapshot s
         --inner join filter for sales with a live offer
         INNER JOIN offer_allocation_live_offer_min_start_date o
                    ON o.offer_sale_id = s.id
    -- inner join filter for odm sales that are live in at least one territory
         INNER JOIN active_sale_territory t
                    ON t.sale_id = s.id
UNION
-- connected (new data model) sales currently live
SELECT s.salesforce_opportunity_id AS sf_opp_id,
       'A' || s.id                 AS saleid,
       s.start_date,
       s.end_date,
       CASE
           WHEN s.active = 1
               AND s.start_date <= current_date
               AND s.end_date > current_date
               THEN 1
           ELSE 0
           END                     AS is_live,
       'ndm'::VARCHAR              AS source
FROM data_vault_mvp.cms_mysql_snapshots.base_sale_snapshot s
--remove TB sales
WHERE s.class != 'com.flashsales.sale.ConnectedWebRedirectSale';

------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE scratch.robinpatel.live_sales2 AS
WITH offer_allocation_live_offer_min_start_date AS (
    SELECT o.sale_id         AS offer_sale_id,
           MAX(a.start_date) AS allocation_start_date
    FROM data_vault_mvp.cms_mysql_snapshots.offer_snapshot o
             INNER JOIN data_vault_mvp.cms_mysql_snapshots.allocation_snapshot a
                        ON a.offer_id = o.id
    WHERE o.active = 1
    GROUP BY 1
),
     active_sale_territory AS (
         --list of odm sale ids that are live in at least one territory
         SELECT DISTINCT sale_id
         FROM data_vault_mvp.cms_mysql_snapshots.sale_territory_snapshot
         --additional where filter? or do we need to change ingest to snapshot, due to deletions
     )
-- regular (old data model) sales currently live
SELECT s.salesforce_opportunity_id AS sf_opp_id,
       s.id::VARCHAR               AS saleid,
       s.start_date,
       s.end_date,
       CASE
           WHEN s.active = 1
               AND s.start_date <= current_date
               AND s.end_date > current_date
               -- active offers with an allocation available in the future
               AND o.allocation_start_date >= current_date
               --offers live in at least one territory
               AND t.sale_id IS NOT NULL
               THEN 1
           ELSE 0
           END                     AS is_live,
       'odm'::VARCHAR              AS source
FROM data_vault_mvp.cms_mysql_snapshots.sale_snapshot s
         LEFT JOIN offer_allocation_live_offer_min_start_date o
                   ON o.offer_sale_id = s.id
         LEFT JOIN active_sale_territory t
                   ON t.sale_id = s.id
UNION
-- connected (new data model) sales currently live
SELECT s.salesforce_opportunity_id AS sf_opp_id,
       'A' || s.id                 AS saleid,
       s.start_date,
       s.end_date,
       CASE
           WHEN s.active = 1
               AND s.start_date <= current_date
               AND s.end_date > current_date
               THEN 1
           ELSE 0
           END                     AS is_live,
       'ndm'::VARCHAR              AS source
FROM data_vault_mvp.cms_mysql_snapshots.base_sale_snapshot s
--remove TB sales
WHERE s.class != 'com.flashsales.sale.ConnectedWebRedirectSale';


SELECT source, is_live, count(*)
FROM scratch.robinpatel.live_sales l
GROUP BY 1, 2;

SELECT source, count(*)
FROM scratch.robinpatel.live_sales2 l
GROUP BY 1;


SELECT s.data_model,
       count(*)
FROM data_vault_mvp_dev_robin.dwh.se_sale s
GROUP BY 1;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.se_sale s
WHERE sale_name LIKE '%|%';

SELECT s.sale_id, count(*)
FROM hygiene_snapshot_vault_mvp.cms_mysql.sale s
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.sale_company_snapshot sc ON s.sale_id = sc.sale_id AND s.type = 'HOTEL'
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.company_snapshot c ON c.id = sc.company_id
GROUP BY 1
HAVING count(*) > 1

SELECT *
FROM scratch.robinpatel.live_sales2 l
WHERE l.is_live = 1;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.se_sale s
WHERE sale_active;

SELECT count(*)
FROM data_vault_mvp.cms_mysql_snapshots.sale_snapshot ss;

SELECT count(*)
FROM hygiene_snapshot_vault_mvp.cms_mysql.sale s;

