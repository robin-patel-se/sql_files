SELECT COUNT(*)
FROM (
         SELECT *
         FROM raw_vault_mvp.cms_mysql.promotion_territory
             QUALIFY row_number OVER (PARTITION BY promotion_territory.promotion_territories_id, promotion_territory.territory_id, promotion_territory.territories_idx ORDER BY promotion_territory.loaded_at) =
                     1
     );

SELECT COUNT(*)
FROM data_vault_mvp_dev_robin.cms_mysql_snapshots.promotion_territory_snapshot;

SELECT COUNT(*)
FROM data_vault_mvp.cms_mysql_snapshots.promotion_territory_snapshot;



SELECT *
FROM raw_vault_mvp.cms_mysql.promotion_territory c
         INNER JOIN (
    SELECT max(loaded_at) AS loaded_at FROM raw_vault_mvp.cms_mysql.promotion_territory
) a ON a.loaded_at = c.loaded_at;

------------------------------------------------------------------------------------------------------------------------


SELECT COUNT(*)
FROM (
         SELECT *
         FROM raw_vault_mvp.cms_mysql.promotion_sale ps
             QUALIFY row_number OVER (PARTITION BY ps.sale_id, ps.promotion_sales_id ,ps.sales_idx ORDER BY ps.loaded_at ) = 1
     );


SELECT *
FROM raw_vault_mvp.cms_mysql.promotion_sale ps
         INNER JOIN (
    SELECT max(loaded_at) AS loaded_at FROM raw_vault_mvp.cms_mysql.promotion_sale p
) a ON a.loaded_at = ps.loaded_at;

SELECT COUNT(*)
FROM data_vault_mvp_dev_robin.cms_mysql_snapshots.promotion_sale_snapshot;

SELECT COUNT(*)
FROM data_vault_mvp.cms_mysql_snapshots.promotion_sale_snapshot;

------------------------------------------------------------------------------------------------------------------------

SELECT COUNT(*)
FROM (
         SELECT *
         FROM raw_vault_mvp.cms_mysql.promotion_sales_force_ids ps
             QUALIFY row_number OVER (PARTITION BY ps.promotion_id, ps.sales_force_ids_idx ,ps.sales_force_ids_idx ORDER BY ps.loaded_at ) =
                     1
     );


SELECT *
FROM raw_vault_mvp.cms_mysql.promotion_sales_force_ids ps
         INNER JOIN (
    SELECT max(loaded_at) AS loaded_at FROM raw_vault_mvp.cms_mysql.promotion_sales_force_ids p
) a ON a.loaded_at = ps.loaded_at;

SELECT COUNT(*)
FROM data_vault_mvp_dev_robin.cms_mysql_snapshots.promotion_sales_force_ids_snapshot;

SELECT COUNT(*)
FROM data_vault_mvp.cms_mysql_snapshots.promotion_sales_force_ids_snapshot;

------------------------------------------------------------------------------------------------------------------------


SELECT *
FROM raw_vault_mvp.cms_mysql.promotion_promotion_translation ppt
         INNER JOIN (
    SELECT max(loaded_at) AS loaded_at FROM raw_vault_mvp.cms_mysql.promotion_promotion_translation t
) a ON a.loaded_at = ppt.loaded_at;


SELECT *
FROM raw_vault_mvp.cms_mysql.promotion_translation pt;

CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.cms_mysql.promotion_territory CLONE raw_vault_mvp.cms_mysql.promotion_territory;
CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.cms_mysql.promotion_sale CLONE raw_vault_mvp.cms_mysql.promotion_sale;
CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.cms_mysql.promotion_sales_force_ids CLONE raw_vault_mvp.cms_mysql.promotion_sales_force_ids;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.promotion_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.promotion_snapshot;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.promotion_promotion_translation_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.promotion_promotion_translation_snapshot;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.promotion_translation_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.promotion_translation_snapshot;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.promotion_sale_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.promotion_sale_snapshot;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.sale_territory_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.sale_territory_snapshot;
CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.cms_mysql.sale_territory CLONE raw_vault_mvp.cms_mysql.sale_territory;

CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.base_sale CLONE hygiene_snapshot_vault_mvp.cms_mysql.base_sale;
CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.sale CLONE hygiene_snapshot_vault_mvp.cms_mysql.sale;


------------------------------------------------------------------------------------------------------------------------

self_describing_task --include 'dv/cms_snapshots/cms_mysql_snapshot_bulk_wave3.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

SELECT ss.salesforce_opportunity_id,
       psi.sales_force_ids_string,
       psi.promotion_id,
       psi.sales_force_ids_idx,
       p.*

FROM data_vault_mvp.dwh.se_sale ss
         LEFT JOIN data_vault_mvp_dev_robin.cms_mysql_snapshots.promotion_sales_force_ids_snapshot psi
                   ON ss.salesforce_opportunity_id = psi.sales_force_ids_string
         LEFT JOIN data_vault_mvp_dev_robin.cms_mysql_snapshots.promotion_snapshot p ON psi.promotion_id = p.id;

------------------------------------------------------------------------------------------------------------------------
--NDM
SELECT 'A' || bs.id                                                        AS sale_id,
       pr.id                                                               AS promotion_id,
       bs.salesforce_opportunity_id,
       bs.territory_id,
       pr.id = FIRST_VALUE(pr.id) OVER (PARTITION BY bs.id ORDER BY pr.id) AS sale_active_promotion,
       ts.name                                                             AS territory,
       pr.name                                                             AS promotion_name,
       pr.active,
       pr.start_date,
       pr.end_date,
       pt.description,
       pt.label
FROM data_vault_mvp.cms_mysql_snapshots.base_sale_snapshot bs
         INNER JOIN data_vault_mvp.cms_mysql_snapshots.promotion_sales_force_ids_snapshot psf
                    ON psf.sales_force_ids_string = bs.salesforce_opportunity_id
         INNER JOIN data_vault_mvp.cms_mysql_snapshots.promotion_snapshot pr ON pr.id = psf.promotion_id
         INNER JOIN data_vault_mvp.cms_mysql_snapshots.promotion_promotion_translation_snapshot ppt
                    ON ppt.promotion_translations_id = pr.id
         INNER JOIN data_vault_mvp.cms_mysql_snapshots.promotion_translation_snapshot pt
                    ON pt.id = ppt.promotion_translation_id
                        AND pt.territory_id = bs.territory_id
         INNER JOIN data_vault_mvp.cms_mysql_snapshots.promotion_territory_snapshot prt
                    ON prt.promotion_territories_id = pr.id
                        AND prt.territory_id = bs.territory_id
         INNER JOIN data_vault_mvp.cms_mysql_snapshots.territory_snapshot ts ON prt.territory_id = ts.id
WHERE bs.id = 16689;

--ODM
SELECT s.id    AS sale_id,
       pr.id   AS promotion_id,
       s.salesforce_opportunity_id,
       st.territory_id,
       --active promotion flag
       t.name  AS territory,
       pr.name AS promotion_name,
       pr.active,
       pr.start_date,
       pr.end_date,
       pt.description,
       pt.label
FROM data_vault_mvp.cms_mysql_snapshots.sale_snapshot s
         INNER JOIN data_vault_mvp.cms_mysql_snapshots.sale_territory_snapshot st ON st.sale_id = s.id
         INNER JOIN data_vault_mvp.cms_mysql_snapshots.territory_snapshot t ON st.territory_id = t.id
         INNER JOIN data_vault_mvp.cms_mysql_snapshots.promotion_sale_snapshot ps ON ps.sale_id = s.id
         INNER JOIN data_vault_mvp.cms_mysql_snapshots.promotion_snapshot pr ON pr.id = ps.promotion_sales_id
         INNER JOIN data_vault_mvp.cms_mysql_snapshots.promotion_promotion_translation_snapshot ppt
                    ON ppt.promotion_translations_id = pr.id
         INNER JOIN data_vault_mvp.cms_mysql_snapshots.promotion_translation_snapshot pt ON pt.id = ppt.promotion_translation_id
    AND pt.territory_id = st.territory_id
         INNER JOIN data_vault_mvp.cms_mysql_snapshots.promotion_territory_snapshot prt ON prt.promotion_territories_id = pr.id
    AND prt.territory_id = st.territory_id
WHERE s.id = 112210;


self_describing_task --include 'dv/cms_snapshots/cms_mysql_sale_territory.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

self_describing_task --include 'dv/dwh/transactional/se_promotions.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

SELECT *
FROM data_vault_mvp_dev_robin.dwh.se_promotion__step01__model_ndm
    QUALIFY count(*) OVER (PARTITION BY promotion_id, se_sale_id) > 1;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.se_promotion__step01__model_ndm sps01mn
WHERE sps01mn.se_sale_id = 'A17410';

SELECT 'A' || bs.id                                                        AS sale_id,
       pr.id                                                               AS promotion_id,
       bs.salesforce_opportunity_id,
       bs.territory_id,
       pr.id = FIRST_VALUE(pr.id) OVER (PARTITION BY bs.id ORDER BY pr.id) AS sale_active_promotion,
       ts.name                                                             AS territory,
       pr.name                                                             AS promotion_name,
       pr.active,
       pr.start_date,
       pr.end_date,
       pt.*,
       pt.description,
       pt.label
FROM hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.base_sale bs
         INNER JOIN data_vault_mvp_dev_robin.cms_mysql_snapshots.promotion_sales_force_ids_snapshot psf
                    ON psf.sales_force_ids_string = bs.salesforce_opportunity_id
         INNER JOIN data_vault_mvp_dev_robin.cms_mysql_snapshots.promotion_snapshot pr ON pr.id = psf.promotion_id
         INNER JOIN data_vault_mvp_dev_robin.cms_mysql_snapshots.promotion_territory_snapshot prt
                    ON pr.id = prt.promotion_territories_id
                        AND bs.territory_id = prt.territory_id
         INNER JOIN data_vault_mvp_dev_robin.cms_mysql_snapshots.promotion_promotion_translation_snapshot ppt
                    ON prt.promotion_territories_id = ppt.promotion_translations_id
         INNER JOIN data_vault_mvp_dev_robin.cms_mysql_snapshots.promotion_translation_snapshot pt
                    ON pt.id = ppt.promotion_translation_id
                        AND pt.territory_id = bs.territory_id

         INNER JOIN data_vault_mvp_dev_robin.cms_mysql_snapshots.territory_snapshot ts ON prt.territory_id = ts.id
WHERE bs.id = 17410;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.se_promotion__step01__model_ndm
WHERE se_promotion__step01__model_ndm.se_sale_id = 'A16689';

SELECT *
FROM data_vault_mvp_dev_robin.dwh.se_promotion__step02__model_odm
    QUALIFY count(*) OVER (PARTITION BY promotion_id, se_sale_id) > 1;



self_describing_task --include 'dv/cms_snapshots/cms_mysql_snapshot_bulk_wave3.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'


CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.cms_mysql.promotion CLONE raw_vault_mvp.cms_mysql.promotion;
CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.cms_mysql.promotion_promotion_translation CLONE raw_vault_mvp.cms_mysql.promotion_promotion_translation;
CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.cms_mysql.promotion_sale CLONE raw_vault_mvp.cms_mysql.promotion_sale;
CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.cms_mysql.promotion_sales_force_ids CLONE raw_vault_mvp.cms_mysql.promotion_sales_force_ids;
CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.cms_mysql.promotion_territory CLONE raw_vault_mvp.cms_mysql.promotion_territory;
CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.cms_mysql.promotion_translation CLONE raw_vault_mvp.cms_mysql.promotion_translation;


SELECT *
FROM raw_vault_mvp_dev_robin.cms_mysql.promotion_sales_force_ids
    QUALIFY count(*) OVER (PARTITION BY promotion_id, sales_force_ids_string) > 1
ORDER BY sales_force_ids_string;

SELECT get_ddl('table', 'raw_vault_mvp_dev_robin.cms_mysql.promotion_sales_force_ids');

--snapshot on loaded at = max loaded at


CREATE OR REPLACE TABLE promotion_sales_force_ids CLUSTER BY (TO_DATE(schedule_tstamp))
(

    promotion_id           NUMBER,
    sales_force_ids_string VARCHAR,
    sales_force_ids_idx    NUMBER,

    PRIMARY KEY (dataset_name, dataset_source, schedule_interval, schedule_tstamp, run_tstamp, filename, file_row_number)
);


SELECT *
FROM raw_vault_mvp_dev_robin.cms_mysql.promotion_translation;

self_describing_task --include 'hygiene/cms_mysql/promotion_sales_force_ids.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
self_describing_task --include 'hygiene_snapshots/cms_mysql/promotion_sales_force_ids.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

SELECT *
FROM hygiene_vault_mvp_dev_robin.cms_mysql.promotion_sales_force_ids;

------------------------------------------------------------------------------------------------------------------------

SELECT get_ddl('table', 'raw_vault_mvp_dev_robin.cms_mysql.promotion');

CREATE OR REPLACE TABLE promotion CLUSTER BY (TO_DATE(schedule_tstamp))
(

    id           NUMBER,
    version      NUMBER,
    active       NUMBER,
    date_created TIMESTAMP,
    end_date     TIMESTAMP,
    last_updated TIMESTAMP,
    name         VARCHAR,
    start_date   TIMESTAMP
);

self_describing_task --include 'hygiene/cms_mysql/promotion.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
self_describing_task --include 'hygiene_snapshots/cms_mysql/promotion.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

------------------------------------------------------------------------------------------------------------------------
SELECT get_ddl('table', 'raw_vault_mvp_dev_robin.cms_mysql.promotion_promotion_translation');

CREATE OR REPLACE TABLE promotion_promotion_translation CLUSTER BY (TO_DATE(schedule_tstamp))
(

    promotion_translations_id NUMBER,
    promotion_translation_id  NUMBER
);

self_describing_task --include 'hygiene/cms_mysql/promotion_promotion_translation.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
self_describing_task --include 'hygiene_snapshots/cms_mysql/promotion_promotion_translation.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

------------------------------------------------------------------------------------------------------------------------
SELECT get_ddl('table', 'raw_vault_mvp.cms_mysql.promotion_sale');

CREATE OR REPLACE TABLE promotion_sale CLUSTER BY (TO_DATE(schedule_tstamp))
(
    promotion_sales_id NUMBER,
    sale_id            NUMBER,
    sales_idx          NUMBER,
);

self_describing_task --include 'hygiene/cms_mysql/promotion_sale.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
self_describing_task --include 'hygiene_snapshots/cms_mysql/promotion_sale.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'


SELECT *
FROM hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.promotion_sale
    QUALIFY count(*) OVER (PARTITION BY promotion_id, sale_id) > 1

------------------------------------------------------------------------------------------------------------------------

SELECT get_ddl('table', 'raw_vault_mvp.cms_mysql.promotion_territory');

CREATE OR REPLACE TABLE promotion_territory CLUSTER BY (TO_DATE(schedule_tstamp))
(

    promotion_territories_id NUMBER,
    territory_id             NUMBER,
    territories_idx          NUMBER
);

self_describing_task --include 'hygiene/cms_mysql/promotion_territory.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
self_describing_task --include 'hygiene_snapshots/cms_mysql/promotion_territory.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

------------------------------------------------------------------------------------------------------------------------

SELECT get_ddl('table', 'raw_vault_mvp.cms_mysql.promotion_translation');

CREATE OR REPLACE TABLE promotion_translation CLUSTER BY (TO_DATE(schedule_tstamp))
(

    id                NUMBER,
    version           NUMBER,
    description       VARCHAR,
    font_awesome_code VARCHAR,
    hex_code          VARCHAR,
    label             VARCHAR,
    territory_id      NUMBER,
    PRIMARY KEY (dataset_name, dataset_source, schedule_interval, schedule_tstamp, run_tstamp, filename, file_row_number)
);

self_describing_task --include 'hygiene/cms_mysql/promotion_translation.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
self_describing_task --include 'hygiene_snapshots/cms_mysql/promotion_translation.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

------------------------------------------------------------------------------------------------------------------------
DROP TABLE raw_vault_mvp_dev_robin.cms_mysql.promotion;
DROP TABLE raw_vault_mvp_dev_robin.cms_mysql.promotion_promotion_translation;
DROP TABLE raw_vault_mvp_dev_robin.cms_mysql.promotion_sale;
DROP TABLE raw_vault_mvp_dev_robin.cms_mysql.promotion_sales_force_ids;
DROP TABLE raw_vault_mvp_dev_robin.cms_mysql.promotion_territory;
DROP TABLE raw_vault_mvp_dev_robin.cms_mysql.promotion_translation;

self_describing_task --include 'dv/dwh/transactional/se_promotions.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'



SELECT *
FROM data_vault_mvp_dev_robin.dwh.se_promotion
WHERE se_promotion.sale_active_promotion = FALSE;


self_describing_task --include 'se/data/dwh/se_sale_attributes.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

SELECT *
FROM se_dev_robin.data.se_sale_attributes ssa;

SELECT *
FROM se.data.se_room_rates srr;


SELECT sb.check_in_date::DATE AS check_in_date,
       ssa.product_configuration,
       count(*)               AS bookings
FROM se.data.se_booking sb
         INNER JOIN se.data.se_sale_attributes ssa ON sb.sale_id = ssa.se_sale_id
WHERE sb.booking_status = 'COMPLETE'
  AND sb.check_in_date >= current_date - 60
  AND sb.check_in_date <= current_date + 8
GROUP BY 1, 2;

SELECT ssa.product_configuration,
       count(*) AS bookings
FROM se.data.se_booking sb
         INNER JOIN se.data.se_sale_attributes ssa ON sb.sale_id = ssa.se_sale_id
WHERE sb.booking_status = 'COMPLETE'
  AND sb.check_in_date >= current_date

GROUP BY 1;


SELECT get_ddl('table', 'se.data_pii.crm_events_clicks');


CREATE OR REPLACE VIEW crm_email_segments
    COPY GRANTS
AS
SELECT SHA2(COALESCE(es.send_id::VARCHAR, '') ||
            COALESCE(es.list_id::VARCHAR, '')
           ) AS email_segment_key,
       es.send_id,
       es.list_id,
       es.data_source_name,
       es.segment,
       es.mapping_type
FROM data_vault_mvp.dwh.crm_email_segments es
;


SELECT *
FROM data_vault_mvp.dwh.crm_email_segments;


CREATE OR REPLACE VIEW crm_events_clicks
    COPY GRANTS
AS
SELECT ec.event_hash,
       ec.event_date,
       ec.event_tstamp,
       ec.data_source_key,
       ec.shiro_user_id,
       ec.client_id,
       ec.send_id,
       ec.subscriber_key,
       ec.email_address,
       ec.subscriber_id,
       ec.list_id,
       ec.event_date__o,
       ec.event_type,
       ec.send_url_id,
       ec.url_id,
       ec.url,
       ec.alias,
       ec.batch_id,
       ec.triggered_send_external_key,
       ec.ip_address,
       ec.country,
       ec.region,
       ec.city,
       ec.latitude,
       ec.longitude,
       ec.metrocode,
       ec.area_code,
       ec.browser,
       ec.email_client,
       ec.operating_system,
       ec.device,
       SHA2(COALESCE(ec.send_id::VARCHAR, '') ||
            COALESCE(ec.list_id::VARCHAR, '')
           ) AS email_segment_key
FROM hygiene_snapshot_vault_mvp.sfmc.events_clicks ec
;

SELECT *
FROM hygiene_snapshot_vault_mvp.sfmc.events_clicks


SELECT get_ddl('table', 'se.data_pii.crm_jobs_list');


SELECT *
FROM se.data_pii.crm_events_sends ces;



CREATE OR REPLACE VIEW crm_events_opens
    COPY GRANTS
AS
SELECT eo.event_hash,
       eo.event_date,
       eo.event_tstamp,
       eo.data_source_key,
       eo.shiro_user_id,
       eo.client_id,
       eo.send_id,
       eo.subscriber_key,
       eo.email_address,
       eo.subscriber_id,
       eo.list_id,
       eo.event_date__o,
       eo.event_type,
       eo.batch_id,
       eo.triggered_send_external_key,
       eo.ip_address,
       eo.country,
       eo.region,
       eo.city,
       eo.latitude,
       eo.longitude,
       eo.metrocode,
       eo.area_code,
       eo.browser,
       eo.email_client,
       eo.operating_system,
       eo.device,
       eo.email_segment_key
FROM hygiene_snapshot_vault_mvp.sfmc.events_opens_plus_inferred eo
;



CREATE OR REPLACE VIEW crm_jobs_list
    COPY GRANTS
AS
SELECT jl.send_id,
       jl.scheduled_date,
       jl.scheduled_tstmap,
       jl.sent_date,
       jl.sent_tstamp,
       jl.email_name,
       jl.is_email_name_remapped,
       jl.mapped_crm_date,
       jl.mapped_territory,
       jl.mapped_objective,
       jl.mapped_platform,
       jl.mapped_campaign,
       jl.client_id,
       jl.from_name,
       jl.from_email,
       jl.sched_time,
       jl.sent_time,
       jl.subject,
       jl.email_name__o,
       jl.triggered_send_external_key,
       jl.send_definition_external_key,
       jl.job_status,
       jl.preview_url,
       jl.is_multipart,
       jl.additional
FROM hygiene_snapshot_vault_mvp.sfmc.jobs_list jl
;

SELECT *
FROM se.data_pii.crm_events_opens ceo;
/
USERS/
robin/
myrepos/
one-DATA-pipeline/
biapp/
task_catalogue/
se/
DATA/
crm/
create_se_data_objects_crm.py

self_describing_task --include 'se/data/crm/create_se_data_objects_crm.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
self_describing_task --include 'se/data/crm/crm_email_segments.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
self_describing_task --include 'se/data/crm/crm_events_clicks.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
self_describing_task --include 'se/data/crm/crm_events_opens.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
self_describing_task --include 'se/data/crm/crm_events_sends.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
self_describing_task --include 'se/data/crm/crm_jobs_list.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

DROP TABLE se_dev_robin.data.crm_historic_data_sources;



CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.promotion CLONE hygiene_snapshot_vault_mvp.cms_mysql.promotion;
CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.promotion_promotion_translation CLONE hygiene_snapshot_vault_mvp.cms_mysql.promotion_promotion_translation;
CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.promotion_sale CLONE hygiene_snapshot_vault_mvp.cms_mysql.promotion_sale;
CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.promotion_sales_force_ids CLONE hygiene_snapshot_vault_mvp.cms_mysql.promotion_sales_force_ids;
CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.promotion_territory CLONE hygiene_snapshot_vault_mvp.cms_mysql.promotion_territory;
CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.promotion_translation CLONE hygiene_snapshot_vault_mvp.cms_mysql.promotion_translation;

self_describing_task --include 'dv/dwh/transactional/se_promotions.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'


SELECT *
FROM data_vault_mvp_dev_robin.dwh.se_promotion__step01__model_ndm
    QUALIFY COUNT(*) OVER (PARTITION BY se_sale_id, promotion_id) > 1
ORDER BY se_sale_id, promotion_id;


SELECT *
FROM data_vault_mvp_dev_robin.dwh.se_promotion__step02__model_odm
    QUALIFY COUNT(*) OVER (PARTITION BY se_sale_id, promotion_id) > 1
ORDER BY se_sale_id, promotion_id;

WITH promotion_data AS (
    SELECT s.id::VARCHAR                                                                                  AS se_sale_id,
           pr.id                                                                                          AS promotion_id,
           pr.name                                                                                        AS promotion_name,
           s.salesforce_opportunity_id,
           st.territory_id,
           t.name                                                                                         AS territory,
           pr.active AND pr.id = FIRST_VALUE(pr.id)
                                             OVER (PARTITION BY s.id, pr.active ORDER BY pr.date_created) AS sale_active_promotion,
           pr.active,
           pr.date_created,
           pr.start_date,
           pr.end_date,
           pt.description,
           pt.label
    FROM hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.sale s
             INNER JOIN data_vault_mvp_dev_robin.cms_mysql_snapshots.sale_territory_snapshot st ON st.sale_id = s.id
             INNER JOIN data_vault_mvp_dev_robin.cms_mysql_snapshots.territory_snapshot t ON st.territory_id = t.id
             INNER JOIN hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.promotion_sale ps ON ps.sale_id = s.id
             INNER JOIN hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.promotion pr ON pr.id = ps.promotion_sales_id
             INNER JOIN hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.promotion_promotion_translation ppt
                        ON ppt.promotion_translations_id = pr.id
             INNER JOIN hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.promotion_translation pt
                        ON pt.id = ppt.promotion_translation_id AND pt.territory_id = st.territory_id
    WHERE s.id = 100554
)
SELECT pd.se_sale_id,
       pd.promotion_id,
       pd.promotion_name,
       pd.salesforce_opportunity_id,
       LISTAGG(pd.territory_id, ', ') AS territory_id,
       LISTAGG(pd.territory, ', ')    AS territory,
       pd.sale_active_promotion,
       pd.active,
       pd.date_created,
       pd.start_date,
       pd.end_date,
       pd.description,
       pd.label
FROM promotion_data pd
GROUP BY 1, 2, 3, 4, 7, 8, 9, 10, 11, 12, 13;


--promotion
SELECT COUNT(*)
FROM hygiene_snapshot_vault_mvp.cms_mysql.promotion;
--promotion_promotion_translation
SELECT COUNT(*)
FROM hygiene_snapshot_vault_mvp.cms_mysql.promotion_promotion_translation;
--promotion_sale
SELECT COUNT(*)
FROM hygiene_snapshot_vault_mvp.cms_mysql.promotion_sale;
--promotion_sales_force_ids
SELECT COUNT(*)
FROM hygiene_snapshot_vault_mvp.cms_mysql.promotion_sales_force_ids;
--promotion_territory
SELECT COUNT(*)
FROM hygiene_snapshot_vault_mvp.cms_mysql.promotion_territory;
--promotion_translation
SELECT COUNT(*)
FROM hygiene_snapshot_vault_mvp.cms_mysql.promotion_translation;


SELECT *
FROM data_vault_mvp.dwh.se_promotion sp
WHERE sp.se_sale_id = '112210';
SELECT *
FROM data_vault_mvp_dev_robin.dwh.se_promotion sp;

SELECT *
FROM se.data.se_sale_attributes ssa
WHERE ssa.promotion_label IS NOT NULL;



SELECT *
FROM data_vault_mvp.dwh.se_promotion sp
    QUALIFY COUNT(*) OVER (PARTITION BY se_sale_id, promotion_id) > 1
ORDER BY se_sale_id, promotion_id;

SELECT *
FROM data_vault_mvp.dwh.se_promotion__step01__model_ndm
    QUALIFY COUNT(*) OVER (PARTITION BY se_sale_id, promotion_id) > 1
ORDER BY se_sale_id, promotion_id;


SELECT *
FROM data_vault_mvp.dwh.se_promotion__step02__model_odm
    QUALIFY COUNT(*) OVER (PARTITION BY se_sale_id, promotion_id) > 1
ORDER BY se_sale_id, promotion_id;



WITH promotion_data AS (
    SELECT s.id::VARCHAR                                                                                         AS se_sale_id,
           pr.id                                                                                                 AS promotion_id,
           pr.name                                                                                               AS promotion_name,
           s.salesforce_opportunity_id,
           st.territory_id,
           t.name                                                                                                AS territory,
           pr.active AND pr.id = FIRST_VALUE(pr.id)
                                             OVER (PARTITION BY s.id, pr.active ORDER BY pr.date_created)        AS sale_active_promotion,
           pr.active,
           pr.date_created,
           pr.start_date,
           pr.end_date,
           pt.description,
           pt.label
    FROM hygiene_snapshot_vault_mvp.cms_mysql.sale s
             INNER JOIN data_vault_mvp.cms_mysql_snapshots.sale_territory_snapshot st ON st.sale_id = s.id
             INNER JOIN data_vault_mvp.cms_mysql_snapshots.territory_snapshot t ON st.territory_id = t.id
             INNER JOIN hygiene_snapshot_vault_mvp.cms_mysql.promotion_sale ps ON ps.sale_id = s.id
             INNER JOIN hygiene_snapshot_vault_mvp.cms_mysql.promotion pr ON pr.id = ps.promotion_sales_id
             INNER JOIN hygiene_snapshot_vault_mvp.cms_mysql.promotion_promotion_translation ppt
                        ON ppt.promotion_translations_id = pr.id
             INNER JOIN hygiene_snapshot_vault_mvp.cms_mysql.promotion_translation pt
                        ON pt.id = ppt.promotion_translation_id AND pt.territory_id = st.territory_id
), agg AS (
    SELECT pd.se_sale_id,
           pd.promotion_id,
           pd.promotion_name,
           pd.salesforce_opportunity_id,
           LISTAGG(pd.territory_id, ', ') AS territory_id,
           LISTAGG(pd.territory, ', ')    AS territory,
           pd.sale_active_promotion,
           pd.active,
           pd.date_created,
           pd.start_date,
           pd.end_date,
           LISTAGG(pd.description, ', ')  AS description,
           LISTAGG(pd.label, ', ')        AS label
    FROM promotion_data pd
    GROUP BY 1, 2, 3, 4, 7, 8, 9, 10, 11
)
SELECT * FROM agg
QUALIFY COUNT(*) OVER (PARTITION BY se_sale_id, promotion_id) > 1;

self_describing_task --include 'dv/dwh/transactional/se_promotions.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'