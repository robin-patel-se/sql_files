SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_touch_utm_referrer mtur
WHERE mtur.utm_medium = 'facebookads'
   OR mtur.click_id IS NULL;


SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel mtmc
WHERE mtmc.touch_mkt_channel IN ('Other', 'Partner');

CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_utm_referrer AS
    (
        SELECT mtur.*
        FROM data_vault_mvp.single_customer_view_stg.module_touch_utm_referrer mtur
                 INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes mtba
                            ON mtur.touch_id = mtba.touch_id
        WHERE DATE_TRUNC(WEEK, mtba.touch_start_tstamp) = '2020-02-10'
           OR DATE_TRUNC(WEEK, mtba.touch_start_tstamp) = '2020-11-02'
    );



CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel;
TRUNCATE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel;


CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification CLONE data_vault_mvp.single_customer_view_stg.module_touchification;
TRUNCATE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.affiliate_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.affiliate_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.territory_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.territory_snapshot;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes AS (
    SELECT *
    FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
    WHERE DATE_TRUNC(WEEK, mtba.touch_start_tstamp) = '2020-02-10'
       OR DATE_TRUNC(WEEK, mtba.touch_start_tstamp) = '2020-11-02'
);
self_describing_task --include 'dv/dwh/events/05_touch_channelling/02_module_touch_marketing_channel.py'  --method 'run' --start '2020-02-01 00:00:00' --end '2020-02-01 00:00:00'



CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel_updated CLONE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel_regular AS
SELECT mtmc.*
FROM data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel mtmc
         INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes mtba
                    ON mtmc.touch_id = mtba.touch_id
WHERE DATE_TRUNC(WEEK, mtba.touch_start_tstamp) = '2020-02-10'
   OR DATE_TRUNC(WEEK, mtba.touch_start_tstamp) = '2020-11-02'
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes_test CLONE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes;

--updated channelling
SELECT date_trunc(WEEK, mtba.touch_start_tstamp) AS week,
       mtmc.touch_mkt_channel,
       count(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel_updated mtmc
         INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes mtba
                    ON mtmc.touch_id = mtba.touch_id
GROUP BY 1, 2;

--regular channelling
SELECT date_trunc(WEEK, mtba.touch_start_tstamp) AS week,
       mtmc.touch_mkt_channel,
       count(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel_regular mtmc
         INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes mtba
                    ON mtmc.touch_id = mtba.touch_id
GROUP BY 1, 2;

CREATE SCHEMA collab.scv_channelling_update_202011;
GRANT USAGE ON SCHEMA collab.scv_channelling_update_202011 TO ROLE personal_role__gianniraftis;
GRANT USAGE ON SCHEMA collab.scv_channelling_update_202011 TO ROLE personal_role__roseyin;
GRANT USAGE ON SCHEMA collab.scv_channelling_update_202011 TO ROLE personal_role__rumyanamiteva;
GRANT USAGE ON SCHEMA collab.scv_channelling_update_202011 TO ROLE personal_role__michaeldobinson;
GRANT SELECT ON ALL TABLES IN SCHEMA collab.scv_channelling_update_202011 TO ROLE personal_role__gianniraftis;
GRANT SELECT ON ALL TABLES IN SCHEMA collab.scv_channelling_update_202011 TO ROLE personal_role__roseyin;
GRANT SELECT ON ALL TABLES IN SCHEMA collab.scv_channelling_update_202011 TO ROLE personal_role__rumyanamiteva;
GRANT SELECT ON ALL TABLES IN SCHEMA collab.scv_channelling_update_202011 TO ROLE personal_role__michaeldobinson;
GRANT SELECT ON ALL VIEWS IN SCHEMA collab.scv_channelling_update_202011 TO ROLE personal_role__gianniraftis;
GRANT SELECT ON ALL VIEWS IN SCHEMA collab.scv_channelling_update_202011 TO ROLE personal_role__roseyin;
GRANT SELECT ON ALL VIEWS IN SCHEMA collab.scv_channelling_update_202011 TO ROLE personal_role__rumyanamiteva;
GRANT SELECT ON ALL VIEWS IN SCHEMA collab.scv_channelling_update_202011 TO ROLE personal_role__michaeldobinson;

CREATE OR REPLACE VIEW collab.scv_channelling_update_202011.module_touch_marketing_channel_updated AS
SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel_updated;

CREATE OR REPLACE VIEW collab.scv_channelling_update_202011.module_touch_marketing_channel_regular AS
SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel_regular;

CREATE OR REPLACE VIEW collab.scv_channelling_update_202011.module_touch_basic_attributes_test AS
SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes_test;


--updated channelling
SELECT date_trunc(WEEK, mtba.touch_start_tstamp) AS week,
       mtmc.touch_mkt_channel,
       count(*)
FROM collab.scv_channelling_update_202011.module_touch_marketing_channel_updated mtmc
         INNER JOIN collab.scv_channelling_update_202011.module_touch_basic_attributes_test mtba
                    ON mtmc.touch_id = mtba.touch_id
GROUP BY 1, 2;

--regular channelling
SELECT date_trunc(WEEK, mtba.touch_start_tstamp) AS week,
       mtmc.touch_mkt_channel,
       count(*)
FROM collab.scv_channelling_update_202011.module_touch_marketing_channel_regular mtmc
         INNER JOIN collab.scv_channelling_update_202011.module_touch_basic_attributes_test mtba
                    ON mtmc.touch_id = mtba.touch_id
GROUP BY 1, 2;

SELECT *
FROM hygiene_vault_mvp.finance_gsheets.manual_refunds mr;

SELECT *
FROM se.data.tb_offer t
WHERE t.sale_active;

TRUNCATE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel;
self_describing_task --include 'dv/dwh/events/05_touch_channelling/02_module_touch_marketing_channel.py'  --method 'run' --start '2020-02-01 00:00:00' --end '2020-02-01 00:00:00'
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel_updated CLONE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel;


SELECT *
FROM collab.scv_channelling_update_202011.module_touch_marketing_channel_updated mtmcu
WHERE mtmcu.touch_mkt_channel = 'Paid Social CPL'
  AND affiliate LIKE '%cpa%';

SELECT COUNT(*)
FROM collab.scv_channelling_update_202011.module_touch_marketing_channel_updated mtmcu
WHERE mtmcu.touch_mkt_channel = 'Other'
  AND mtmcu.utm_medium = 'newsletter';

------------------------------------------------------------------------------------------------------------------------
-- UTM_MEDIUM, UTM_SOURCE, UTM_TERM, CLICK_ID, AFFILIATE REFERRER_HOSTNAME, REFERRER_MEDIUM, SESSIONS

SELECT mtmcu.touch_mkt_channel,
       mtmcu.touch_landing_page,
       mtmcu.touch_hostname,
       mtmcu.utm_campaign,
       mtmcu.utm_medium,
       mtmcu.utm_source,
       mtmcu.utm_term,
       mtmcu.utm_content,
       mtmcu.click_id,
       mtmcu.affiliate,
       mtmcu.touch_affiliate_territory,
       mtmcu.referrer_hostname,
       mtmcu.referrer_medium,
       count(*) AS sessions
FROM collab.scv_channelling_update_202011.module_touch_marketing_channel_updated mtmcu;

------------------------------------------------------------------------------------------------------------------------
--test full scv
SELECT *
FROM data_vault_mvp.information_schema.tables t
WHERE t.table_schema = 'SINGLE_CUSTOMER_VIEW_STG';


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_extracted_params CLONE data_vault_mvp.single_customer_view_stg.module_extracted_params;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_identity_associations CLONE data_vault_mvp.single_customer_view_stg.module_identity_associations;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_identity_stitching CLONE data_vault_mvp.single_customer_view_stg.module_identity_stitching;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_time_diff_marker CLONE data_vault_mvp.single_customer_view_stg.module_time_diff_marker;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs CLONE data_vault_mvp.single_customer_view_stg.module_touched_spvs;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs_bkup CLONE data_vault_mvp.single_customer_view_stg.module_touched_spvs_bkup;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_transactions CLONE data_vault_mvp.single_customer_view_stg.module_touched_transactions;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchifiable_events CLONE data_vault_mvp.single_customer_view_stg.module_touchifiable_events;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification CLONE data_vault_mvp.single_customer_view_stg.module_touchification;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution CLONE data_vault_mvp.single_customer_view_stg.module_touch_attribution;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_utm_referrer CLONE data_vault_mvp.single_customer_view_stg.module_touch_utm_referrer;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_unique_urls CLONE data_vault_mvp.single_customer_view_stg.module_unique_urls;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_url_hostname CLONE data_vault_mvp.single_customer_view_stg.module_url_hostname;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_url_params CLONE data_vault_mvp.single_customer_view_stg.module_url_params;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_utm_referrer_marker CLONE data_vault_mvp.single_customer_view_stg.module_utm_referrer_marker;
CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream CLONE hygiene_vault_mvp.snowplow.event_stream;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_booking CLONE data_vault_mvp.dwh.se_booking;

SELECT updated_at,
       count(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel mtmc
WHERE mtmc.updated_at >= current_date - 5
GROUP BY 1;

SELECT *,
       CASE
           WHEN url_hostname LIKE 'webmail.%' OR
                url_hostname LIKE '%.email' OR
                url_hostname LIKE 'email.%' OR
                url_hostname LIKE '%.email.%'
               THEN 'email'

           WHEN url_hostname LIKE '%.secretescapes.%' OR
                url_hostname LIKE '%.evasionssecretes.%' OR
                url_hostname = 'escapes.travelbook.de' OR
                url_hostname = 'api.secretescapes.com' OR
                url_hostname LIKE '%.fs-staging.escapes.tech' OR
                url_hostname = 'www.optimizelyedit.com' OR
                url_hostname = 'cdn.secretescapes.com' OR
                url_hostname = 'secretescapes--c.eu12.visual.force.com' OR
                url_hostname = 'secretescapes.my.salesforce.com' OR
                url_hostname = 'cms.secretescapes.com' OR
                url_hostname = 'escapes.jetsetter.com' OR
                url_hostname LIKE '%travelbird.%' OR
                url_hostname LIKE '%travelist.pl' OR
                url_hostname = 'holidays.pigsback.com' OR
                url_hostname = 'www.travista.de' OR
                url_hostname = 'www.mycityvenueescapes.com' OR
                url_hostname = 'admin.co.uk.sales.secretescapes.com'
               --url_hostname = 'optimizely' -- TODO: expand on optimizely
               THEN 'internal' -- TODO: expand on internal definitions

           WHEN (url_hostname LIKE '%.facebook.%' AND url LIKE '%oauth%') --fb oauth logins
               THEN 'oauth'

           WHEN url_hostname = 'www.guardianescapes.com' OR
                url_hostname = 'www.gilttravel.com' OR
                url_hostname = 'www.hand-picked.telegraph.co.uk' OR
                url_hostname = 'escapes.radiotimes.com' OR
                url_hostname = 'escapes.timeout.com' OR
                url_hostname = 'www.independentescapes.com' OR
                url_hostname = 'www.confidentialescapes.co.uk' OR
                url_hostname = 'www.eveningstandardescapes.com' OR
                url_hostname = 'asap.shermanstravel.com' OR
                url_hostname = 'www.lateluxury.com' OR
                url_hostname = 'secretescapes.urlaubsguru.de'
               THEN 'whitelabel'

           WHEN url_hostname = 'www.paypal.com' OR
                url_hostname = 'secure.worldpay.com' OR
                url_hostname = 'secure.bidverdrd.com' OR
                url_hostname = '3d-secure.pluscard.de' OR
                url_hostname = 'mastercardsecurecode.sparkassen-kreditkarten.de' OR
                url_hostname = '3d-secure.postbank.de' OR
                url_hostname = 'german-3dsecure.wlp-acs.com' OR
                url_hostname = '3d-secure-code.de' OR
                url_hostname = 'search.f-secure.com'
               THEN 'payment_gateway'

           WHEN url_hostname LIKE '%.google.%' OR
                url_hostname LIKE '%.bing.%' OR
                url_hostname LIKE '%.duckduckgo.%' OR
                url_hostname LIKE '%.ecosia.%' OR
                url_hostname LIKE '%.aol.%' OR
                url_hostname LIKE '%.aolsearch.%'
               THEN 'search'

           WHEN url_hostname LIKE '%.pinterest.%' OR
                url_hostname LIKE '%.facebook.%' OR
                url_hostname = 'instagram.com'
               THEN 'social'

           WHEN
                   LOWER(url_hostname) REGEXP
                   '(.*(web|db-loadtesting|sandbox).*\\\\.secretescapes.com|.*.fs-staging.escapes.tech|[0-9]{{{{1,3}}}}\\\\.[0-9]{{{{1,3}}}}\\\\.[0-9]{{{{1,3}}}}\\\\.[0-9]{{{{1,3}}}})'
               THEN 'SE TECH'

           ELSE 'unknown'
           END AS url_medium2
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_url_hostname muh
WHERE muh.url_medium != url_medium2;

USE WAREHOUSE pipe_xlarge;
DELETE
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_extracted_params
WHERE updated_at::DATE = '2020-12-07';
DELETE
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_identity_associations
WHERE updated_at::DATE = '2020-12-07';
DELETE
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_identity_stitching
WHERE updated_at::DATE = '2020-12-07';
DELETE
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_time_diff_marker
WHERE updated_at::DATE = '2020-12-07';
DELETE
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs
WHERE updated_at::DATE = '2020-12-07';
DELETE
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_transactions
WHERE updated_at::DATE = '2020-12-07';
DELETE
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchifiable_events
WHERE updated_at::DATE = '2020-12-07';
DELETE
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
WHERE updated_at::DATE = '2020-12-07';
DELETE
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution
WHERE updated_at::DATE = '2020-12-07';
DELETE
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
WHERE updated_at::DATE = '2020-12-07';
DELETE
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel
WHERE updated_at::DATE = '2020-12-07';
DELETE
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_utm_referrer
WHERE updated_at::DATE = '2020-12-07';
DELETE
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_unique_urls
WHERE updated_at::DATE = '2020-12-07';
DELETE
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_url_hostname
WHERE updated_at::DATE = '2020-12-07';
DELETE
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_url_params
WHERE updated_at::DATE = '2020-12-07';
DELETE
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_utm_referrer_marker
WHERE updated_at::DATE = '2020-12-07';

--to update referrer medium
UPDATE data_vault_mvp_dev_robin.single_customer_view_stg.module_url_hostname target
SET target.url_medium = CASE
                            WHEN url_hostname LIKE 'webmail.%' OR
                                 url_hostname LIKE '%.email' OR
                                 url_hostname LIKE 'email.%' OR
                                 url_hostname LIKE '%.email.%'
                                THEN 'email'

                            WHEN url_hostname LIKE '%.secretescapes.%' OR
                                 url_hostname LIKE '%.evasionssecretes.%' OR
                                 url_hostname = 'escapes.travelbook.de' OR
                                 url_hostname = 'api.secretescapes.com' OR
                                 url_hostname LIKE '%.fs-staging.escapes.tech' OR
                                 url_hostname = 'www.optimizelyedit.com' OR
                                 url_hostname = 'cdn.secretescapes.com' OR
                                 url_hostname = 'secretescapes--c.eu12.visual.force.com' OR
                                 url_hostname = 'secretescapes.my.salesforce.com' OR
                                 url_hostname = 'cms.secretescapes.com' OR
                                 url_hostname = 'escapes.jetsetter.com' OR
                                 url_hostname LIKE '%travelbird.%' OR
                                 url_hostname LIKE '%travelist.pl' OR
                                 url_hostname = 'holidays.pigsback.com' OR
                                 url_hostname = 'www.travista.de' OR
                                 url_hostname = 'www.mycityvenueescapes.com' OR
                                 url_hostname = 'admin.co.uk.sales.secretescapes.com'
                                --url_hostname = 'optimizely' -- TODO: expand on optimizely
                                THEN 'internal' -- TODO: expand on internal definitions

                            WHEN (url_hostname LIKE '%.facebook.%' AND url LIKE '%oauth%') --fb oauth logins
                                THEN 'oauth'

                            WHEN url_hostname = 'www.guardianescapes.com' OR
                                 url_hostname = 'www.gilttravel.com' OR
                                 url_hostname = 'www.hand-picked.telegraph.co.uk' OR
                                 url_hostname = 'escapes.radiotimes.com' OR
                                 url_hostname = 'escapes.timeout.com' OR
                                 url_hostname = 'www.independentescapes.com' OR
                                 url_hostname = 'www.confidentialescapes.co.uk' OR
                                 url_hostname = 'www.eveningstandardescapes.com' OR
                                 url_hostname = 'asap.shermanstravel.com' OR
                                 url_hostname = 'www.lateluxury.com' OR
                                 url_hostname = 'secretescapes.urlaubsguru.de'
                                THEN 'whitelabel'

                            WHEN url_hostname = 'www.paypal.com' OR
                                 url_hostname = 'secure.worldpay.com' OR
                                 url_hostname = 'secure.bidverdrd.com' OR
                                 url_hostname = '3d-secure.pluscard.de' OR
                                 url_hostname = 'mastercardsecurecode.sparkassen-kreditkarten.de' OR
                                 url_hostname = '3d-secure.postbank.de' OR
                                 url_hostname = 'german-3dsecure.wlp-acs.com' OR
                                 url_hostname = '3d-secure-code.de' OR
                                 url_hostname = 'search.f-secure.com'
                                THEN 'payment_gateway'

                            WHEN url_hostname LIKE '%.google.%' OR
                                 url_hostname LIKE '%.bing.%' OR
                                 url_hostname LIKE '%.duckduckgo.%' OR
                                 url_hostname LIKE '%.ecosia.%' OR
                                 url_hostname LIKE '%.aol.%' OR
                                 url_hostname LIKE '%.aolsearch.%'
                                THEN 'search'

                            WHEN url_hostname LIKE '%.pinterest.%' OR
                                 url_hostname LIKE '%.facebook.%' OR
                                 url_hostname = 'instagram.com'
                                THEN 'social'

                            WHEN
                                    LOWER(url_hostname) REGEXP
                                    '(.*(web|db-loadtesting|sandbox).*\\\\.secretescapes.com|.*.fs-staging.escapes.tech|[0-9]{{{{1,3}}}}\\\\.[0-9]{{{{1,3}}}}\\\\.[0-9]{{{{1,3}}}}\\\\.[0-9]{{{{1,3}}}})'
                                THEN 'SE TECH'

                            ELSE 'unknown'
    END;

airflow clear --start_date '2020-12-06 03:00:00' --end_date '2020-12-06 03:00:00' --task_regex '.*' single_customer_view__daily_at_03h00
airflow backfill --start_date '2020-12-06 03:00:00' --end_date '2020-12-06 03:00:00' --task_regex '.*' single_customer_view__daily_at_03h00


SELECT count(*)
FROM se.data.fact_booking
WHERE shiro_user_id = < user_id >
  AND booking_status_type IN ('live', 'cancelled')
  AND booking_completed_date >= dateadd('year', -7, current_date());

self_describing_task --include 'se/data_pii/scv/scv_event_stream.py'  --method 'run' --start '2020-12-06 00:00:00' --end '2020-12-06 00:00:00'

SELECT mt.event_hash,
       mt.attributed_user_id,
       mt.stitched_identity_type,
       mt.event_tstamp,
       mt.touch_id,
       mt.event_index_within_touch
FROM data_vault_mvp.single_customer_view_stg.module_touchification mt
         self_describing_task
--include 'se/data_pii/scv/scv_session_events_link.py'  --method 'run' --start '2020-12-06 00:00:00' --end '2020-12-06 00:00:00'

--production
SELECT mtmc.touch_mkt_channel, count(*)
FROM data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel mtmc
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba ON mtmc.touch_id = mtba.touch_id
WHERE mtba.touch_start_tstamp::DATE >= '2020-12-06'
GROUP BY 1;

--development
SELECT mtmc.touch_mkt_channel, count(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel mtmc
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba ON mtmc.touch_id = mtba.touch_id
WHERE mtba.touch_start_tstamp::DATE >= '2020-12-06'
GROUP BY 1;

------------------------------------------------------------------------------------------------------------------------
--back up of single day run
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel_20201207 CLONE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel;

USE WAREHOUSE pipe_xlarge;
--update referrer medium

UPDATE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel t
SET t.referrer_medium = CASE
                            WHEN t.referrer_hostname LIKE 'webmail.%' OR
                                 t.referrer_hostname LIKE '%.email' OR
                                 t.referrer_hostname LIKE 'email.%' OR
                                 t.referrer_hostname LIKE '%.email.%'
                                THEN 'email'

                            WHEN t.referrer_hostname LIKE '%.secretescapes.%' OR
                                 t.referrer_hostname LIKE '%.evasionssecretes.%' OR
                                 t.referrer_hostname = 'escapes.travelbook.de' OR
                                 t.referrer_hostname = 'api.secretescapes.com' OR
                                 t.referrer_hostname LIKE '%.fs-staging.escapes.tech' OR
                                 t.referrer_hostname = 'www.optimizelyedit.com' OR
                                 t.referrer_hostname = 'cdn.secretescapes.com' OR
                                 t.referrer_hostname = 'secretescapes--c.eu12.visual.force.com' OR
                                 t.referrer_hostname = 'secretescapes.my.salesforce.com' OR
                                 t.referrer_hostname = 'cms.secretescapes.com' OR
                                 t.referrer_hostname = 'escapes.jetsetter.com' OR
                                 t.referrer_hostname LIKE '%travelbird.%' OR
                                 t.referrer_hostname LIKE '%travelist.pl' OR
                                 t.referrer_hostname = 'holidays.pigsback.com' OR
                                 t.referrer_hostname = 'www.travista.de' OR
                                 t.referrer_hostname = 'www.mycityvenueescapes.com' OR
                                 t.referrer_hostname = 'admin.co.uk.sales.secretescapes.com'
                                --t.referrer_hostname = 'optimizely' -- TODO: expand on optimizely
                                THEN 'internal' -- TODO: expand on internal definitions

                            WHEN (t.referrer_hostname LIKE '%.facebook.%' AND
                                  mtba.touch_referrer_url LIKE '%oauth%') --fb oauth logins
                                THEN 'oauth'

                            WHEN t.referrer_hostname = 'www.guardianescapes.com' OR
                                 t.referrer_hostname = 'www.gilttravel.com' OR
                                 t.referrer_hostname = 'www.hand-picked.telegraph.co.uk' OR
                                 t.referrer_hostname = 'escapes.radiotimes.com' OR
                                 t.referrer_hostname = 'escapes.timeout.com' OR
                                 t.referrer_hostname = 'www.independentescapes.com' OR
                                 t.referrer_hostname = 'www.confidentialescapes.co.uk' OR
                                 t.referrer_hostname = 'www.eveningstandardescapes.com' OR
                                 t.referrer_hostname = 'asap.shermanstravel.com' OR
                                 t.referrer_hostname = 'www.lateluxury.com' OR
                                 t.referrer_hostname = 'secretescapes.urlaubsguru.de'
                                THEN 'whitelabel'

                            WHEN t.referrer_hostname = 'www.paypal.com' OR
                                 t.referrer_hostname = 'secure.worldpay.com' OR
                                 t.referrer_hostname = 'secure.bidverdrd.com' OR
                                 t.referrer_hostname = '3d-secure.pluscard.de' OR
                                 t.referrer_hostname = 'mastercardsecurecode.sparkassen-kreditkarten.de' OR
                                 t.referrer_hostname = '3d-secure.postbank.de' OR
                                 t.referrer_hostname = 'german-3dsecure.wlp-acs.com' OR
                                 t.referrer_hostname = '3d-secure-code.de' OR
                                 t.referrer_hostname = 'search.f-secure.com'
                                THEN 'payment_gateway'

                            WHEN t.referrer_hostname LIKE '%.google.%' OR
                                 t.referrer_hostname LIKE '%.bing.%' OR
                                 t.referrer_hostname LIKE '%.duckduckgo.%' OR
                                 t.referrer_hostname LIKE '%.ecosia.%' OR
                                 t.referrer_hostname LIKE '%.aol.%' OR
                                 t.referrer_hostname LIKE '%.aolsearch.%'
                                THEN 'search'

                            WHEN t.referrer_hostname LIKE '%.pinterest.%' OR
                                 t.referrer_hostname LIKE '%.facebook.%' OR
                                 t.referrer_hostname = 'instagram.com'
                                THEN 'social'

                            WHEN
                                    LOWER(t.referrer_hostname) REGEXP
                                    '(.*(web|db-loadtesting|sandbox).*\\\\.secretescapes.com|.*.fs-staging.escapes.tech|[0-9]{{{{1,3}}}}\\\\.[0-9]{{{{1,3}}}}\\\\.[0-9]{{{{1,3}}}}\\\\.[0-9]{{{{1,3}}}})'
                                THEN 'SE TECH'
                            WHEN t.referrer_hostname IS NULL THEN NULL --handle missing referrers
                            ELSE 'unknown'
    END
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel batch
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba ON batch.touch_id = mtba.touch_id
WHERE t.touch_id = batch.touch_id;

USE WAREHOUSE pipe_xlarge;

UPDATE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel t
SET t.touch_mkt_channel =
        CASE
            --flag testing/staging sessions
            WHEN
                t.referrer_medium = 'SE TECH' THEN 'Test'

            --no utm or referrer data
            WHEN
                    (
                            t.utm_campaign IS NULL AND
                            t.utm_content IS NULL AND
                            t.utm_term IS NULL AND
                            t.utm_medium IS NULL AND
                            t.utm_source IS NULL AND
                            t.click_id IS NULL AND
                            (
                                    t.referrer_medium IS NULL
                                    OR
                                    t.referrer_medium IN ('internal', 'oauth')
                                    OR
                                    referrer_hostname = 'm.facebook.com' -- to handle facebook oauth logins for direct traffic
                                )
                        )
                    OR
                    ( --handle switch between internal hostnames
                            t.referrer_medium IN ('internal', 'oauth')
                            AND
                            t.utm_medium IS DISTINCT FROM 'email' --to remove autocomms coming through as internal
                        )
                THEN 'Direct'

            --when the utm or gclid params aren't all null

            WHEN t.utm_medium = 'email'
                THEN
                CASE
                    WHEN t.utm_source = 'newsletter' THEN 'Email - Newsletter'
                    WHEN t.utm_source = 'ame' THEN 'Email - Triggers'
                    --might have more
                    ELSE 'Email - Other'
                    END

            WHEN t.utm_source = 'criteo' OR
                 LOWER(t.affiliate) LIKE '%cpa-gdn%'
                THEN 'Display CPA'

            WHEN t.utm_medium IN ('display', 'tpemail', 'native', 'gdn')
                THEN 'Display CPL'

            WHEN utm_medium = 'affiliateprogramme'
                THEN 'Affiliate Program'

            WHEN utm_medium = 'SE_media' THEN 'Media'

            WHEN utm_medium = 'blog' THEN 'Blog'

            WHEN utm_source = 'youtube' THEN 'YouTube' --not in place yet but will be

            WHEN t.affiliate LIKE '%cpa%'
                AND (
                         t.utm_medium = 'facebookads'
                         OR t.affiliate LIKE 'fb%'
                     )
                THEN 'Paid Social CPA' -- placed above PPC because fb paid social has click ids

            WHEN t.utm_medium = 'facebookads'
                OR
                 (
                         (t.affiliate LIKE 'facebook%' OR t.affiliate LIKE 'fb%')
                         AND (t.utm_medium = 'facebookads' OR t.click_id IS NOT NULL)
                     )
                THEN 'Paid Social CPL' -- placed above PPC because fb paid social has click ids

            WHEN t.click_id IS NOT NULL -- how to capture all ppc
                THEN
                --case logic provided by Rumi in Marketing:
                --https://docs.google.com/spreadsheets/d/19RBArUlM5pn2YFcMGTZVDwmfk818WYxoBLKjjm_p1zU/edit#gid=1957868123

                CASE
                    WHEN
                            (
                                    LOWER(t.affiliate) LIKE '%bra%'
                                    OR
                                    LOWER(t.affiliate) REGEXP
                                    '.*(gooaus|goobelgian|goo-dane|goodeutsch|secret-id|goodutch|goo-norway|gooswede|gooswi|goosups|goousa|goousa-ec|goousa-fl).*'
                                )
                            AND LOWER(t.affiliate) NOT LIKE '%yahnobra%'
                        THEN 'PPC - Brand'

                    WHEN
                            (
                                    LOWER(t.affiliate) LIKE '%cpa%'
                                    OR
                                    LOWER(t.affiliate) REGEXP '.*(dsa - france|active - eagle|hpa - uk).*'
                                )
                            AND LOWER(t.affiliate) NOT LIKE '%brand%'
                        THEN 'PPC - Non Brand CPA'

                    WHEN
                            (
                                    LOWER(t.affiliate) LIKE '%cpl%'
                                    OR
                                    LOWER(t.affiliate) LIKE '%secret%'
                                    OR
                                    LOWER(t.affiliate) REGEXP
                                    '.*(at-dsa|de-dsa|ppc-de2-test-variant-a-de-printfox|ppc-de2-test-variant-b-de-maponos|dsa-italy|nl-dsa|ch-dsa|ppc-uk3-test-variant-a-uk-printfox|ppc-uk3-test-variant-b-uk-maponos|usa-dsa-ec|usa-dsa|stalion-italy|yahooppcdsa|yahooppc|yahnobra-german|yahoo2ppc|yahnobra2-german|yahoo3ppc|yahnobra-dutch|yahnobra-sweden|yahnobra-denmark|yahnobra-usa|yahnobra-norway).*'
                                )
                            --AND LOWER(t.affiliate) NOT LIKE '%cpa%'
                            AND LOWER(t.affiliate) != 'secret-bra-id'
                            AND LOWER(t.affiliate) != 'secret-id'
                        THEN 'PPC - Non Brand CPL'

                    ELSE 'PPC - Undefined'
                    END

            WHEN t.utm_medium = 'organic-social'
                OR (t.utm_medium = 'social' AND t.utm_source LIKE 'whatsapp%') --whatsapp shares
                OR (t.utm_medium = 'social' AND t.utm_source LIKE 'fbshare%') --facebook shares
                OR (t.utm_medium = 'social' AND t.utm_source LIKE 'tweet%') --twitter shares
                OR (t.affiliate LIKE 'instagram%' AND t.referrer_hostname = 'linkinprofile.com')
                THEN 'Organic Social'

            WHEN t.referrer_medium = 'search' AND
                 (
                             PARSE_URL(t.touch_landing_page, 1)['path']::VARCHAR IN ('', 'current-sales')
                         OR
                             PARSE_URL(t.touch_landing_page, 1)['path']::VARCHAR IS NULL
                     ) THEN 'Organic Search Brand'

            WHEN t.referrer_medium = 'search' THEN 'Organic Search Non-Brand'

            -- no utm or glcid params (but there are referrer details)
            WHEN
                    t.utm_campaign IS NULL AND
                    t.utm_content IS NULL AND
                    t.utm_term IS NULL AND
                    t.utm_medium IS NULL AND
                    t.utm_source IS NULL AND
                    t.click_id IS NULL
                THEN
                CASE
                    WHEN (
                            (t.referrer_medium IN ('internal', 'oauth'))
                            OR
                            (
                                    t.referrer_medium = 'unknown' AND
                                    (
                                            t.referrer_hostname LIKE '%secretescapes.%' OR
                                            t.referrer_hostname LIKE 'evasionssecretes.%' OR
                                            t.referrer_hostname LIKE 'travelbird.%' OR
                                            t.referrer_hostname LIKE '%.travelist.%' OR
                                            t.referrer_hostname LIKE '%.pigsback.%'
                                        )
                                )
                        ) THEN 'Direct'

                    WHEN t.referrer_medium = 'unknown' AND
                         (
                                 (t.referrer_hostname LIKE '%urlaub%' OR
                                  t.referrer_hostname LIKE '%butterholz%' OR
                                  t.referrer_hostname LIKE '%mydealz%' OR
                                  t.referrer_hostname LIKE '%travel-dealz%' OR
                                  t.referrer_hostname LIKE '%travel-dealz%' OR
                                  t.referrer_hostname LIKE '%discountvouchers%'
                                     )
                                 OR
                                 se.data.partner_affiliate_param(LOWER(t.affiliate)) -- udf to test if affiliate is within partner list
                             ) THEN 'Partner'
                    ELSE 'Other'
                    END
            ELSE 'Other'
            END
;

SELECT DISTINCT referrer_medium
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel mtmc;
--development
SELECT mtmc.touch_mkt_channel,
       count(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel mtmc
GROUP BY 1;

--production
SELECT mtmc.touch_mkt_channel,
       count(*)
FROM data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel mtmc
GROUP BY 1;

SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel mtmc
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel m
                    ON mtmc.touch_id = m.touch_id AND m.touch_mkt_channel = 'Direct'
WHERE mtmc.touch_mkt_channel = 'Other';

USE WAREHOUSE pipe_medium;

SELECT mtmc.touch_mkt_channel, count(*)
FROM data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel mtmc
GROUP BY 1;


SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel;

