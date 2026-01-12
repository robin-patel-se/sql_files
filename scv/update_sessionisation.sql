SELECT *
FROM se.data.scv_touched_transactions stt
WHERE stt.booking_id = 'A3236069' -- 4b97c36e248a7fa6b0cde5c4d598d5f63ec8ebf38ddd3f8d6a3348edfdc8d60c

SELECT *
FROM se.data.scv_touch_basic_attributes stba
WHERE stba.touch_id = '4b97c36e248a7fa6b0cde5c4d598d5f63ec8ebf38ddd3f8d6a3348edfdc8d60c';


SELECT PARSE_URL(stmc.touch_landing_page, 1): PATH::VARCHAR AS landing_page_path, --used to channel organic search
       CASE
           --flag testing/staging sessions
           WHEN
               stmc.referrer_medium = 'SE TECH' THEN 'Test'

           --no utm or referrer data
           WHEN
                   (
                           stmc.utm_campaign IS NULL AND
                           stmc.utm_content IS NULL AND
                           stmc.utm_term IS NULL AND
                           stmc.utm_medium IS NULL AND
                           stmc.utm_source IS NULL AND
                           stmc.click_id IS NULL AND
                           (
                                   stmc.referrer_medium IS NULL
                                   OR
                                   stmc.referrer_medium IN ('internal', 'oauth')
                                   OR
                                   referrer_hostname = 'm.facebook.com' -- to handle facebook oauth logins for direct traffic
                               )
                       )
                   OR
                   ( --handle switch between internal hostnames
                           stmc.referrer_medium IN ('internal', 'oauth')
                           AND
                           stmc.utm_medium IS DISTINCT FROM 'email' --to remove autocomms coming through as internal
                       )
               THEN 'Direct'

           --when the utm or gclid params aren't all null

           WHEN stmc.utm_medium = 'email'
               THEN
               CASE
                   WHEN stmc.utm_source = 'newsletter' THEN 'Email - Newsletter'
                   WHEN stmc.utm_source = 'ame' THEN 'Email - Triggers'
                   --might have more
                   ELSE 'Email - Other'
                   END

           WHEN stmc.utm_source = 'criteo' OR
                LOWER(stmc.affiliate) LIKE '%cpa-gdn%'
               THEN 'Display CPA'

           WHEN stmc.utm_medium IN ('display', 'tpemail', 'native', 'gdn')
               THEN 'Display CPL'

           WHEN utm_medium = 'affiliateprogramme'
               THEN 'Affiliate Program'

           WHEN utm_medium = 'SE_media' THEN 'Media'

           WHEN utm_medium = 'blog' THEN 'Blog'

           WHEN utm_source = 'youtube' THEN 'YouTube' --not in place yet but will be

           WHEN stmc.affiliate LIKE '%cpa%'
               AND (
                        stmc.utm_medium = 'facebookads'
                        OR stmc.affiliate LIKE 'fb%'
                    )
               THEN 'Paid Social CPA' -- placed above PPC because fb paid social has click ids

           WHEN stmc.utm_medium = 'facebookads'
               OR
                (
                        (stmc.affiliate LIKE 'facebook%' OR stmc.affiliate LIKE 'fb%')
                        AND (stmc.utm_medium = 'facebookads' OR stmc.click_id IS NOT NULL)
                    )
               THEN 'Paid Social CPL' -- placed above PPC because fb paid social has click ids

           WHEN stmc.click_id IS NOT NULL -- how to capture all ppc
               THEN
               --case logic provided by Rumi in Marketing:
               --https://docs.google.com/spreadsheets/d/19RBArUlM5pn2YFcMGTZVDwmfk818WYxoBLKjjm_p1zU/edit#gid=1957868123

               CASE
                   WHEN
                           (
                                   LOWER(stmc.affiliate) LIKE '%bra%'
                                   OR
                                   LOWER(stmc.affiliate) REGEXP '.*({brand_categories}).*'
                               )
                           AND LOWER(stmc.affiliate) NOT LIKE '%yahnobra%'
                       THEN 'PPC - Brand'

                   WHEN
                           (
                                   LOWER(stmc.affiliate) LIKE '%cpa%'
                                   OR
                                   LOWER(stmc.affiliate) REGEXP '.*({cpa_categories}).*'
                               )
                           AND LOWER(stmc.affiliate) NOT LIKE '%brand%'
                       THEN 'PPC - Non Brand CPA'

                   WHEN
                           (
                                   LOWER(stmc.affiliate) LIKE '%cpl%'
                                   OR
                                   LOWER(stmc.affiliate) LIKE '%secret%'
                                   OR
                                   LOWER(stmc.affiliate) REGEXP '.*({cpl_categories}).*'
                               )
                           --AND LOWER(stmc.affiliate) NOT LIKE '%cpa%'
                           AND LOWER(stmc.affiliate) != 'secret-bra-id'
                           AND LOWER(stmc.affiliate) != 'secret-id'
                       THEN 'PPC - Non Brand CPL'

                   ELSE 'PPC - Undefined'
                   END

           WHEN stmc.utm_medium = 'organic-social'
               OR (stmc.utm_medium = 'social' AND stmc.utm_source LIKE 'whatsapp%') --whatsapp shares
               OR (stmc.utm_medium = 'social' AND stmc.utm_source LIKE 'fbshare%') --facebook shares
               OR (stmc.utm_medium = 'social' AND stmc.utm_source LIKE 'tweet%') --twitter shares
               OR (stmc.affiliate LIKE 'instagram%' AND stmc.referrer_hostname = 'linkinprofile.com')
               THEN 'Organic Social'

           WHEN stmc.referrer_medium = 'search' AND
                (
                        landing_page_path IN ('', 'current-sales')
                        OR
                        landing_page_path IS NULL
                    ) THEN 'Organic Search Brand'

           WHEN stmc.referrer_medium = 'search' THEN 'Organic Search Non-Brand'

           -- no utm or click params (but there are referrer details)
           WHEN
                   stmc.utm_campaign IS NULL AND
                   stmc.utm_content IS NULL AND
                   stmc.utm_term IS NULL AND
                   stmc.utm_medium IS NULL AND
                   stmc.utm_source IS NULL AND
                   stmc.click_id IS NULL
               THEN
               CASE
                   WHEN (
                           (stmc.referrer_medium IN ('internal', 'oauth'))
                           OR
                           (
                                   stmc.referrer_medium = 'unknown' AND
                                   (
                                           stmc.referrer_hostname LIKE '%secretescapes.%' OR
                                           stmc.referrer_hostname LIKE 'evasionssecretes.%' OR
                                           stmc.referrer_hostname LIKE 'travelbird.%' OR
                                           stmc.referrer_hostname LIKE '%.travelist.%' OR
                                           stmc.referrer_hostname LIKE '%.pigsback.%'
                                       )
                               )
                       ) THEN 'Direct'

                   WHEN
                           (
                                   stmc.referrer_medium = 'unknown'
                                   AND
                                   (
                                           (
                                                   stmc.referrer_hostname LIKE '%urlaub%' OR
                                                   stmc.referrer_hostname LIKE '%butterholz%' OR
                                                   stmc.referrer_hostname LIKE '%mydealz%' OR
                                                   stmc.referrer_hostname LIKE '%travel-dealz%' OR
                                                   stmc.referrer_hostname LIKE '%travel-dealz%' OR
                                                   stmc.referrer_hostname LIKE '%discountvouchers%'
                                               )
                                           OR
                                           se.data.partner_affiliate_param(LOWER(stmc.affiliate)) -- udf to test if affiliate is within partner list
                                       )
                               )
                           OR
                           se.data.partner_url(stmc.touch_hostname) --udf to test if the hostname is a partner hostname
                       THEN 'Partner'
                   ELSE 'Other'
                   END
           ELSE 'Other'
           END                                              AS touch_mkt_channel,
       *
FROM se.data.scv_touch_marketing_channel stmc
WHERE stmc.touch_mkt_channel = 'Direct'
  AND affiliate LIKE 'impact%';

USE WAREHOUSE pipe_2xlarge;

SELECT *
FROM se.data_pii.scv_session_events_link ssel
         INNER JOIN se.data_pii.scv_event_stream ses ON ssel.event_hash = ses.event_hash
WHERE ssel.touch_id = '6452aa55e510a3c29366b782de21867498b1413425ad28b6f0c4078a20ac6bca'


CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream CLONE hygiene_vault_mvp.snowplow.event_stream;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification CLONE data_vault_mvp.single_customer_view_stg.module_touchification;
SELECT MIN(updated_at)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification mt; -- 2020-02-28 17:06:45.849000000

TRUNCATE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes;


self_describing_task --include 'dv/dwh/events/04_touch_basic_attributes/01_module_touch_basic_attributes.py'  --method 'run' --start '2020-02-28 00:00:00' --end '2020-02-28 00:00:00'
self_describing_task --include 'dv/dwh/events/04_touch_basic_attributes/01_module_touch_basic_attributes.py'  --method 'run' --start '2021-04-15 00:00:00' --end '2021-04-15 00:00:00'

SELECT COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba;
SELECT COUNT(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes mtba;


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_extracted_params CLONE data_vault_mvp.single_customer_view_stg.module_extracted_params;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_url_hostname CLONE data_vault_mvp.single_customer_view_stg.module_url_hostname;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification CLONE data_vault_mvp.single_customer_view_stg.module_touchification;

DROP TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_utm_referrer;
self_describing_task --include 'dv/dwh/events/05_touch_channelling/01_module_touch_utm_referrer.py'  --method 'run' --start '2021-04-15 00:00:00' --end '2021-04-15 00:00:00'

--'6452aa55e510a3c29366b782de21867498b1413425ad28b6f0c4078a20ac6bca' touch id of affiliate incorrectly being categorised
SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes mtba
WHERE mtba.touch_id = '6452aa55e510a3c29366b782de21867498b1413425ad28b6f0c4078a20ac6bca';
SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_utm_referrer mtba
WHERE mtba.touch_id = '6452aa55e510a3c29366b782de21867498b1413425ad28b6f0c4078a20ac6bca';

DROP TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel;
CREATE OR REPLACE VIEW hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.affiliate CLONE hygiene_snapshot_vault_mvp.cms_mysql.affiliate;

self_describing_task --include 'dv/dwh/events/05_touch_channelling/02_module_touch_marketing_channel.py'  --method 'run' --start '2021-04-15 00:00:00' --end '2021-04-15 00:00:00'



SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel mtmc
WHERE mtmc.touch_id = '6452aa55e510a3c29366b782de21867498b1413425ad28b6f0c4078a20ac6bca'

--dev
SELECT mtmc.touch_mkt_channel,
       COUNT(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel mtmc
         INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes mtba
                    ON mtmc.touch_id = mtba.touch_id
WHERE mtba.touch_start_tstamp >= CURRENT_DATE - 30
GROUP BY 1;

--prod
SELECT mtmc.touch_mkt_channel,
       COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel mtmc
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba ON mtmc.touch_id = mtba.touch_id
WHERE mtba.touch_start_tstamp >= CURRENT_DATE - 30
GROUP BY 1;


--dev
SELECT COUNT(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel mtmc;

--prod
SELECT COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel mtmc;

USE WAREHOUSE pipe_xlarge;


SELECT mtmc.touch_mkt_channel,
       stt.booking_id
FROM se.data.scv_touched_transactions stt
         INNER JOIN se.data.scv_touch_marketing_channel mtmc ON stt.touch_id = mtmc.touch_id
WHERE stt.booking_id IN (
                         'A3236069',
                         'A3229658',
                         'A3229627',
                         'A3128885',
                         'A3165988',
                         'A3165999',
                         'A3120072',
                         'A3125331',
                         'A3152933'
    )


SELECT *
FROM se.data.scv_touched_transactions stt
WHERE stt.booking_id = 'A3236069';

SELECT *
FROM se.data_pii.scv_touch_basic_attributes stba
WHERE stba.touch_id = '4b97c36e248a7fa6b0cde5c4d598d5f63ec8ebf38ddd3f8d6a3348edfdc8d60c';

SELECT *
FROM se.data_pii.scv_session_events_link ssel
WHERE ssel.attributed_user_id = '53766548';

SELECT ssel.*,
       ses.event_name,
       ses.device_platform
FROM se.data_pii.scv_session_events_link ssel
         INNER JOIN se.data_pii.scv_event_stream ses ON ssel.event_hash = ses.event_hash
WHERE ssel.attributed_user_id = '53766548';

--ed0af0819f235da49283465b524d798beb218f90f42572a507b4acbd948014a0 touch id that is between 4b97c36e248a7fa6b0cde5c4d598d5f63ec8ebf38ddd3f8d6a3348edfdc8d60c

SELECT *
FROM se.data_pii.scv_touch_basic_attributes stba
WHERE stba.touch_id = 'ed0af0819f235da49283465b524d798beb218f90f42572a507b4acbd948014a0';

SELECT *
FROM se.data.scv_touch_marketing_channel stmc
WHERE stmc.touch_id = '4b97c36e248a7fa6b0cde5c4d598d5f63ec8ebf38ddd3f8d6a3348edfdc8d60c';