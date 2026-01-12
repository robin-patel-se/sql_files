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


self_describing_task --include 'dv/dwh/events/07_events_of_interest/01_module_touched_spvs.py'  --method 'run' --start '2021-02-02 00:00:00' --end '2021-02-02 00:00:00'


                --SPVs from page views
SELECT e.event_hash,
       t.touch_id,
       e.event_tstamp,
       CASE
           WHEN
               -- Travelist sales have conflicting sale ids so we prefix the sale id
                   PARSE_URL(e.page_url, 1)['host']::VARCHAR LIKE '%travelist%' AND v_tracker LIKE 'py-%'
               THEN 'TVL' || e.se_sale_id
           ELSE e.se_sale_id END AS se_sale_id,
       'page views'              AS event_category,
       'SPV'                     AS event_subcategory,
       e.page_url
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification t
         INNER JOIN hygiene_vault_mvp_dev_robin.snowplow.event_stream e ON e.event_hash = t.event_hash
WHERE e.event_name = 'page_view'
  AND e.se_sale_id IS NOT NULL
  AND t.updated_at >= TIMESTAMPADD('day', -1, '2021-02-01 03:00:00'::TIMESTAMP)
  AND e.device_platform NOT IN ('native app ios', 'native app android')                     --explicitly remove native app (as app offer pages appear like web SPVs)
  AND PARSE_URL(e.page_url, 1)['host']::VARCHAR NOT LIKE '%.eu-west-1.compute.amazonaws.com'--remove se scanning tool
  AND (--line in sand between client side and server side tracking
        (--client side tracking, prior implementation/validation
                e.collector_tstamp < '2020-02-28 00:00:00'
                AND (
                        e.page_urlpath LIKE '%/sale'
                        OR
                        e.page_urlpath LIKE '%/sale-%' --eg. /sale-ihp, /sale-hotel for connected sales
                    )
                AND e.is_server_side_event = FALSE -- exclude non validated ss events
            )
        OR
        (--server side tracking, post implementation/validation
                e.collector_tstamp >= '2020-02-28 00:00:00'
                AND e.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR = 'sale'
                AND e.is_server_side_event = TRUE
            )
    );

USE WAREHOUSE pipe_xlarge;


DELETE
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs mts
    USING data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes mtba
WHERE mts.touch_id = mtba.touch_id
  AND mtba.touch_hostname_territory = 'SE TECH'
;

DELETE
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs mts
    USING data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes mtba
WHERE PARSE_URL(mts.page_url):host::VARCHAR LIKE '%.amazonaws.com'
;

-- PARSE_URL(mts.page_url):host::VARCHAR LIKE '%.eu-west-1.compute.amazonaws.com'


SELECT PARSE_URL(mts.page_url):host::VARCHAR
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs mts;


SELECT PARSE_URL(sts.page_url):host::varchar AS host,
       COUNT(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs sts
         INNER JOIN se.data.scv_touch_marketing_channel stmc ON sts.touch_id = stmc.touch_id
WHERE sts.event_tstamp::DATE >= '2020-10-01'
  AND stmc.touch_mkt_channel = 'Other'
GROUP BY 1;


--update existing affiliate for newly identified se tech

UPDATE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
SET touch_hostname_territory = 'SE TECH',
    touch_posa_territory     = 'SE TECH'
WHERE touch_hostname LIKE '%.amazonaws.com';


UPDATE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel
SET touch_hostname_territory  = 'SE TECH',
    touch_affiliate_territory = 'SE TECH'
WHERE touch_hostname LIKE '%.amazonaws.com';


SELECT '60.123.231.323' AS string_hostname,
       CASE
           WHEN
                   LOWER(string_hostname) REGEXP
                   '(blue.secretescapes.com|(www.)*confidentialescapes.co.uk|escapes.instyle.co.uk|escapes.planetradiooffers.co.uk|escapes.radiotimes.com|escapes.timeout.com|escapes.vikingfm-offers.co.uk|escapes.wave105deals.co.uk|(www.)*eveningstandardescapes.com|(www.)*guardianescapes.com|(www.)*hand-picked.telegraph.co.uk|hellomagazine.com/travel|holidays.pigsback.com|icelollyescapes.com|independent.co.uk/travelholidays|independent.secretescapes.com|(www.)*independentescapes.com|(www.)*lateluxury.com|mailescapes.co.uk|mycityvenueescapes.com|planetconfidential.co.uk|planetradiooffers.co.uk|secretsales.secretescapes.com|standard.co.uk/lifestyletravel|talktalkescapes.com|teletext.secretescapes.com|travel.radiotimes.com|trips.5pm.co.uk|(www.)*secretescapes.com|fathomaway.com|gilt.com|jetsetter.com|roomerluxury.com|roomertravel.com|shermanstravel.com|society19.com|society19travel.com|co.uk.sales.secretescapes.com|travel.discountvouchers.co.uk|secretescapes.holidaypirates.com|(www.)*luxurylinkescapes.com|se-sales-asia.darkbluehq.com|travelbird.com|sales.travelbird.com|escapes.oe24.at|secretescapes.com|www.gilttravel.com|www.mailescapes.com|www.secretescapes.group|www.secretescapescf.com|magazine.secretescapes.com|mp.secretescapes.com|se-sales-asia.darkbluehq.com|www.mycityvenueescapes.com).*'
               THEN 'UK'

           WHEN
                   LOWER(string_hostname) REGEXP
                   '.*(cats.www.secretescapes.de.meowbify.com|escapes.fernweh-aktuell.com|escapes.travelbook.de|luxusreisecluc.urlaubsplus.de|luxusurlauc.tagesspiegel.de|secretescapes.brigitte.de|secretescapes.computerbild.de|secretescapes.de|secretescapes.urlaubsguru.de|secretescapes.urlaubspiraten.de|specials.rp-online.de|traumreisen.welt.de|travelbird.de|travelbook.com|travista.de|urlaubsguru.de|urlaubsplus.de|www.secretescapes.de|www.secretescapes.hna.de|se-sales-de.darkbluehq.com|escapist-de.secretescapes.cloud.ec|de.sales.secretescapes.com|escapes.oe24.at).*'
               THEN 'DE'

           WHEN
                   LOWER(string_hostname) REGEXP
                   '.*(dk.secretescapes.com|travelbird.dk|escapes.campadre.dk|dk.sales.secretescapes.com).*'
               THEN 'DK'

           WHEN
                   LOWER(string_hostname) REGEXP
                   '.*(id.secretescapes.com).*'
               THEN 'ID'

           WHEN
                   LOWER(string_hostname) REGEXP
                   '.*(be.secretescapes.com|fr.travelbird.be|travelbird.be|(admin.)*be.sales.secretescapes.com|hotels.shedeals.be).*'
               THEN 'BE'

           WHEN
                   LOWER(string_hostname) REGEXP
                   '.*(proprietes.lefigaro.fr/location-vacances|travelbird.fr|www.evasionssecretes.fr|tresor.voyagespirates.fr|evasionssecretes.perfectstay.com|idee-evasion.lefigaro.fr).*'
               THEN 'FR'

           WHEN
                   LOWER(string_hostname) REGEXP
                   '(es.secretescapes.com|se-sales-es.darkbluehq.com).*'
               THEN 'ES'

           WHEN
                   LOWER(string_hostname) REGEXP
                   '.*(it.secretescapes.com|it.sales.secretescapes.com|secretescapes.perfectstay.com|se-sales-it.darkbluehq.com|escapist-it.secretescapes.cloud.ec|secretdeals.lol.travel).*'
               THEN 'IT'

           WHEN
                   LOWER(string_hostname) REGEXP
                   '.*(travelbird.at|www.secretescapes.at).*'
               THEN 'AT'

           WHEN
                   LOWER(string_hostname) REGEXP
                   '.*(travelist).*'
               THEN 'PL'

           WHEN
                   LOWER(string_hostname) REGEXP
                   '.*(hk.secretescapes.com).*'
               THEN 'HK'

           WHEN
                   LOWER(string_hostname) REGEXP
                   '.*(nl.secretescapes.com|travelbird.nl|travelhotelcard.com|nl.sales.secretescapes.com).*'
               THEN 'NL'

           WHEN
                   LOWER(string_hostname) REGEXP
                   '.*(my.secretescapes.com).*'
               THEN 'MY'

           WHEN
                   LOWER(string_hostname) REGEXP
                   '.*(ie.secretescapes.com).*'
               THEN 'IE'

           WHEN
                   LOWER(string_hostname) REGEXP
                   '.*(hu.secretescapes.com).*'
               THEN 'HU'

           WHEN
                   LOWER(string_hostname) REGEXP
                   '.*(cz.secretescapes.com).*'
               THEN 'CZ'

           WHEN
                   LOWER(string_hostname) REGEXP
                   '.*(sg.secretescapes.com).*'
               THEN 'SG'

           WHEN
                   LOWER(string_hostname) REGEXP
                   '.*(sk.secretescapes.com).*'
               THEN 'SK'

           WHEN
                   LOWER(string_hostname) REGEXP
                   '.*(no.secretescapes.com|travelbird.no~www.secretescapes|www.secretescapes.no|travelbird.no).*'
               THEN 'NO'

           WHEN
                   LOWER(string_hostname) REGEXP
                   '.*(campadre.se|travelbird.se|www.secretescapes.se|se.sales.secretescapes.com|travelbird.fi|escapes.campadre.com|escapes.campadre.se).*'
               THEN 'SE'

           WHEN
                   LOWER(string_hostname) REGEXP
                   '.*(ch.secretescapes.com|travelbird.ch|se-sales-ch.darkbluehq.com|www.travelescapes.ch).*'
               THEN 'CH'

           WHEN
                   LOWER(string_hostname) REGEXP
                   '.*(fathomaway.com|gilt.com|jetsetter.com|roomerluxury.com|roomertravel.com|shermanstravel.com|society19.com|society19travel.com|us.secretescapes.com|se-sales-us.darkbluehq.com|escapes-us.timeout.com|www.luxurylinkescapes.com|www.gilttravel.com|www.mailescapes.com).*'
               THEN 'US'

           WHEN
                       LOWER(string_hostname) REGEXP
                       '(.*(web|db-loadtesting|sandbox).*\\.secretescapes\\.com|.*.fs-staging.escapes.tech|[0-9]{{1,3}}\\.[0-9]{{1,3}}\\.[0-9]{{1,3}}\\.[0-9]{{1,3}}|.*\\.amazonaws\\.com)'
                   OR
                       LOWER(string_hostname) IN (
                                                  'api.secretescapes.com',
                                                  'applitool-affiliate.secretescapes.com',
                                                  'applitools-whitelabel.secretescapes.com',
                                                  'cdn.secretescapes.com',
                                                  'click.ebm.secretescapes.com',
                                                  'click.email.secretescapes.com',
                                                  'dev.secretescapes.com',
                                                  'flights.secretescapes.com',
                                                  'mobile-staging.secretescapes.com',
                                                  'staging.secretescapes.com',
                                                  'staging01.secretescapes.com',
                                                  'staging02.secretescapes.com',
                                                  'tracker.secretescapes.com'
                           )
               THEN 'SE TECH'


           END                                            AS touch_hostname_territory;




SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes stba
WHERE stba.touch_hostname = 'ec2-54-73-62-134.eu-west-1.compute.amazonaws.com'



SELECT 'ec2-54-73-62-134.eu-west-1.compute.amazonaws.com' REGEXP '(.*(web|db-loadtesting|sandbox).*\\.secretescapes\\.com|.*\\.fs-staging\\.escapes\\.tech|[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}|.*\\.amazonaws\\.com)'


self_describing_task --include 'dv/dwh/events/04_touch_basic_attributes/01_module_touch_basic_attributes.py'  --method 'run' --start '2021-02-03 00:00:00' --end '2021-02-03 00:00:00'

;

USE WAREHOUSE pipe_xlarge;

UPDATE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
SET touch_hostname_territory = 'SE TECH'
WHERE touch_hostname REGEXP '(.*(web|db-loadtesting|sandbox).*\\.secretescapes\\.com|.*\\.fs-staging\\.escapes\\.tech|[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}|.*\\.amazonaws\\.com)'

UPDATE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel
SET touch_hostname_territory = 'SE TECH',
    touch_affiliate_territory = 'SE TECH'
WHERE touch_hostname REGEXP '(.*(web|db-loadtesting|sandbox).*\\.secretescapes\\.com|.*\\.fs-staging\\.escapes\\.tech|[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}|.*\\.amazonaws\\.com)'


self_describing_task --include 'dv/dwh/events/04_touch_basic_attributes/01_module_touch_basic_attributes.py'  --method 'run' --start '2021-02-03 00:00:00' --end '2021-02-03 00:00:00'

/Users/robin/myrepos/one-data-pipeline/biapp/task_catalogue/dv/dwh/events/04_touch_basic_attributes/01_module_touch_basic_attributes.py

SELECT * FROM scratch.robinpatel.