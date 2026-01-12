CREATE OR REPLACE TABLE hygiene_vault_mvp_dev_robin.cms_mongodb.booking_summary CLONE hygiene_vault_mvp.cms_mongodb.booking_summary;

UPDATE hygiene_vault_mvp_dev_robin.cms_mongodb.booking_summary bs
SET bs.device_platform = 'native app android'
WHERE bs.device_platform = 'mobile wrap android'
  AND bs.platform_name__o = 'ANDROID_APP';

CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mongodb.booking_summary CLONE hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary;

UPDATE hygiene_snapshot_vault_mvp_dev_robin.cms_mongodb.booking_summary bs
SET bs.device_platform = 'native app android'
WHERE bs.device_platform = 'mobile wrap android'
  AND bs.platform_name__o = 'ANDROID_APP';


self_describing_task --include 'hygiene/cms_mongodb/booking_summary.py'  --method 'run' --start '2020-11-05 00:00:00' --end '2020-11-05 00:00:00'

CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.cms_mongodb.booking_summary CLONE raw_vault_mvp.cms_mongodb.booking_summary;

SELECT *
FROM hygiene_vault_mvp_dev_robin.cms_mongodb.booking_summary bs
WHERE bs.platform_name__o = 'ANDROID_APP';

CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.dwh.user_subscription_event CLONE data_vault_mvp.dwh.user_subscription_event;

SELECT *
FROM data_vault_mvp.dwh.se_booking sb
WHERE sb.booking_id IN (
    SELECT bs.booking_id FROM hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary bs WHERE bs.platform_name__o = 'ANDROID_APP'
);

airflow backfill --start_date '2020-11-08 03:00:00' --end_date '2020-11-08 03:00:00' --task_regex '.*' dwh__transactional__booking__daily_at_03h00

SELECT *
FROM se.data_pii.se_user_subscription_event
WHERE user_id = 62972247;

SELECT *
FROM se.data.active_user_base aub;

------------------------------------------------------------------------------------------------------------------------
--active user base bug

SELECT se.data.platform_from_touch_experience('native app android');

CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;
CREATE OR REPLACE TABLE se_dev_robin.data.user_emails CLONE se.data.user_emails;

CREATE OR REPLACE TABLE se_dev_robin.data.user_activity CLONE se.data.user_activity;
DELETE
FROM se_dev_robin.data.user_activity
WHERE date > '2020-11-02';

self_describing_task --include 'se/data/se_user_activity.py'  --method 'run' --start '2020-11-02 00:00:00' --end '2020-11-02 00:00:00'
-- need to rerun all historic runs

SELECT *
FROM se_dev_robin.data.user_activity ua
WHERE date = (
    SELECT MAX(date)
    FROM se_dev_robin.data.user_activity u
);

CREATE OR REPLACE TABLE se_dev_robin.data.active_user_base CLONE se.data.active_user_base;

DELETE
FROM se_dev_robin.data.active_user_base aub
WHERE date >= '2020-11-02';

SELECT *
FROM se_dev_robin.data.active_user_base
WHERE date = (
    SELECT MAX(date)
    FROM se_dev_robin.data.active_user_base
);

self_describing_task --include 'se/data/se_active_user_base.py'  --method 'run' --start '2020-11-02 00:00:00' --end '2020-11-02 00:00:00'

SELECT *
FROM se.data.se_calendar sc
WHERE sc.date_value >= current_date - 10
  AND sc.date_value <= current_date;

airflow clear --start_date '2020-11-02 03:00:00' --end_date '2020-11-08 03:00:00' --task_regex '.*' dwh__user_activity__daily_at_03h00
airflow backfill --start_date '2020-11-02 00:00:00' --end_date '2020-11-08 00:00:00' --task_regex '.*' dwh__user_activity__daily_at_03h00

airflow clear --start_date '2020-11-02 03:00:00' --end_date '2020-11-08 03:00:00' --task_regex '.*' dwh__user_activity__daily_at_03h00

airflow clear --start_date '2020-11-02 03:00:00' --end_date '2020-11-02 03:00:00' --task_regex '.*' active_user_base__daily_at_03h00
airflow backfill --start_date '2020-11-02 03:00:00' --end_date '2020-11-02 03:00:00' --task_regex '.*' active_user_base__daily_at_03h00

SELECT date, count(*)
FROM se.data.active_user_base aub
GROUP BY 1

SELECT *
FROM se.data.active_user_base aub
WHERE date >= '2020-11-01';



SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel mtmc
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba ON mtmc.touch_id = mtba.touch_id
WHERE mtmc.touch_affiliate_territory IS NULL
  AND mtba.touch_start_tstamp >= '2020-09-01';

CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel;





SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
WHERE mtba.touch_hostname_territory IS NULL
  AND mtba.touch_experience LIKE 'native app%'

USE WAREHOUSE pipe_xlarge;



SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel mtmc ON mtba.touch_id = mtmc.touch_id
WHERE mtmc.touch_affiliate_territory IS NULL
  AND mtba.touch_experience = 'native app ios';

CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;

UPDATE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes target
SET target.touch_hostname = PARSE_URL(target.touch_landing_page):host::VARCHAR
WHERE target.touch_landing_page IS NOT NULL
  AND target.touch_hostname IS NULL;

UPDATE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes target
SET target.touch_hostname_territory = CASE
                                          WHEN
                                                  LOWER(touch_hostname) REGEXP
                                                  '(blue.secretescapes.com|(www.)*confidentialescapes.co.uk|escapes.instyle.co.uk|escapes.planetradiooffers.co.uk|escapes.radiotimes.com|escapes.timeout.com|escapes.vikingfm-offers.co.uk|escapes.wave105deals.co.uk|(www.)*eveningstandardescapes.com|(www.)*guardianescapes.com|(www.)*hand-picked.telegraph.co.uk|hellomagazine.com/travel|holidays.pigsback.com|icelollyescapes.com|independent.co.uk/travelholidays|independent.secretescapes.com|(www.)*independentescapes.com|(www.)*lateluxury.com|mailescapes.co.uk|mycityvenueescapes.com|planetconfidential.co.uk|planetradiooffers.co.uk|secretsales.secretescapes.com|standard.co.uk/lifestyletravel|talktalkescapes.com|teletext.secretescapes.com|travel.radiotimes.com|trips.5pm.co.uk|(www.)*secretescapes.com|fathomaway.com|gilt.com|jetsetter.com|roomerluxury.com|roomertravel.com|shermanstravel.com|society19.com|society19travel.com|co.uk.sales.secretescapes.com|travel.discountvouchers.co.uk|secretescapes.holidaypirates.com|(www.)*luxurylinkescapes.com|se-sales-asia.darkbluehq.com|travelbird.com|sales.travelbird.com|escapes.oe24.at|secretescapes.com|www.gilttravel.com|www.mailescapes.com|www.secretescapes.group|www.secretescapescf.com|magazine.secretescapes.com|mp.secretescapes.com|se-sales-asia.darkbluehq.com).*'
                                              THEN 'UK'

                                          WHEN
                                                  LOWER(touch_hostname) REGEXP
                                                  '.*(cats.www.secretescapes.de.meowbify.com|escapes.fernweh-aktuell.com|escapes.travelbook.de|luxusreisecluc.urlaubsplus.de|luxusurlauc.tagesspiegel.de|secretescapes.brigitte.de|secretescapes.computerbild.de|secretescapes.de|secretescapes.urlaubsguru.de|secretescapes.urlaubspiraten.de|specials.rp-online.de|traumreisen.welt.de|travelbird.de|travelbook.com|travista.de|urlaubsguru.de|urlaubsplus.de|www.secretescapes.de|www.secretescapes.hna.de|se-sales-de.darkbluehq.com|escapist-de.secretescapes.cloud.ec|de.sales.secretescapes.com|escapes.oe24.at).*'
                                              THEN 'DE'

                                          WHEN
                                                  LOWER(touch_hostname) REGEXP
                                                  '.*(dk.secretescapes.com|travelbird.dk|escapes.campadre.dk|dk.sales.secretescapes.com).*'
                                              THEN 'DK'

                                          WHEN
                                                  LOWER(touch_hostname) REGEXP
                                                  '.*(id.secretescapes.com).*'
                                              THEN 'ID'

                                          WHEN
                                                  LOWER(touch_hostname) REGEXP
                                                  '.*(be.secretescapes.com|fr.travelbird.be|travelbird.be|(admin.)*be.sales.secretescapes.com|hotels.shedeals.be).*'
                                              THEN 'BE'

                                          WHEN
                                                  LOWER(touch_hostname) REGEXP
                                                  '.*(proprietes.lefigaro.fr/location-vacances|travelbird.fr|www.evasionssecretes.fr|tresor.voyagespirates.fr|evasionssecretes.perfectstay.com|idee-evasion.lefigaro.fr).*'
                                              THEN 'FR'

                                          WHEN
                                                  LOWER(touch_hostname) REGEXP
                                                  '(es.secretescapes.com|se-sales-es.darkbluehq.com).*'
                                              THEN 'ES'

                                          WHEN
                                                  LOWER(touch_hostname) REGEXP
                                                  '.*(it.secretescapes.com|it.sales.secretescapes.com|secretescapes.perfectstay.com|se-sales-it.darkbluehq.com|escapist-it.secretescapes.cloud.ec|secretdeals.lol.travel).*'
                                              THEN 'IT'

                                          WHEN
                                                  LOWER(touch_hostname) REGEXP
                                                  '.*(travelbird.at|www.secretescapes.at).*'
                                              THEN 'AT'

                                          WHEN
                                                  LOWER(touch_hostname) REGEXP
                                                  '.*(travelist).*'
                                              THEN 'PL'

                                          WHEN
                                                  LOWER(touch_hostname) REGEXP
                                                  '.*(hk.secretescapes.com).*'
                                              THEN 'HK'

                                          WHEN
                                                  LOWER(touch_hostname) REGEXP
                                                  '.*(nl.secretescapes.com|travelbird.nl|travelhotelcard.com|nl.sales.secretescapes.com).*'
                                              THEN 'NL'

                                          WHEN
                                                  LOWER(touch_hostname) REGEXP
                                                  '.*(my.secretescapes.com).*'
                                              THEN 'MY'

                                          WHEN
                                                  LOWER(touch_hostname) REGEXP
                                                  '.*(ie.secretescapes.com).*'
                                              THEN 'IE'

                                          WHEN
                                                  LOWER(touch_hostname) REGEXP
                                                  '.*(hu.secretescapes.com).*'
                                              THEN 'HU'

                                          WHEN
                                                  LOWER(touch_hostname) REGEXP
                                                  '.*(cz.secretescapes.com).*'
                                              THEN 'CZ'

                                          WHEN
                                                  LOWER(touch_hostname) REGEXP
                                                  '.*(sg.secretescapes.com).*'
                                              THEN 'SG'

                                          WHEN
                                                  LOWER(touch_hostname) REGEXP
                                                  '.*(sk.secretescapes.com).*'
                                              THEN 'SK'

                                          WHEN
                                                  LOWER(touch_hostname) REGEXP
                                                  '.*(no.secretescapes.com|travelbird.no~www.secretescapes|www.secretescapes.no|travelbird.no).*'
                                              THEN 'NO'

                                          WHEN
                                                  LOWER(touch_hostname) REGEXP
                                                  '.*(campadre.se|travelbird.se|www.secretescapes.se|se.sales.secretescapes.com|travelbird.fi|escapes.campadre.com|escapes.campadre.se).*'
                                              THEN 'SE'

                                          WHEN
                                                  LOWER(touch_hostname) REGEXP
                                                  '.*(ch.secretescapes.com|travelbird.ch|se-sales-ch.darkbluehq.com|www.travelescapes.ch).*'
                                              THEN 'CH'

                                          WHEN
                                                  LOWER(touch_hostname) REGEXP
                                                  '.*(fathomaway.com|gilt.com|jetsetter.com|roomerluxury.com|roomertravel.com|shermanstravel.com|society19.com|society19travel.com|us.secretescapes.com|se-sales-us.darkbluehq.com|escapes-us.timeout.com|www.luxurylinkescapes.com|www.gilttravel.com|www.mailescapes.com).*'
                                              THEN 'US'

                                          WHEN
                                                      LOWER(touch_hostname) REGEXP
                                                      '(.*(web|db-loadtesting|sandbox).*\\\\.secretescapes.com|.*.fs-staging.escapes.tech|[0-9]{{{{1,3}}}}\\\\.[0-9]{{{{1,3}}}}\\\\.[0-9]{{{{1,3}}}}\\\\.[0-9]{{{{1,3}}}})'
                                                  OR
                                                      LOWER(touch_hostname) IN (
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

                                          WHEN touch_hostname IS NULL AND
                                               touch_experience IN ('native app ios', 'native app android', 'not specified')
                                              THEN IFF(touch_posa_territory = 'GB', 'UK', touch_posa_territory) -- Native app does not have a hostname so we are lifting the territory from 'app id' in atomic events. Note to explore the robustness of this method.
                                          WHEN touch_posa_territory IS NOT NULL
                                              THEN IFF(touch_posa_territory = 'GB', 'UK', touch_posa_territory)
                                          ELSE 'Other'
    END
WHERE target.touch_hostname_territory IS NULL
;


UPDATE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel target
SET target.touch_hostname_territory = batch.touch_hostname_territory
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes batch
WHERE target.touch_id = batch.touch_id
  AND target.touch_hostname_territory IS NULL
  AND batch.touch_hostname_territory IS NOT NULL;

UPDATE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel target
SET target.touch_affiliate_territory = target.touch_hostname_territory
WHERE target.touch_affiliate_territory IS NULL
  AND target.touch_hostname_territory IS NOT NULL;

SELECT PARSE_URL('https://www.secretescapes.com/'):host::VARCHAR


SELECT DATE_TRUNC(WEEK, touch_start_tstamp) AS week,
       mtba.touch_experience,
       count(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes mtba
         INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel mtmc ON mtba.touch_id = mtmc.touch_id
WHERE mtmc.touch_affiliate_territory IS NULL
GROUP BY 1, 2;