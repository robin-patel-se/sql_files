SELECT fcb.booking_completed_date::DATE                                             AS date,
       target.page_url.channel_category(stmc.touch_mkt_channel)                     AS channel, -- last click channel
       target.page_url.platform_from_touch_experience(stba.touch_experience)        AS platform,
       target.page_url.posa_category_from_territory(stmc.touch_affiliate_territory) AS posa_category,
       stba.touch_experience,
       stmc.*
FROM target.page_url.fact_complete_booking fcb
         INNER JOIN target.page_url.scv_touched_transactions stt ON fcb.booking_id = stt.booking_id
         INNER JOIN target.page_url.scv_touch_marketing_channel stmc ON stt.touch_id = stmc.touch_id
         INNER JOIN target.page_url.scv_touch_basic_attributes stba ON stt.touch_id = stba.touch_id
WHERE fcb.booking_completed_date >= '2020-09-01'
  AND posa_category = 'Other';

SELECT *
FROM target.page_url.scv_touch_basic_attributes stba
         INNER JOIN target.page_url.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
WHERE stba.touch_start_tstamp::DATE = '2020-10-04'
  AND stba.touch_hostname IS NULL;



SELECT fcb.booking_completed_date::DATE                                      AS date,
       target.page_url.channel_category(stmc.touch_mkt_channel)              AS channel, -- last click channel
       target.page_url.platform_from_touch_experience(stba.touch_experience) AS platform,
       stmc.touch_affiliate_territory,
       count(*)
FROM target.page_url.fact_complete_booking fcb
         INNER JOIN target.page_url.scv_touched_transactions stt ON fcb.booking_id = stt.booking_id
         INNER JOIN target.page_url.scv_touch_marketing_channel stmc ON stt.touch_id = stmc.touch_id
         INNER JOIN target.page_url.scv_touch_basic_attributes stba ON stt.touch_id = stba.touch_id
WHERE fcb.booking_completed_date >= '2020-09-01'
GROUP BY 1, 2, 3, 4;


--DATE          CHANNEL	PLATFORM	TOUCH_AFFILIATE_TERRITORY	COUNT(*)
--2020-10-12	Free	Web	        Other	                    585

SELECT stba.*,
       stmc.*
FROM target.page_url.fact_complete_booking fcb
         INNER JOIN target.page_url.scv_touched_transactions stt ON fcb.booking_id = stt.booking_id
         INNER JOIN target.page_url.scv_touch_marketing_channel stmc ON stt.touch_id = stmc.touch_id
         INNER JOIN target.page_url.scv_touch_basic_attributes stba ON stt.touch_id = stba.touch_id
WHERE fcb.booking_completed_date::DATE = '2020-10-12'
  AND target.page_url.channel_category(stmc.touch_mkt_channel) = 'Free'
  AND target.page_url.platform_from_touch_experience(stba.touch_experience) = 'Web'
  AND stmc.touch_affiliate_territory = 'Other';

SELECT parse_url('us.secretescapes.com');


SELECT *
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.useragent = 'data_team_artificial_insemination_transactions'
  AND es.collector_tstamp >= '2020-10-01';

SELECT *
FROM snowplow.atomic.events e
WHERE e.etl_tstamp >= '2020-10-01';



USE WAREHOUSE pipe_xlarge;
SELECT stba.touch_start_tstamp::DATE                                                AS date,
       target.page_url.channel_category(stmc.touch_mkt_channel)                     AS channel, -- last click channel
       target.page_url.platform_from_touch_experience(stba.touch_experience)        AS platform,
       target.page_url.posa_category_from_territory(stmc.touch_affiliate_territory) AS posa_category,
       stba.touch_experience,
       stmc.*
FROM target.page_url.scv_touch_basic_attributes stba
         INNER JOIN target.page_url.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
WHERE stba.touch_start_tstamp >= '2020-09-01';


--update event stream
CREATE OR REPLACE TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream CLONE hygiene_vault_mvp.snowplow.event_stream;
CREATE OR REPLACE TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream_art_bookings AS
SELECT *
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream es
WHERE useragent = 'data_team_artificial_insemination_transactions';

UPDATE hygiene_vault_mvp_dev_robin.snowplow.event_stream_art_bookings target
SET target.page_url = 'https://' || target.page_url || '/';

USE WAREHOUSE pipe_xlarge;
MERGE INTO hygiene_vault_mvp_dev_robin.snowplow.event_stream target
    USING hygiene_vault_mvp_dev_robin.snowplow.event_stream_art_bookings
        AS batch ON target.event_hash = batch.event_hash
    WHEN MATCHED THEN UPDATE SET
        target.page_url = batch.page_url;


--update sessions
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes_art_sess AS
SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes mtba
WHERE mtba.useragent = 'data_team_artificial_insemination_transactions';

UPDATE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes_art_sess target
SET target.touch_landing_page       = IFF(LEFT(target.touch_landing_page, 8) IS DISTINCT FROM 'https://',
                                          'https://' || target.touch_landing_page || '/', target.touch_landing_page),
    target.touch_hostname           = target.touch_landing_page,
    target.touch_hostname_territory = CASE
                                          WHEN
                                                  LOWER(target.touch_landing_page) REGEXP
                                                  '(blue.secretescapes.com|(www.)*confidentialescapes.co.uk|escapes.instyle.co.uk|escapes.planetradiooffers.co.uk|escapes.radiotimes.com|escapes.timeout.com|escapes.vikingfm-offers.co.uk|escapes.wave105deals.co.uk|(www.)*eveningstandardescapes.com|(www.)*guardianescapes.com|(www.)*hand-picked.telegraph.co.uk|hellomagazine.com/travel|holidays.pigsback.com|icelollyescapes.com|independent.co.uk/travelholidays|independent.secretescapes.com|(www.)*independentescapes.com|(www.)*lateluxury.com|mailescapes.co.uk|mycityvenueescapes.com|planetconfidential.co.uk|planetradiooffers.co.uk|secretsales.secretescapes.com|standard.co.uk/lifestyletravel|talktalkescapes.com|teletext.secretescapes.com|travel.radiotimes.com|trips.5pm.co.uk|(www.)*secretescapes.com|fathomaway.com|gilt.com|jetsetter.com|roomerluxury.com|roomertravel.com|shermanstravel.com|society19.com|society19travel.com|co.uk.sales.secretescapes.com|travel.discountvouchers.co.uk|secretescapes.holidaypirates.com|(www.)*luxurylinkescapes.com|se-sales-asia.darkbluehq.com|travelbird.com|sales.travelbird.com|escapes.oe24.at|secretescapes.com|www.gilttravel.com|www.mailescapes.com|www.secretescapes.group|www.secretescapescf.com|magazine.secretescapes.com|mp.secretescapes.com|se-sales-asia.darkbluehq.com).*'
                                              THEN 'UK'

                                          WHEN
                                                  LOWER(target.touch_landing_page) REGEXP
                                                  '.*(cats.www.secretescapes.de.meowbify.com|escapes.fernweh-aktuell.com|escapes.travelbook.de|luxusreisecluc.urlaubsplus.de|luxusurlauc.tagesspiegel.de|secretescapes.brigitte.de|secretescapes.computerbild.de|secretescapes.de|secretescapes.urlaubsguru.de|secretescapes.urlaubspiraten.de|specials.rp-online.de|traumreisen.welt.de|travelbird.de|travelbook.com|travista.de|urlaubsguru.de|urlaubsplus.de|www.secretescapes.de|www.secretescapes.hna.de|se-sales-de.darkbluehq.com|escapist-de.secretescapes.cloud.ec|de.sales.secretescapes.com|escapes.oe24.at).*'
                                              THEN 'DE'

                                          WHEN
                                                  LOWER(target.touch_landing_page) REGEXP
                                                  '.*(dk.secretescapes.com|travelbird.dk|escapes.campadre.dk|dk.sales.secretescapes.com).*'
                                              THEN 'DK'

                                          WHEN
                                                  LOWER(target.touch_landing_page) REGEXP
                                                  '.*(id.secretescapes.com).*'
                                              THEN 'ID'

                                          WHEN
                                                  LOWER(target.touch_landing_page) REGEXP
                                                  '.*(be.secretescapes.com|fr.travelbird.be|travelbird.be|(admin.)*be.sales.secretescapes.com).*'
                                              THEN 'BE'

                                          WHEN
                                                  LOWER(target.touch_landing_page) REGEXP
                                                  '.*(proprietes.lefigaro.fr/location-vacances|travelbird.fr|www.evasionssecretes.fr|tresor.voyagespirates.fr|evasionssecretes.perfectstay.com).*'
                                              THEN 'FR'

                                          WHEN
                                                  LOWER(target.touch_landing_page) REGEXP
                                                  '(es.secretescapes.com|se-sales-es.darkbluehq.com).*'
                                              THEN 'ES'

                                          WHEN
                                                  LOWER(target.touch_landing_page) REGEXP
                                                  '.*(it.secretescapes.com|secretescapes.perfectstay.com|se-sales-it.darkbluehq.com|escapist-it.secretescapes.cloud.ec|secretdeals.lol.travel).*'
                                              THEN 'IT'

                                          WHEN
                                                  LOWER(target.touch_landing_page) REGEXP
                                                  '.*(travelbird.at|www.secretescapes.at).*'
                                              THEN 'AT'

                                          WHEN
                                                  LOWER(target.touch_landing_page) REGEXP
                                                  '.*(travelist).*'
                                              THEN 'PL'

                                          WHEN
                                                  LOWER(target.touch_landing_page) REGEXP
                                                  '.*(hk.secretescapes.com).*'
                                              THEN 'HK'

                                          WHEN
                                                  LOWER(target.touch_landing_page) REGEXP
                                                  '.*(nl.secretescapes.com|travelbird.nl|travelhotelcard.com|nl.sales.secretescapes.com).*'
                                              THEN 'NL'

                                          WHEN
                                                  LOWER(target.touch_landing_page) REGEXP
                                                  '.*(my.secretescapes.com).*'
                                              THEN 'MY'

                                          WHEN
                                                  LOWER(target.touch_landing_page) REGEXP
                                                  '.*(ie.secretescapes.com).*'
                                              THEN 'IE'

                                          WHEN
                                                  LOWER(target.touch_landing_page) REGEXP
                                                  '.*(hu.secretescapes.com).*'
                                              THEN 'HU'

                                          WHEN
                                                  LOWER(target.touch_landing_page) REGEXP
                                                  '.*(cz.secretescapes.com).*'
                                              THEN 'CZ'

                                          WHEN
                                                  LOWER(target.touch_landing_page) REGEXP
                                                  '.*(sg.secretescapes.com).*'
                                              THEN 'SG'

                                          WHEN
                                                  LOWER(target.touch_landing_page) REGEXP
                                                  '.*(sk.secretescapes.com).*'
                                              THEN 'SK'

                                          WHEN
                                                  LOWER(target.touch_landing_page) REGEXP
                                                  '.*(no.secretescapes.com|travelbird.no~www.secretescapes|www.secretescapes.no|travelbird.no).*'
                                              THEN 'NO'

                                          WHEN
                                                  LOWER(target.touch_landing_page) REGEXP
                                                  '.*(campadre.se|travelbird.se|www.secretescapes.se|se.sales.secretescapes.com|travelbird.fi|escapes.campadre.com|escapes.campadre.se).*'
                                              THEN 'SE'

                                          WHEN
                                                  LOWER(target.touch_landing_page) REGEXP
                                                  '.*(ch.secretescapes.com|travelbird.ch|se-sales-ch.darkbluehq.com|www.travelescapes.ch).*'
                                              THEN 'CH'

                                          WHEN
                                                  LOWER(target.touch_landing_page) REGEXP
                                                  '.*(fathomaway.com|gilt.com|jetsetter.com|roomerluxury.com|roomertravel.com|shermanstravel.com|society19.com|society19travel.com|us.secretescapes.com|se-sales-us.darkbluehq.com|escapes-us.timeout.com|www.luxurylinkescapes.com|www.gilttravel.com|www.mailescapes.com).*'
                                              THEN 'US'

                                          WHEN
                                                      LOWER(target.touch_landing_page) REGEXP
                                                      '(.*(web|db-loadtesting|sandbox).*\\\\.secretescapes.com|.*.fs-staging.escapes.tech|[0-9]{{{{1,3}}}}\\\\.[0-9]{{{{1,3}}}}\\\\.[0-9]{{{{1,3}}}}\\\\.[0-9]{{{{1,3}}}})'
                                                  OR
                                                      LOWER(target.touch_landing_page) IN (
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

                                          WHEN target.touch_landing_page IS NULL AND target.touch_experience = 'native app'
                                              THEN touch_posa_territory -- Native app does not have a hostname so we are lifting the territory from 'app id' in atomic events. Note to explore the robustness of this method.

                                          ELSE 'Other'
        END
;

SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes_art_sess;

MERGE INTO data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes target
    USING data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes_art_sess
        AS batch ON target.touch_id = batch.touch_id
    WHEN MATCHED THEN UPDATE SET
        target.touch_landing_page = batch.touch_landing_page,
        target.touch_hostname = batch.touch_hostname,
        target.touch_hostname_territory = batch.touch_hostname_territory;

SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
WHERE useragent = 'data_team_artificial_insemination_transactions';

CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel;
UPDATE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel target
SET target.touch_landing_page = batch.touch_landing_page,
    target.touch_hostname     = batch.touch_hostname
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes batch
WHERE target.touch_id = batch.touch_id
  AND target.touch_hostname IS NULL;

UPDATE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel target
SET target.touch_hostname_territory = batch.touch_hostname_territory
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes batch
WHERE target.touch_id = batch.touch_id
  AND target.touch_hostname_territory = 'Other'
  AND batch.touch_hostname_territory IS DISTINCT FROM 'Other';

USE WAREHOUSE pipe_xlarge;

UPDATE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel target
SET target.touch_affiliate_territory = target.touch_hostname_territory
WHERE target.touch_affiliate_territory = 'Other';

SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes mtba
         INNER JOIN
     data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel mtmc ON mtba.touch_id = mtmc.touch_id
WHERE mtba.useragent = 'data_team_artificial_insemination_transactions';


------------------------------------------------------------------------------------------------------------------------

SELECT *
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream es
WHERE es.useragent = 'data_team_artificial_insemination_transactions'
LIMIT 50;


SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
WHERE mtba.useragent = 'data_team_artificial_insemination_transactions';

SELECT date_trunc(WEEK, mtba.touch_start_tstamp) AS week,
       count(*)
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
WHERE mtba.useragent = 'data_team_artificial_insemination_transactions'
GROUP BY 1;

SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
WHERE mtba.touch_hostname_territory = 'Other'
  AND mtba.touch_start_tstamp BETWEEN '2020-09-28' AND '2020-10-04';

------------------------------------------------------------------------------------------------------------------------


CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;

--all landing pages with incorrect url add https://
UPDATE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes AS target
SET target.touch_landing_page = 'https://' || target.touch_landing_page || '/'
WHERE parse_url(target.touch_landing_page, 1):error = 'scheme not specified';

--check that no other errors have been created
SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes mtba
WHERE parse_url(mtba.touch_landing_page, 1):error = 'scheme not specified';

--update hostname
UPDATE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes AS target
SET target.touch_hostname = PARSE_URL(target.touch_landing_page):host::VARCHAR
WHERE target.touch_hostname IS NULL;


--update hostname territory
UPDATE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes target
SET target.touch_hostname_territory = CASE
                                          WHEN
                                                  LOWER(target.touch_hostname) REGEXP
                                                  '(blue.secretescapes.com|(www.)*confidentialescapes.co.uk|escapes.instyle.co.uk|escapes.planetradiooffers.co.uk|escapes.radiotimes.com|escapes.timeout.com|escapes.vikingfm-offers.co.uk|escapes.wave105deals.co.uk|(www.)*eveningstandardescapes.com|(www.)*guardianescapes.com|(www.)*hand-picked.telegraph.co.uk|hellomagazine.com/travel|holidays.pigsback.com|icelollyescapes.com|independent.co.uk/travelholidays|independent.secretescapes.com|(www.)*independentescapes.com|(www.)*lateluxury.com|mailescapes.co.uk|mycityvenueescapes.com|planetconfidential.co.uk|planetradiooffers.co.uk|secretsales.secretescapes.com|standard.co.uk/lifestyletravel|talktalkescapes.com|teletext.secretescapes.com|travel.radiotimes.com|trips.5pm.co.uk|(www.)*secretescapes.com|fathomaway.com|gilt.com|jetsetter.com|roomerluxury.com|roomertravel.com|shermanstravel.com|society19.com|society19travel.com|co.uk.sales.secretescapes.com|travel.discountvouchers.co.uk|secretescapes.holidaypirates.com|(www.)*luxurylinkescapes.com|se-sales-asia.darkbluehq.com|travelbird.com|sales.travelbird.com|escapes.oe24.at|secretescapes.com|www.gilttravel.com|www.mailescapes.com|www.secretescapes.group|www.secretescapescf.com|magazine.secretescapes.com|mp.secretescapes.com|se-sales-asia.darkbluehq.com).*'
                                              THEN 'UK'

                                          WHEN
                                                  LOWER(target.touch_hostname) REGEXP
                                                  '.*(cats.www.secretescapes.de.meowbify.com|escapes.fernweh-aktuell.com|escapes.travelbook.de|luxusreisecluc.urlaubsplus.de|luxusurlauc.tagesspiegel.de|secretescapes.brigitte.de|secretescapes.computerbild.de|secretescapes.de|secretescapes.urlaubsguru.de|secretescapes.urlaubspiraten.de|specials.rp-online.de|traumreisen.welt.de|travelbird.de|travelbook.com|travista.de|urlaubsguru.de|urlaubsplus.de|www.secretescapes.de|www.secretescapes.hna.de|se-sales-de.darkbluehq.com|escapist-de.secretescapes.cloud.ec|de.sales.secretescapes.com|escapes.oe24.at).*'
                                              THEN 'DE'

                                          WHEN
                                                  LOWER(target.touch_hostname) REGEXP
                                                  '.*(dk.secretescapes.com|travelbird.dk|escapes.campadre.dk|dk.sales.secretescapes.com).*'
                                              THEN 'DK'

                                          WHEN
                                                  LOWER(target.touch_hostname) REGEXP
                                                  '.*(id.secretescapes.com).*'
                                              THEN 'ID'

                                          WHEN
                                                  LOWER(target.touch_hostname) REGEXP
                                                  '.*(be.secretescapes.com|fr.travelbird.be|travelbird.be|(admin.)*be.sales.secretescapes.com|hotels.shedeals.be).*'
                                              THEN 'BE'

                                          WHEN
                                                  LOWER(target.touch_hostname) REGEXP
                                                  '.*(proprietes.lefigaro.fr/location-vacances|travelbird.fr|www.evasionssecretes.fr|tresor.voyagespirates.fr|evasionssecretes.perfectstay.com|idee-evasion.lefigaro.fr).*'
                                              THEN 'FR'

                                          WHEN
                                                  LOWER(target.touch_hostname) REGEXP
                                                  '(es.secretescapes.com|se-sales-es.darkbluehq.com).*'
                                              THEN 'ES'

                                          WHEN
                                                  LOWER(target.touch_hostname) REGEXP
                                                  '.*(it.secretescapes.com|it.sales.secretescapes.com|secretescapes.perfectstay.com|se-sales-it.darkbluehq.com|escapist-it.secretescapes.cloud.ec|secretdeals.lol.travel).*'
                                              THEN 'IT'

                                          WHEN
                                                  LOWER(target.touch_hostname) REGEXP
                                                  '.*(travelbird.at|www.secretescapes.at).*'
                                              THEN 'AT'

                                          WHEN
                                                  LOWER(target.touch_hostname) REGEXP
                                                  '.*(travelist).*'
                                              THEN 'PL'

                                          WHEN
                                                  LOWER(target.touch_hostname) REGEXP
                                                  '.*(hk.secretescapes.com).*'
                                              THEN 'HK'

                                          WHEN
                                                  LOWER(target.touch_hostname) REGEXP
                                                  '.*(nl.secretescapes.com|travelbird.nl|travelhotelcard.com|nl.sales.secretescapes.com).*'
                                              THEN 'NL'

                                          WHEN
                                                  LOWER(target.touch_hostname) REGEXP
                                                  '.*(my.secretescapes.com).*'
                                              THEN 'MY'

                                          WHEN
                                                  LOWER(target.touch_hostname) REGEXP
                                                  '.*(ie.secretescapes.com).*'
                                              THEN 'IE'

                                          WHEN
                                                  LOWER(target.touch_hostname) REGEXP
                                                  '.*(hu.secretescapes.com).*'
                                              THEN 'HU'

                                          WHEN
                                                  LOWER(target.touch_hostname) REGEXP
                                                  '.*(cz.secretescapes.com).*'
                                              THEN 'CZ'

                                          WHEN
                                                  LOWER(target.touch_hostname) REGEXP
                                                  '.*(sg.secretescapes.com).*'
                                              THEN 'SG'

                                          WHEN
                                                  LOWER(target.touch_hostname) REGEXP
                                                  '.*(sk.secretescapes.com).*'
                                              THEN 'SK'

                                          WHEN
                                                  LOWER(target.touch_hostname) REGEXP
                                                  '.*(no.secretescapes.com|travelbird.no~www.secretescapes|www.secretescapes.no|travelbird.no).*'
                                              THEN 'NO'

                                          WHEN
                                                  LOWER(target.touch_hostname) REGEXP
                                                  '.*(campadre.se|travelbird.se|www.secretescapes.se|se.sales.secretescapes.com|travelbird.fi|escapes.campadre.com|escapes.campadre.se).*'
                                              THEN 'SE'

                                          WHEN
                                                  LOWER(target.touch_hostname) REGEXP
                                                  '.*(ch.secretescapes.com|travelbird.ch|se-sales-ch.darkbluehq.com|www.travelescapes.ch).*'
                                              THEN 'CH'

                                          WHEN
                                                  LOWER(target.touch_hostname) REGEXP
                                                  '.*(fathomaway.com|gilt.com|jetsetter.com|roomerluxury.com|roomertravel.com|shermanstravel.com|society19.com|society19travel.com|us.secretescapes.com|se-sales-us.darkbluehq.com|escapes-us.timeout.com|www.luxurylinkescapes.com|www.gilttravel.com|www.mailescapes.com).*'
                                              THEN 'US'

                                          WHEN
                                                      LOWER(target.touch_hostname) REGEXP
                                                      '(.*(web|db-loadtesting|sandbox).*\\.secretescapes.com|.*.fs-staging.escapes.tech|[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3})'
                                                  OR
                                                      LOWER(target.touch_hostname) IN (
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
                                                                                       'tracker.secretescapes.com',
                                                                                       '2bf9.secretescapes.com'
                                                          )
                                              THEN 'SE TECH'

                                          WHEN target.touch_hostname IS NULL AND
                                               target.touch_experience IN ('native app', 'not specified')
                                              THEN target.touch_posa_territory -- Native app does not have a hostname so we are lifting the territory from 'app id' in atomic events. Note to explore the robustness of this method.
                                          WHEN touch_posa_territory IS NOT NULL
                                              THEN IFF(touch_posa_territory = 'GB', 'UK', touch_posa_territory)

                                          ELSE 'Other'
    END
;

SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes mtba
WHERE mtba.useragent = 'data_team_artificial_insemination_transactions';

SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
WHERE mtba.touch_hostname_territory = 'Other'
ORDER BY touch_hostname NULLS LAST;

SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes mtba
WHERE mtba.touch_hostname = 'admin.co.uk.sales.secretescapes.com'

------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel;
--check other territory bookings
SELECT mtba.touch_start_tstamp::DATE                            AS date,
       SUM(IFF(mtmc.touch_affiliate_territory = 'Other', 1, 0)) AS other_territory_sessions,
       SUM(IFF(mtmc.touch_affiliate_territory = 'UK', 1, 0))    AS uk_territory_sessions,
       SUM(IFF(mtmc.touch_affiliate_territory = 'DE', 1, 0))    AS de_territory_sessions,
       count(*)                                                 AS bookings,
       other_territory_sessions / count(*)                      AS ratio_other
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
         INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel mtmc
                    ON mtba.touch_id = mtmc.touch_id
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_touched_transactions mtt ON mtba.touch_id = mtt.touch_id
WHERE mtba.touch_start_tstamp >= '2020-09-01'
GROUP BY 1;

--inspect some sessions
SELECT mtba.touch_landing_page,
       mtba.touch_hostname,
       mtba.touch_posa_territory,
       mtba.touch_hostname_territory,
       mtba.useragent,
       mtba.touch_event_count,
       mtba.touch_experience,
       mtba.touch_start_tstamp,

       mtmc.touch_hostname,
       m.touch_hostname,
       mtmc.touch_landing_page,
       m.touch_landing_page,
       mtmc.touch_hostname_territory,
       m.touch_hostname_territory,
       mtmc.touch_affiliate_territory,
       m.touch_affiliate_territory
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
         INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel mtmc
                    ON mtba.touch_id = mtmc.touch_id
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_touched_transactions mtt ON mtba.touch_id = mtt.touch_id
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel m ON mtba.touch_id = m.touch_id
WHERE m.touch_affiliate_territory = 'Other';

UPDATE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel target
SET target.touch_hostname = batch.touch_hostname
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes batch
WHERE target.touch_id = batch.touch_id
  AND target.touch_hostname IS NULL;

UPDATE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel target
SET target.touch_hostname_territory = batch.touch_hostname_territory
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes batch
WHERE target.touch_id = batch.touch_id
  AND target.touch_hostname_territory = 'Other'
  AND batch.touch_landing_page IS DISTINCT FROM 'Other';

UPDATE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel target
SET target.touch_landing_page = batch.touch_landing_page
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes batch
WHERE target.touch_id = batch.touch_id
  AND target.touch_landing_page IS NULL
  AND batch.touch_landing_page IS NOT NULL;

UPDATE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel target
SET target.touch_affiliate_territory = batch.touch_hostname_territory
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes batch
WHERE target.touch_id = batch.touch_id
  AND target.touch_affiliate_territory = 'Other';
