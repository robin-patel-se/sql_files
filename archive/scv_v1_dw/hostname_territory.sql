USE WAREHOUSE PIPE_XLARGE;
USE SCHEMA DATA_VAULT_MVP_DEV_ROBIN.SINGLE_CUSTOMER_VIEW_STG;


SELECT *
FROM (
         SELECT c.touch_hostname,
                b.TOUCH_EXPERIENCE,
                CASE
                    WHEN
                            LOWER(c.touch_hostname) REGEXP
                            '(blue.secretescapes.com|confidentialescapes.co.uk|escapes.instyle.co.uk|escapes.planetradiooffers.co.uk|escapes.radiotimes.com|escapes.timeout.com|escapes.vikingfm-offers.co.uk|escapes.wave105deals.co.uk|eveningstandardescapes.com|guardianescapes.com|hand-picked.telegraph.co.uk|hellomagazine.com/travel|holidays.pigsback.com|icelollyescapes.com|independent.co.uk/travelholidays|independent.secretescapes.com|independentescapes.com|lateluxury.com|mailescapes.co.uk|mycityvenueescapes.com|planetconfidential.co.uk|planetradiooffers.co.uk|secretsales.secretescapes.com|standard.co.uk/lifestyletravel|talktalkescapes.com|teletext.secretescapes.com|travel.radiotimes.com|trips.5pm.co.uk|(www.)*secretescapes.com|fathomaway.com|gilt.com|jetsetter.com|roomerluxury.com|roomertravel.com|shermanstravel.com|society19.com|society19travel.com|co.uk.sales.secretescapes.com|travel.discountvouchers.co.uk|secretescapes.holidaypirates.comwww.luxurylinkescapes.com|se-sales-asia.darkbluehq.com|travelbird.com|sales.travelbird.com|escapes.oe24.at|secretdeals.lol.travel|secretescapes.com|www.gilttravel.com|www.mailescapes.com|www.secretescapes.group|www.secretescapescf.com).*'
                        THEN 'UK'


                    WHEN
                            LOWER(c.touch_hostname) REGEXP
                            '.*(cats.www.secretescapes.de.meowbify.com|escapes.fernweh-aktuell.com|escapes.travelbook.de|luxusreisecluc.urlaubsplus.de|luxusurlauc.tagesspiegel.de|secretescapes.brigitte.de|secretescapes.computerbild.de|secretescapes.de|secretescapes.urlaubsguru.de|secretescapes.urlaubspiraten.de|specials.rp-online.de|traumreisen.welt.de|travelbird.de|travelbook.com|travista.de|urlaubsguru.de|urlaubsplus.de|www.secretescapes.de|www.secretescapes.hna.de|se-sales-de.darkbluehq.com|escapist-de.secretescapes.cloud.ec|de.sales.secretescapes.com).*'
                        THEN 'DE'

                    WHEN
                            LOWER(c.touch_hostname) REGEXP
                            '.*(dk.secretescapes.com|travelbird.dk).*'
                        THEN 'DK'

                    WHEN
                            LOWER(c.touch_hostname) REGEXP
                            '.*(be.secretescapes.com|fr.travelbird.be|travelbird.be|admin.be.sales.secretescapes.com).*'
                        THEN 'BE'

                    WHEN
                            LOWER(c.touch_hostname) REGEXP
                            '.*(proprietes.lefigaro.fr/location-vacances|travelbird.fr|www.evasionssecretes.fr|tresor.voyagespirates.fr|evasionssecretes.perfectstay.com).*'
                        THEN 'FR'

                    WHEN
                            LOWER(c.touch_hostname) REGEXP
                            '(es.secretescapes.com|se-sales-es.darkbluehq.com).*'
                        THEN 'ES'

                    WHEN
                            LOWER(c.touch_hostname) REGEXP
                            '.*(it.secretescapes.com|secretescapes.perfectstay.com|se-sales-it.darkbluehq.com|escapist-it.secretescapes.cloud.ec).*'
                        THEN 'IT'

                    WHEN
                            LOWER(c.touch_hostname) REGEXP
                            '.*(travelbird.at|www.secretescapes.at).*'
                        THEN 'AT'

                    WHEN
                            LOWER(c.touch_hostname) REGEXP
                            '.*(travelist).*'
                        THEN 'PL'

                    WHEN
                            LOWER(c.touch_hostname) REGEXP
                            '.*(hk.secretescapes.com).*'
                        THEN 'HK'

                    WHEN
                            LOWER(c.touch_hostname) REGEXP
                            '.*(nl.secretescapes.com|travelbird.nl|travelhotelcard.com|nl.sales.secretescapes.com).*'
                        THEN 'NL'

                    WHEN
                            LOWER(c.touch_hostname) REGEXP
                            '.*(no.secretescapes.com|travelbird.no~www.secretescapes|www.secretescapes.no|travelbird.no).*'
                        THEN 'NO'

                    WHEN
                            LOWER(c.touch_hostname) REGEXP
                            '.*(campadre.se|travelbird.se|www.secretescapes.se|se.sales.secretescapes.com).*'
                        THEN 'SE'

                    WHEN
                            LOWER(c.touch_hostname) REGEXP
                            '.*(ch.secretescapes.com|travelbird.ch|se-sales-ch.darkbluehq.com).*'
                        THEN 'CH'

                    WHEN
                            LOWER(c.touch_hostname) REGEXP
                            '.*(fathomaway.com|gilt.com|jetsetter.com|roomerluxury.com|roomertravel.com|shermanstravel.com|society19.com|society19travel.com|us.secretescapes.com|se-sales-us.darkbluehq.com|escapes-us.timeout.com).*'
                        THEN 'US'


                    WHEN
                                LOWER(c.touch_hostname) REGEXP
                                '(.*(web|db-loadtesting|sandbox).*\.secretescapes.com|.*.fs-staging.escapes.tech|[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})'
                            OR
                                LOWER(c.touch_hostname) IN (
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

                    WHEN c.touch_hostname IS NULL AND b.touch_experience = 'native app'
                        THEN b.touch_posa_territory -- Native app does not have a hostname so we are lifting the territory from 'app id' in atomic events. Note to explore the robustness of this method.

                    ELSE 'Other'
                    END AS hostname_territory,
                CASE
                    WHEN hostname_territory = 'SE TECH'
                        THEN 'SE TECH' --override affiliate territory if hostname is a se development hostname
                    WHEN c.touch_hostname IS NULL AND b.touch_experience = 'native app'
                        THEN b.TOUCH_POSA_TERRITORY -- Native app does not have a hostname so we are lifting the territory from 'app id' in atomic events. Note to explore the robustness of this method.
                    ELSE
                        COALESCE(t.name, hostname_territory) --choose the affiliate territory if there is one, otherwise default to the hostname territory
                    END AS affiliate_territory
         FROM data_vault_mvp_dev_robin.single_customer_view_stg.MODULE_TOUCH_MARKETING_CHANNEL c
                  LEFT JOIN MODULE_TOUCH_BASIC_ATTRIBUTES b ON c.TOUCH_ID = b.TOUCH_ID
                  LEFT JOIN DATA_VAULT_MVP.CMS_MYSQL_SNAPSHOTS.AFFILIATE_SNAPSHOT a ON c.AFFILIATE = a.URL_STRING
                  LEFT JOIN DATA_VAULT_MVP.CMS_MYSQL_SNAPSHOTS.TERRITORY_SNAPSHOT t ON a.TERRITORY_ID = t.ID
     )
GROUP BY 1, 2, 3, 4;

------------------------------------------------------------------------------------------------------------------------

SELECT a.NAME,
       a.URL_STRING,
       a.TERRITORY_ID,
       t.name AS affiliate_territory

FROM DATA_VAULT_MVP.CMS_MYSQL_SNAPSHOTS.AFFILIATE_SNAPSHOT a
         LEFT JOIN DATA_VAULT_MVP.CMS_MYSQL_SNAPSHOTS.TERRITORY_SNAPSHOT t ON a.TERRITORY_ID = t.ID
;

CREATE OR REPLACE TABLE SCRATCH.ROBINPATEL.MODULE_TOUCH_BASIC_ATTRIBUTES CLONE DATA_VAULT_MVP_DEV_ROBIN.SINGLE_CUSTOMER_VIEW_STG.MODULE_TOUCH_BASIC_ATTRIBUTES;
DROP TABLE DATA_VAULT_MVP_DEV_ROBIN.SINGLE_CUSTOMER_VIEW_STG.MODULE_TOUCH_BASIC_ATTRIBUTES;
CREATE OR REPLACE TABLE SCRATCH.ROBINPATEL.MODULE_TOUCH_MARKETING_CHANNEL CLONE DATA_VAULT_MVP_DEV_ROBIN.SINGLE_CUSTOMER_VIEW_STG.MODULE_TOUCH_MARKETING_CHANNEL;
DROP TABLE DATA_VAULT_MVP_DEV_ROBIN.SINGLE_CUSTOMER_VIEW_STG.MODULE_TOUCH_MARKETING_CHANNEL;


SELECT TOUCH_START_TSTAMP::DATE, COUNT(*)
FROM DATA_VAULT_MVP_DEV_ROBIN.SINGLE_CUSTOMER_VIEW_STG.MODULE_TOUCH_BASIC_ATTRIBUTES
WHERE TOUCH_EXPERIENCE = 'native app'
GROUP BY 1;

SELECT EVENT_TSTAMP::DATE, COUNT(*)
FROM HYGIENE_VAULT_MVP_DEV_ROBIN.SNOWPLOW.EVENT_STREAM
WHERE DEVICE_PLATFORM = 'native app'
GROUP BY 1;

SELECT EVENT_TSTAMP,
       COLLECTOR_TSTAMP,
       DERIVED_TSTAMP,
       DVCE_CREATED_TSTAMP,
       DVCE_SENT_TSTAMP,
       APP_ID LIKE 'ios_app%' AND DVCE_SENT_TSTAMP <= dateadd(hour, 1, DVCE_SENT_TSTAMP)


FROM HYGIENE_VAULT_MVP_DEV_ROBIN.SNOWPLOW.EVENT_STREAM
WHERE DEVICE_PLATFORM = 'native app'
  AND EVENT_TSTAMP::DATE <= '2018-01-01';


CREATE SCHEMA DATA_VAULT_MVP_DEV_ROBIN.CMS_MYSQL_SNAPSHOTS;

CREATE OR REPLACE TABLE DATA_VAULT_MVP_DEV_ROBIN.CMS_MYSQL_SNAPSHOTS.AFFILIATE_SNAPSHOT CLONE DATA_VAULT_MVP.CMS_MYSQL_SNAPSHOTS.AFFILIATE_SNAPSHOT;
CREATE OR REPLACE TABLE DATA_VAULT_MVP_DEV_ROBIN.CMS_MYSQL_SNAPSHOTS.TERRITORY_SNAPSHOT CLONE DATA_VAULT_MVP.CMS_MYSQL_SNAPSHOTS.TERRITORY_SNAPSHOT;

SELECT URL_STRING,
       COUNT(*)
FROM DATA_VAULT_MVP_DEV_ROBIN.CMS_MYSQL_SNAPSHOTS.AFFILIATE_SNAPSHOT
GROUP BY 1
HAVING COUNT(*) > 1;

SELECT URL_STRING,
       TERRITORY_ID
FROM DATA_VAULT_MVP_DEV_ROBIN.CMS_MYSQL_SNAPSHOTS.AFFILIATE_SNAPSHOT
WHERE URL_STRING IN (
                     'google-s-travelbird-nl-cpa-generic',
                     'google-s-travelbird-benl-cpa-generic',
                     'theirperfectgift',
                     'mobile-sv',
                     'inflectodk',
                     'mobile-de',
                     'mobile-es',
                     'mfr',
                     'mobilebe',
                     'mobilecz',
                     'lev-twiago-de',
                     'outbrain-nl',
                     'urlaubspiratenat',
                     'simplaex-sg',
                     'mobilenl',
                     'mobilesk',
                     'valuedemit',
                     'mobile-ch',
                     'mobile-it',
                     'im-logout-lounge',
                     'paypal',
                     'sovendusfr',
                     'mobile-dk',
                     'mobile-us',
                     'mobile-esp',
                     'mediabroker-de',
                     'mobilehu',
                     'mobile-no',
                     'mobile-fr',
                     'loveexploring'
    )

SELECT DISTINCT URL_STRING,
                TERRITORY_ID
FROM RAW_VAULT_MVP.CMS_MYSQL.AFFILIATE
    QUALIFY ROW_NUMBER() OVER (PARTITION BY URL_STRING ORDER BY LAST_UPDATED DESC) = 1;

SELECT COUNT(*)
FROM RAW_VAULT_MVP.CMS_MYSQL.AFFILIATE;


MERGE INTO data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel_clone AS TARGET
    USING (
        SELECT t.TOUCH_ID,
               CASE
                   --no utm or referrer data

                   WHEN
                           (
                                   t.UTM_CAMPAIGN IS NULL AND
                                   t.UTM_CONTENT IS NULL AND
                                   t.UTM_TERM IS NULL AND
                                   t.UTM_MEDIUM IS NULL AND
                                   t.UTM_SOURCE IS NULL AND
                                   t.CLICK_ID IS NULL AND
                                   (
                                           t.REFERRER_MEDIUM IS NULL
                                           OR
                                           REFERRER_HOSTNAME = 'm.facebook.com' -- to handle facebook oauth logins for direct traffic
                                       )
                               )
                           OR
                           (
                                   t.REFERRER_MEDIUM = 'internal'
                                   AND
                                   t.UTM_MEDIUM IS DISTINCT FROM 'email' --to handle autocomms coming through as internal
                               )
                       THEN 'Direct'

                   --when the utm or gclid params aren't all null

                   WHEN t.UTM_MEDIUM = 'email'
                       THEN
                       CASE
                           WHEN t.UTM_SOURCE = 'newsletter' THEN 'Email - Newsletter'
                           WHEN t.UTM_SOURCE = 'ame' THEN 'Email - Triggers'
                           --might have more
                           ELSE 'Email - Other'
                           END


                   WHEN t.UTM_MEDIUM IN ('display', 'tpemail', 'native', 'gdn')
                       THEN 'Display'

                   WHEN UTM_MEDIUM = 'affiliateprogramme'
                       THEN 'Affiliate Program'

                   WHEN UTM_MEDIUM = 'SE_media' THEN 'Media'

                   WHEN UTM_MEDIUM = 'blog' THEN 'Blog'

                   WHEN UTM_SOURCE = 'youtube' THEN 'YouTube' --not in place yet but will be

                   WHEN t.UTM_MEDIUM = 'facebookads'
                       OR (t.CLICK_ID IS NOT NULL
                           AND
                           (t.AFFILIATE LIKE 'facebook-%' OR t.AFFILIATE LIKE 'fb-%')
                            )
                       THEN 'Paid Social' -- placed above PPC because fb paid social has click ids

                   WHEN t.CLICK_ID IS NOT NULL -- how to capture all ppc
                       THEN
                       --case logic provided by Rumi in Marketing:
                       --https://docs.google.com/spreadsheets/d/19RBArUlM5pn2YFcMGTZVDwmfk818WYxoBLKjjm_p1zU/edit#gid=1957868123

                       CASE
                           WHEN
                                   (
                                           LOWER(t.AFFILIATE) LIKE '%bra%'
                                           OR
                                           LOWER(t.AFFILIATE) REGEXP
                                           '.*(gooaus|goobelgian|goo-dane|goodeutsch|secret-id|goodutch|goo-norway|gooswede|gooswi|goosups|goousa|goousa-ec|goousa-fl).*'
                                       )
                                   AND LOWER(t.AFFILIATE) NOT LIKE '%yahnobra%'
                               THEN 'PPC - Brand'

                           WHEN
                                   (
                                           LOWER(t.AFFILIATE) LIKE '%cpa%'
                                           OR
                                           LOWER(t.AFFILIATE) REGEXP '.*(dsa - france|active - eagle|hpa - uk).*'
                                       )
                                   AND LOWER(t.AFFILIATE) NOT LIKE '%brand%'
                               THEN 'PPC - Non Brand CPA'

                           WHEN
                                   (
                                           LOWER(t.AFFILIATE) LIKE '%cpl%'
                                           OR
                                           LOWER(t.AFFILIATE) LIKE '%secret%'
                                           OR
                                           LOWER(t.AFFILIATE) REGEXP
                                           '.*(at-dsa|de-dsa|ppc-de2-test-variant-a-de-printfox|ppc-de2-test-variant-b-de-maponos|dsa-italy|nl-dsa|ch-dsa|ppc-uk3-test-variant-a-uk-printfox|ppc-uk3-test-variant-b-uk-maponos|usa-dsa-ec|usa-dsa|stalion-italy|yahooppcdsa|yahooppc|yahnobra-german|yahoo2ppc|yahnobra2-german|yahoo3ppc|yahnobra-dutch|yahnobra-sweden|yahnobra-denmark|yahnobra-usa|yahnobra-norway).*'
                                       )
                                   --AND LOWER(t.AFFILIATE) NOT LIKE '%cpa%'
                                   AND LOWER(t.AFFILIATE) != 'secret-bra-id'
                                   AND LOWER(t.AFFILIATE) != 'secret-id'
                               THEN 'PPC - Non Brand CPL'

                           ELSE 'PPC - Undefined'
                           END

                   WHEN t.UTM_MEDIUM = 'organic-social'
                       OR (UTM_MEDIUM = 'social' AND UTM_SOURCE LIKE 'whatsapp%') --whatsapp shares
                       OR (UTM_MEDIUM = 'social' AND UTM_SOURCE LIKE 'fbshare%') --facebook shares
                       OR (UTM_MEDIUM = 'social' AND UTM_SOURCE LIKE 'tweet%') --twitter shares
                       THEN 'Organic Social'

                   WHEN t.REFERRER_MEDIUM = 'search' THEN 'Organic Search'

                   -- no utm or glcid params (but there are referrer details)
                   WHEN
                           t.UTM_CAMPAIGN IS NULL AND
                           t.UTM_CONTENT IS NULL AND
                           t.UTM_TERM IS NULL AND
                           t.UTM_MEDIUM IS NULL AND
                           t.UTM_SOURCE IS NULL AND
                           t.CLICK_ID IS NULL
                       THEN
                       CASE
                           WHEN (
                                   (t.REFERRER_MEDIUM = 'internal')
                                   OR
                                   (
                                           t.REFERRER_MEDIUM = 'unknown' AND
                                           (
                                                   t.REFERRER_HOSTNAME LIKE '%secretescapes.%' OR
                                                   t.REFERRER_HOSTNAME LIKE 'evasionssecretes.%' OR
                                                   t.REFERRER_HOSTNAME LIKE 'travelbird.%' OR
                                                   t.REFERRER_HOSTNAME LIKE '%.travelist.%' OR
                                                   t.REFERRER_HOSTNAME LIKE '%.pigsback.%'
                                               )
                                       )
                               )
                               THEN 'Direct'

                           WHEN t.REFERRER_MEDIUM = 'unknown' AND
                                (t.REFERRER_HOSTNAME LIKE '%urlaub%' OR
                                 t.REFERRER_HOSTNAME LIKE '%butterholz%' OR
                                 t.REFERRER_HOSTNAME LIKE '%mydealz%' OR
                                 t.REFERRER_HOSTNAME LIKE '%travel-dealz%' OR
                                 t.REFERRER_HOSTNAME LIKE '%travel-dealz%' OR
                                 t.REFERRER_HOSTNAME LIKE '%discountvouchers%'
                                    ) THEN 'Partner'
                           ELSE 'Other'
                           END
                   ELSE 'Other'
                   END
                       AS TOUCH_MKT_CHANNEL,
               t.TOUCH_LANDING_PAGE,
               t.TOUCH_HOSTNAME,
               t.TOUCH_HOSTNAME_TERRITORY t.UTM_CAMPAIGN,
               t.ATTRIBUTED_USER_ID,
               t.UTM_MEDIUM,
               t.UTM_SOURCE,
               t.UTM_TERM,
               t.UTM_CONTENT,
               t.CLICK_ID,
               t.SUB_AFFILIATE_NAME,
               t.AFFILIATE,

               CASE
                   --override affiliate territory if hostname is a se development hostname
                   WHEN b.TOUCH_HOSTNAME_TERRITORY = 'SE TECH'
                       THEN 'SE TECH'
                   -- Native app does not have a hostname so we are lifting the territory from 'app id' in atomic events. Note to explore the robustness of this method.
                   WHEN t.TOUCH_HOSTNAME IS NULL AND b.TOUCH_EXPERIENCE = 'native app'
                       THEN b.TOUCH_POSA_TERRITORY
                   --choose the affiliate territory if there is one, otherwise default to the hostname territory
                   ELSE
                       COALESCE(tr.NAME, b.TOUCH_HOSTNAME_TERRITORY)
                   END AS TOUCH_AFFILIATE_TERRITORY,

               t.AWADGROUPID,
               t.AWCAMPAIGNID,
               t.REFERRER_HOSTNAME,
               t.REFERRER_MEDIUM
        FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_utm_referrer t
                 LEFT JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes b
                           ON t.TOUCH_ID = b.TOUCH_ID
                 LEFT JOIN raw_vault_mvp_dev_robin.cms_mysql.affiliate_snapshot a ON t.AFFILIATE = a.URL_STRING
                 LEFT JOIN data_vault_mvp_dev_robin.cms_mysql_snapshots.territory_snapshot tr ON a.TERRITORY_ID = tr.ID
        WHERE t.UPDATED_AT >= TIMESTAMPADD('day', -1, '2020-02-21 00:00:00'::TIMESTAMP)
    ) AS BATCH ON TARGET.TOUCH_ID = BATCH.TOUCH_ID
    WHEN NOT MATCHED
        THEN INSERT (
                     SCHEDULE_TSTAMP,
                     RUN_TSTAMP,
                     OPERATION_ID,
                     CREATED_AT,
                     UPDATED_AT,
                     TOUCH_ID,
                     TOUCH_MKT_CHANNEL,
                     TOUCH_LANDING_PAGE,
                     TOUCH_HOSTNAME,
                     TOUCH_HOSTNAME_TERRITORY,
                     ATTRIBUTED_USER_ID,
                     UTM_CAMPAIGN,
                     UTM_MEDIUM,
                     UTM_SOURCE,
                     UTM_TERM,
                     UTM_CONTENT,
                     CLICK_ID,
                     SUB_AFFILIATE_NAME,
                     AFFILIATE,
                     TOUCH_AFFILIATE_TERRITORY,
                     AWADGROUPID,
                     AWCAMPAIGNID,
                     REFERRER_HOSTNAME,
                     REFERRER_MEDIUM
        ) VALUES ('2020-02-21 00:00:00',
                  '2020-02-27 16:37:52',
                  'ScriptOperator__/usr/local/one-data-pipeline/biapp/task_catalogue/dv/dwh_rec/events/05_touch_channelling/02_module_touch_marketing_channel.py__20200221T000000__daily',
                  CURRENT_TIMESTAMP()::TIMESTAMP,
                  CURRENT_TIMESTAMP()::TIMESTAMP,
                  BATCH.TOUCH_ID,
                  BATCH.TOUCH_MKT_CHANNEL,
                  BATCH.TOUCH_LANDING_PAGE,
                  BATCH.TOUCH_HOSTNAME,
                  BATCH.TOUCH_HOSTNAME_TERRITORY,
                  BATCH.ATTRIBUTED_USER_ID,
                  BATCH.UTM_CAMPAIGN,
                  BATCH.UTM_MEDIUM,
                  BATCH.UTM_SOURCE,
                  BATCH.UTM_TERM,
                  BATCH.UTM_CONTENT,
                  BATCH.CLICK_ID,
                  BATCH.SUB_AFFILIATE_NAME,
                  BATCH.AFFILIATE,
                  BATCH.TOUCH_AFFILIATE_TERRITORY,
                  BATCH.AWADGROUPID,
                  BATCH.AWCAMPAIGNID,
                  BATCH.REFERRER_HOSTNAME,
                  BATCH.REFERRER_MEDIUM);



SELECT SALE_ID,
       CLASS,
       HAS_FLIGHTS_AVAILABLE,
       DEFAULT_PREFERRED_AIRPORT_CODE,
       TYPE,
       HOTEL_CHAIN_LINK,
       CLOSEST_AIRPORT_CODE,
       IS_ABLE_TO_SELL_FLIGHTS,
       SALE_PRODUCT,
       SALE_TYPE,
       PRODUCT_TYPE,
       PRODUCT_CONFIGURATION,
       PRODUCT_LINE,
       DATA_MODEL,
       LAST_UPDATED
FROM DATA_VAULT_MVP.DWH.SE_SALE
WHERE SALE_ID IN (
    SELECT SALE_ID
    FROM DATA_VAULT_MVP.DWH.SE_SALE
    GROUP BY 1
    HAVING COUNT(*) > 1);

SELECT *
FROM DATA_VAULT_MVP.DWH.SE_SALE__STEP_02_OLD_MODEL
WHERE SALE_ID IN (
    SELECT SALE_ID
    FROM DATA_VAULT_MVP.DWH.SE_SALE
    GROUP BY 1
    HAVING COUNT(*) > 1);


--old model sale dimension categorisation given by tech
--https://docs.google.com/drawings/d/1W-6U158kklwMJF8ytlszJln7ZfGncPAE_iUe6shiHeI/edit -- sale_product
--https://docs.google.com/drawings/d/1Mxpw_tjFVvPojRqo4oERqW9y63flsrUvwLKCLFeNm48/edit -- sale_type
SELECT s.sale_id,
       s.type,
       s.hotel_chain_link,
       s.closest_airport_code,
       c.is_able_to_sell_flights,
       s.type                                         AS sale_product, --known as `product` in cube, and `type` in cms

       CASE
           WHEN s.type IN ('PACKAGE', 'TRAVEL') THEN
               CASE
                   WHEN s.hotel_chain_link IS NOT NULL THEN 'WRD'
                   ELSE
                       CASE
                           WHEN s.closest_airport_code IS NOT NULL AND c.is_able_to_sell_flights = TRUE
                               THEN 'IHP - dynamic'
                           ELSE
                               CASE
                                   WHEN s.is_team20package = 1 THEN 'IHP - static'
                                   ELSE '3PP'
                                   END
                           END
                   END
           ELSE CASE
                    WHEN s.type = 'HOTEL' THEN
                        CASE
                            WHEN s.hotel_chain_link IS NOT NULL THEN 'WRD'
                            ELSE
                                CASE
                                    WHEN s.closest_airport_code IS NOT NULL AND c.is_able_to_sell_flights = TRUE
                                        THEN 'Hotel Plus'
                                    ELSE 'Hotel'
                                    END
                            END
                    ELSE 'N/A'
               END
           END                                        AS sale_type,    --known as sale_type in cube and sale_dimension in cms

       --new naming convention was created to handle known business reporting issues identified by key stakeholders.
       --resulting document formulated and agreed on to handle the issues:
       --https://docs.google.com/presentation/d/1tP1urQuQAzJ1UBYfx06SuaSIvmfR8AlN-kNMJSSgtlk/edit#slide=id.g70c7fa579c_0_8
       CASE
           WHEN s.type = 'HOTEL' THEN 'Hotel'
           WHEN s.type = 'DAY' THEN 'Day Experience'
           WHEN s.type IN ('PACKAGE', 'TRAVEL') THEN 'Package'
           END                                        AS product_type,


       CASE
           WHEN s.type IN ('PACKAGE', 'TRAVEL') THEN
               CASE
                   WHEN s.hotel_chain_link IS NOT NULL THEN 'WRD'
                   ELSE
                       CASE
                           WHEN s.closest_airport_code IS NOT NULL AND c.is_able_to_sell_flights = TRUE
                               THEN 'IHP - dynamic'
                           ELSE
                               CASE
                                   WHEN s.is_team20package = 1 THEN 'IHP - static'
                                   ELSE '3PP'
                                   END
                           END
                   END
           ELSE CASE
                    WHEN s.type = 'HOTEL' THEN
                        CASE
                            WHEN s.hotel_chain_link IS NOT NULL THEN 'WRD'
                            ELSE
                                CASE
                                    WHEN s.closest_airport_code IS NOT NULL AND c.is_able_to_sell_flights = TRUE
                                        THEN 'Hotel Plus'
                                    ELSE 'Hotel'
                                    END
                            END
                    ELSE 'N/A'
               END
           END                                        AS product_configuration,

       'Flash'                                        AS product_line,
       'Old Model'                                    AS data_model,

       GREATEST(COALESCE(s.updated_at, '1970-01-01'),
                COALESCE(c.updated_at, '1970-01-01')) AS last_updated

FROM hygiene_snapshot_vault_mvp.cms_mysql.sale s
         LEFT JOIN hygiene_snapshot_vault_mvp.cms_mysql.sale_flight_config c ON s.sale_id = c.sale_id
WHERE s.updated_at >=
      TIMESTAMPADD('hour', -1, '2020-03-02 11:00:00'::TIMESTAMP)
   OR c.updated_at >=
      TIMESTAMPADD('hour', -1, '2020-03-02 11:00:00'::TIMESTAMP)

CREATE OR REPLACE TABLE RAW_VAULT_MVP_DEV_ROBIN.CMS_MYSQL.SALE CLONE RAW_VAULT_MVP.CMS_MYSQL.SALE;
CREATE OR REPLACE TABLE RAW_VAULT_MVP_DEV_ROBIN.CMS_MYSQL.BASE_SALE CLONE RAW_VAULT_MVP.CMS_MYSQL.BASE_SALE;
CREATE OR REPLACE TABLE RAW_VAULT_MVP_DEV_ROBIN.CMS_MYSQL.HOTEL CLONE RAW_VAULT_MVP.CMS_MYSQL.HOTEL;
CREATE OR REPLACE TABLE RAW_VAULT_MVP_DEV_ROBIN.CMS_MYSQL.SALE_FLIGHT_CONFIG CLONE RAW_VAULT_MVP.CMS_MYSQL.SALE_FLIGHT_CONFIG;

CREATE OR REPLACE TABLE HYGIENE_VAULT_MVP_DEV_ROBIN.CMS_MYSQL.SALE_FLIGHT_CONFIG CLONE HYGIENE_VAULT_MVP.CMS_MYSQL.SALE_FLIGHT_CONFIG;

airflow backfill -s '2019-12-16 00:00:00' -e '2019-12-16 00:00:00' hygiene__cms_mysql__sale__hourly
airflow backfill -s '2019-12-16 00:00:00' -e '2019-12-16 00:00:00' incoming__cms_mysql__sale__hourly

airflow backfill -s '2019-12-02 00:00:00' -e '2019-12-02 00:00:00' hygiene__cms_mysql__sale_flight_config__hourly

airflow backfill -s '2019-12-02 00:00:00' -e '2019-12-02 00:00:00' hygiene_snapshots__cms_mysql__sale_flight_config__sale_id__hourly


self_describing_task --include 'staging/hygiene_snapshots/cms_mysql/sale_flight_config__sale_id'  --method 'run' --start '2019-12-16 00:00:00' --end '2019-12-16 00:00:00'

SELECT updated_at, count(*)
FROM HYGIENE_VAULT_MVP.CMS_MYSQL.SALE_FLIGHT_CONFIG
group by 1;

