CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.update_affiliate_territory AS (
    SELECT DISTINCT touch_hostname,
                    CASE
                        WHEN touch_hostname = 'sales.travelbird.nl' THEN 'TB-NL'
                        WHEN touch_hostname = 'sales.travelbird.be' THEN 'TB-BE_NL'
                        WHEN touch_hostname = 'sales.travelbird.de' THEN 'DE'
                        WHEN touch_hostname = 'sales.fr.travelbird.be' THEN 'TB-BE_FR'
                        WHEN touch_hostname = 'sales.travelbird.dk' THEN 'DK'
                        WHEN touch_hostname = 'nl.sales.secretescapes.com' THEN 'NL'
                        WHEN touch_hostname = 'dk.sales.secretescapes.com' THEN 'DK'
                        WHEN touch_hostname = 'be.sales.secretescapes.com' THEN 'BE'
                        WHEN touch_hostname = 'de.sales.secretescapes.com' THEN 'DE'
                        WHEN touch_hostname = 'oferty.travelist.pl' THEN 'TL'
                        WHEN touch_hostname = 'co.uk.sales.secretescapes.com' THEN 'UK'
                        WHEN touch_hostname = 'se.sales.secretescapes.com' THEN 'SE'
                        WHEN touch_hostname = 'ie.sales.secretescapes.com' THEN 'IE'
                        END AS territory
    FROM data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel
    WHERE touch_hostname
              IN ('sales.travelbird.nl',
                  'sales.travelbird.be',
                  'sales.travelbird.de',
                  'sales.fr.travelbird.be',
                  'sales.travelbird.dk',
                  'nl.sales.secretescapes.com',
                  'dk.sales.secretescapes.com',
                  'be.sales.secretescapes.com',
                  'de.sales.secretescapes.com',
                  'oferty.travelist.pl',
                  'co.uk.sales.secretescapes.com',
                  'se.sales.secretescapes.com',
                  'ie.sales.secretescapes.com'
              )
);

CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel;

UPDATE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel target
SET target.touch_affiliate_territory = batch.territory
FROM data_vault_mvp_dev_robin.single_customer_view_stg.update_affiliate_territory batch
WHERE batch.touch_hostname = target.touch_hostname;

CREATE OR REPLACE TABLE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel CLONE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel;

SELECT * FROM data_vault_mvp_dev_robin.single_customer_view_stg.update_affiliate_territory;