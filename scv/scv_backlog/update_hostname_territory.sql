USE WAREHOUSE pipe_xlarge;

SELECT touch_hostname,
       touch_hostname_territory,
       count(*)
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
WHERE touch_hostname_territory = 'Other'
GROUP BY 1, 2;

CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;

UPDATE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes target
SET target.sale = LEFT(target.touch_hostname_territory, 15),
    target.sale = LEFT(target.touch_hostname_territory, 15),
    target.sale = LEFT(target.touch_hostname_territory, 15)
WHERE target.id = batch.id
 AND target.touch_hostname_territory = 'Other'
  AND target.touch_hostname = 'sg.secretescapes.com';

UPDATE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes target
SET target.touch_hostname_territory = 'MY'
WHERE target.touch_hostname_territory = 'Other'
  AND target.touch_hostname = 'my.secretescapes.com';

UPDATE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes target
SET target.touch_hostname_territory = 'DK'
WHERE target.touch_hostname_territory = 'Other'
  AND touch_hostname = 'dk.sales.secretescapes.com'
;

UPDATE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes target
SET target.touch_hostname_territory = 'BE'
WHERE target.touch_hostname_territory = 'Other'
  AND touch_hostname = 'be.sales.secretescapes.com'
;

UPDATE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes target
SET target.touch_hostname_territory = 'CH'
WHERE target.touch_hostname_territory = 'Other'
  AND touch_hostname = 'www.travelescapes.ch'
;

UPDATE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes target
SET target.touch_hostname_territory = 'UK'
WHERE target.touch_hostname_territory = 'Other'
  AND LOWER(touch_hostname) REGEXP
      '(blue.secretescapes.com|(www.)*confidentialescapes.co.uk|escapes.instyle.co.uk|escapes.planetradiooffers.co.uk|escapes.radiotimes.com|escapes.timeout.com|escapes.vikingfm-offers.co.uk|escapes.wave105deals.co.uk|(www.)*eveningstandardescapes.com|(www.)*guardianescapes.com|(www.)*hand-picked.telegraph.co.uk|hellomagazine.com/travel|holidays.pigsback.com|icelollyescapes.com|independent.co.uk/travelholidays|independent.secretescapes.com|(www.)*independentescapes.com|(www.)*lateluxury.com|mailescapes.co.uk|mycityvenueescapes.com|planetconfidential.co.uk|planetradiooffers.co.uk|secretsales.secretescapes.com|standard.co.uk/lifestyletravel|talktalkescapes.com|teletext.secretescapes.com|travel.radiotimes.com|trips.5pm.co.uk|(www.)*secretescapes.com|fathomaway.com|gilt.com|jetsetter.com|roomerluxury.com|roomertravel.com|shermanstravel.com|society19.com|society19travel.com|co.uk.sales.secretescapes.com|travel.discountvouchers.co.uk|secretescapes.holidaypirates.com|(www.)*luxurylinkescapes.com|se-sales-asia.darkbluehq.com|travelbird.com|sales.travelbird.com|escapes.oe24.at|secretescapes.com|www.gilttravel.com|www.mailescapes.com|www.secretescapes.group|www.secretescapescf.com|magazine.secretescapes.com|mp.secretescapes.com|se-sales-asia.darkbluehq.com).*'
;

CREATE OR REPLACE TABLE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes CLONE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes;


SELECT touch_hostname,
       touch_hostname_territory,
       count(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
WHERE touch_hostname_territory = 'Other'
GROUP BY 1, 2
ORDER BY 3 DESCxq
;

CREATE OR REPLACE TABLE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes clone data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes;

SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel
WHERE touch_affiliate_territory LIKE 'TB_%';

SELECT DISTINCT domain
FROM data_vault_mvp.cms_mysql_snapshots.affiliate_snapshot a
         INNER JOIN data_vault_mvp.cms_mysql_snapshots.territory_snapshot t ON a.territory_id = t.id
WHERE t.name LIKE 'TB-%';

SELECT DISTINCT url_string, t.name
FROM data_vault_mvp.cms_mysql_snapshots.affiliate_snapshot a
         INNER JOIN data_vault_mvp.cms_mysql_snapshots.territory_snapshot t ON a.territory_id = t.id
WHERE a.url_string like '%travelbird%';