--output for rumi

SELECT tc.touch_mkt_channel       AS last_non_direct_channel,
       b.touch_hostname,
       tc.touch_affiliate_territory,
       tc.touch_hostname_territory,
       COUNT(DISTINCT b.touch_id) AS sessions

FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes b
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_attribution ta ON b.touch_id = ta.touch_id
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel tc
                    ON ta.attributed_touch_id = tc.touch_id AND attribution_model = 'last non direct'
WHERE b.touch_start_tstamp >= '2020-01-1'
  AND b.touch_start_tstamp <= '2020-01-31'
  AND b.touch_hostname LIKE '%secretescapes%'
GROUP BY 1, 2, 3, 4;

USE WAREHOUSE pipe_xlarge;

--UK
SELECT tc.touch_mkt_channel       AS last_non_direct_channel,
       COUNT(DISTINCT b.touch_id) AS sessions

FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes b
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_attribution ta
                    ON b.touch_id = ta.touch_id AND ta.attribution_model = 'last non direct'
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel tc
                    ON ta.attributed_touch_id = tc.touch_id
WHERE b.touch_start_tstamp >= '2020-01-1'
  AND b.touch_start_tstamp <= '2020-01-31'
  AND b.touch_hostname = 'www.secretescapes.com'
  AND tc.touch_affiliate_territory = 'UK'
GROUP BY 1;

--UK -- last non direct
SELECT tc.touch_mkt_channel       AS last_non_direct_channel,
       COUNT(DISTINCT b.touch_id) AS sessions

FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes b
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_attribution ta
                    ON b.touch_id = ta.touch_id AND ta.attribution_model = 'last non direct'
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel tc
                    ON ta.attributed_touch_id = tc.touch_id
WHERE b.touch_start_tstamp >= '2020-01-1'
  AND b.touch_start_tstamp <= '2020-01-31'
  AND b.touch_hostname = 'www.secretescapes.com'
  AND tc.touch_affiliate_territory = 'UK'
  AND b.touch_hostname_territory = 'UK'
GROUP BY 1;


--uk direct
SELECT tc.touch_mkt_channel       AS last_non_direct_channel,
       COUNT(DISTINCT b.touch_id) AS sessions

FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes b
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel tc
                    ON b.touch_id = tc.touch_id
WHERE b.touch_start_tstamp >= '2020-01-1'
  AND b.touch_start_tstamp <= '2020-01-31'
  AND b.touch_hostname = 'www.secretescapes.com'
  AND tc.touch_affiliate_territory = 'UK'
  AND b.touch_hostname_territory = 'UK'
GROUP BY 1;

--DE -- last non direct
SELECT tc.touch_mkt_channel       AS last_non_direct_channel,
       COUNT(DISTINCT b.touch_id) AS sessions

FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes b
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_attribution ta
                    ON b.touch_id = ta.touch_id AND ta.attribution_model = 'last non direct'
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel tc
                    ON ta.attributed_touch_id = tc.touch_id
WHERE b.touch_start_tstamp >= '2020-01-1'
  AND b.touch_start_tstamp <= '2020-01-31'
  AND b.touch_hostname = 'www.secretescapes.de'
  AND tc.touch_affiliate_territory = 'DE'
  AND b.touch_hostname_territory = 'DE'
GROUP BY 1;


--DE direct
SELECT tc.touch_mkt_channel       AS last_non_direct_channel,
       COUNT(DISTINCT b.touch_id) AS sessions

FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes b
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel tc
                    ON b.touch_id = tc.touch_id
WHERE b.touch_start_tstamp >= '2020-01-1'
  AND b.touch_start_tstamp <= '2020-01-31'
  AND b.touch_hostname = 'www.secretescapes.de'
  AND tc.touch_affiliate_territory = 'DE'
  AND b.touch_hostname_territory = 'DE'
GROUP BY 1;


--de channels
SELECT tc.touch_mkt_channel,
       tc.affiliate,
       count(*)
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes b
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel tc
                    ON b.touch_id = tc.touch_id
WHERE b.touch_start_tstamp >= '2020-01-1'
  AND b.touch_start_tstamp <= '2020-01-31'
  AND b.touch_hostname = 'www.secretescapes.de'
  AND tc.touch_affiliate_territory = 'DE'
  AND b.touch_hostname_territory = 'DE'
  AND tc.touch_mkt_channel IN (
                               'PPC - Brand',
                               'PPC - Non Brand CPA',
                               'PPC - Non Brand CPL',
                               'PPC - Undefined')
GROUP BY 1, 2;

--de channels
SELECT
       tc.touch_id,
       tc.touch_mkt_channel,
       tc.touch_landing_page,
       tc.touch_hostname,
       tc.touch_hostname_territory,
       tc.attributed_user_id,
       tc.utm_campaign,
       tc.utm_medium,
       tc.utm_source,
       tc.utm_term,
       tc.utm_content,
       tc.click_id,
       tc.sub_affiliate_name,
       tc.affiliate,
       tc.touch_affiliate_territory,
       tc.awadgroupid,
       tc.awcampaignid,
       tc.referrer_hostname,
       tc.referrer_medium
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes b
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel tc
                    ON b.touch_id = tc.touch_id
WHERE b.touch_start_tstamp >= '2020-01-1'
  AND b.touch_start_tstamp <= '2020-01-31'
  AND b.touch_hostname = 'www.secretescapes.de'
  AND tc.touch_affiliate_territory = 'DE'
  AND b.touch_hostname_territory = 'DE'
  AND tc.touch_mkt_channel = 'PPC - Undefined'
  AND tc.affiliate IS NULL;
