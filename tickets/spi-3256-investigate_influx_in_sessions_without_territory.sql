SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes stba
    INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
WHERE stba.touch_start_tstamp >= '2022-11-01'
  AND stmc.touch_hostname_territory = 'Other';

USE WAREHOUSE pipe_xlarge;

SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_touchification mt
    INNER JOIN hygiene_vault_mvp.snowplow.event_stream es ON mt.event_hash = es.event_hash AND es.event_tstamp::DATE = '2022-11-21'
WHERE mt.touch_id IN (
                      'a30f0443a7ffe3c57cfc7c8b961d8123baefe425fb0fe863423a174fa80fec53',
                      '076a8ad80860122f97fb1e665ca759956590fedc78c1a1d4bc8f307d89f6f980',
                      'faeee3f742a8871c8d5dee9392176839d14830ed882b3e02be53e8f727d89a4d',
                      'a225606f4e33f9c33e3b55e146d37202a8bff31e3668d643df953957f683f00e',
                      '663ace0a16c75fa828af1ab6fb69bf6a508f31028709ff8463415f150463e0b3',
                      '58a0553aa4b3a2b1785333c6ae7e04736522355e9d8fc4650a4b77ffddd34ea9',
                      '263e1e1f9016e80d1f00de87fcf6e55c8268ad6c4d60996f28d70ed535537114',
                      '1ca2d8d4a7778a435ddcd510122d6ad275e1cc3588267771ba161021ef0096f5',
                      'b6b50ee402059d2bfaf3fa5733181ec9534da62df497e3d2c866583f98ca08c2',
                      '6ef556946b1423c8035eec35cc9075c4651c331ff424fbd062f75fa90c45ebae',
                      '5eafb96656c780fe989f51c2a8589296c8fc0427099c07976ad64e033bb0ce65',
                      '92f10354a5c5901ede2cee680013ac38a9977288fd147628179d5a6e6defefc1',
                      '648d6973b4019f6c1ddb71b8e32cf85ff0bac85012c260974b58042b5bad7013',
                      '2a830236c7b96e3c33f32150b066621757829a8491ef32887017b37071495ae6',
                      '5973011758245bd17cba9ba8cb424f5b7fb8c811bd6f6b37c5569b1ce0216168',
                      '0dfbf244e8db9a562f7c31a2a6be5a83f04dd34ebd7b6f0b14ca535c35fcbb64',
                      'e2f8d1ae40ee4cd323d6830acfd385ffc7bedc5372d3266b0870c1825cc65d75',
                      '96ddaf8fbbb45591b4f2189ec9c4160db48c3423777c156aa50ee21fddb5fa04',
                      'f04fc120f8e2705e94a9bc28cde2418582d0079cb75eb690d6a9fb50bdbfc375',
                      'cfd38527a1b51b08b8fe75d4fa98a0a002f67b47d3a83e4a33e6548688617742',
                      'c22987371044ffbf7f67c9746a4453e8ec9a2de49518d817df7ba511b3de4c8f',
                      '428fda795afd9b8b6dd23e7c0d77e53f90cbbaffd5fbc212090124efaded91d7',
                      '3da3051e589821dd3f38034f2e5c631a5733307d9a70b6508058df65de7eec9c',
                      'a0de249dadb01ce8920105df5f291928187b573719c16c4e0e310e313aaccd4b',
                      '36b76e703ce7854e61f8128188a83f36d692f8decb862b1d296c343d53bbc1d6',
                      'd3de4f45466b58ede841b344411ea9a2850fa45c3c0f90d1e1ba47ca001f1694',
                      'fcc1a7b3466872f8cb99e65af5aefba39b043dc245c229d6dc32bef510df0cbd',
                      '25f56e8f33c8d59b091d05e74e968fea4cf10a16fa5a32e577636a4aee94b3cb',
                      '378fc6ee8bc66d01e8fe2231c65f7ed11eb8c6dbfe67598b1d8f5daa09b57048',
                      '7f8c4d5b9581c663ffecd805b84850003b37d7cf784003ad7dc53bd429d12449',
                      '7ec7e00fabc133e228b48391e2c38e77468a7a569f8513c95f9f25117f848812',
                      'e2e6655ebc26744765535759d116838ad4a68a0479ce0dac724b352c0af3653b',
                      '096f98d1d567a74ea991856a3d5099e5211561c8ac6876b1f15c8c32eb57d0ea',
                      'd25c227b6c353f37c65aaa9719b07f4fb6a4c46373aa8ff14bc92f49b2a2d2d4',
                      '7a515f7553c3712208149113d312b2b5f43d07ba7351f5b042bbc0a75611b6ff',
                      '1389b44443970c7f44c8238861e3317d2feb354efb77ac317fa80b509c1badd4',
                      '6d91784df83eec23096f363a2863946e580baca1da6bb0fb33f233e19576f3bc',
                      '2f35ca848edc6a039223761ceab6ef3645378ce08e89b014a60f681984f2a4bd',
                      '4c76e5b63d191c3732db09cbaaac9cb9e96563f8fae43790e887f5625bb368f0',
                      '81f6edb4ededb9b12ffc6297cb2b579ff72ba3aab8bda3627debac5312c325a6',
                      '887d92da6e771a52391fb0ae61c1c8da70082e6435d239d77ce9c15df78d2978',
                      'c6b768ce2d11b156597922b375023e410d953df6c30b5cbef41cf48d492725c2',
                      '7408b9e9060c59faf9b1ed8ba72b6b90d121e3b77d311dc7f4fb23542e1a3645',
                      '82307becfd74c26c282625b513fd6e6a3591ad4ee1ebd15a910d9e8bedfe9cea',
                      '9820c8f6043e610d28bb463bb9b4c94cee6a8a8f99e7488ff23d0137d7caaf0d',
                      '33e64f197cd35c311ca19617df569c6ab4f3dc8779fba6004d90e5c7ddb6e4c2',
                      'b5a3c9de302303f12a477272162be777b859a86a15abd7c3bc0653da4413deaf',
                      '38813502d936dd6392878a4c1fc5e1c7871d9ec24dfdf97641f877dae4994d3f',
                      '4e8530dabec7038e05d0d17464da85c61b1f73322897f0136b82fa277af258d4',
                      '51c61e995250020146ec0ba6131eb37a4709c1bcf5bed8a33c167342540f6ec8',
                      '811f2aa798bf04ca8d1f311ef9506ea4c8ee59160366330b2003ae291e78b5cc'
    )
  AND mt.event_tstamp::DATE = '2022-11-21';


SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_touchification mt
    INNER JOIN hygiene_vault_mvp.snowplow.event_stream es ON mt.event_hash = es.event_hash AND es.event_tstamp::DATE = '2022-11-08'
WHERE mt.touch_id = '68e673673c622f3c27793d079d00ad2f8f10db9b38425a5e162f1c8c30d6b053'
  AND mt.event_tstamp::DATE = '2022-11-08';


SELECT
    PARSE_URL('https://www.secretescapes.com/privacy-policy?userId=78697836&timestamp=1669052170750&noPasswordSignIn=true&authHash=badd9c1299c8f7a67cd93bdfd84025d86222bc5f&utm_medium=email&utm_source=ame&utm_campaign=4111073&utm_content=deal_spotlight_UK')

------------------------------------------------------------------------------------------------------------------------


SELECT
    DATE_TRUNC(MONTH, stba.touch_start_tstamp)   AS month,
    stmc.affiliate_posa_territory,
    COUNT(DISTINCT stba.attributed_user_id_hash) AS mau,
    COUNT(DISTINCT stba.touch_id)                AS sessions
FROM se.data.scv_touch_basic_attributes stba
    INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
WHERE stba.stitched_identity_type = 'se_user_id'
  AND stba.touch_start_tstamp::date >= '2018-01-01'
GROUP BY 1, 2;

SELECT
    DATE_TRUNC(MONTH, stba.touch_start_tstamp) AS month,
    stba.touch_hostname_territory,
    COUNT(DISTINCT stba.touch_id)              AS sessions,
    COUNT(DISTINCT sts.touch_id)               AS sessions_with_search,
    sessions_with_search / sessions            AS perc
FROM se.data.scv_touch_basic_attributes stba
    LEFT JOIN se.data.scv_touched_searches sts ON stba.touch_id = sts.touch_id
WHERE stba.touch_start_tstamp >= '2022-08-01'
  AND stba.touch_hostname_territory IN ('UK', 'DE', 'IT')
GROUP BY 1, 2;

USE WAREHOUSE pipe_xlarge;

------------------------------------------------------------------------------------------------------------------------

-- browse the event stream for authorisation events
SELECT *
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.event_name = 'authorisation_event'
  AND es.event_tstamp >= CURRENT_DATE - 1;

USE WAREHOUSE pipe_xlarge;
-- user with auth event that has a page url
SELECT *
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.event_tstamp >= CURRENT_DATE - 1
  AND es.unique_browser_id = '40a1fb37-ce21-4087-b197-96c3a4d01fd7'
ORDER BY es.event_tstamp;

-- example when authorisation event sessionises correctly
SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_touchification mt
    INNER JOIN hygiene_vault_mvp.snowplow.event_stream es ON mt.event_hash = es.event_hash AND es.event_tstamp::DATE = CURRENT_DATE - 1
WHERE mt.attributed_user_id = '27877685'
  AND mt.event_tstamp::DATE = CURRENT_DATE - 1;


-- example of authorisation event without page url
SELECT *
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.event_tstamp >= CURRENT_DATE - 1
  AND es.unique_browser_id = '42c98348-2325-4548-833f-326dfa482b13'
ORDER BY es.event_tstamp;

-- example when authorisation event sessionises incorrectly
SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_touchification mt
    INNER JOIN hygiene_vault_mvp.snowplow.event_stream es ON mt.event_hash = es.event_hash AND es.event_tstamp::DATE = CURRENT_DATE - 1
WHERE mt.attributed_user_id = '22319027'
  AND mt.event_tstamp::DATE = CURRENT_DATE - 1;


/*-- check partner portal authorisation events
SELECT *
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.event_name = 'authorisation_event'
  AND es.event_tstamp >= CURRENT_DATE - 10
AND es.page_url LIKE '%partners%';*/

-- example of email auto authorisation event without page url
SELECT *
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.event_tstamp >= CURRENT_DATE - 1
  AND es.unique_browser_id = '4c72823e-b20b-4ae9-b251-f2713c9e4229'
ORDER BY es.event_tstamp;


-- example when an email auto authorisation event sessionises incorrectly
SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_touchification mt
    INNER JOIN hygiene_vault_mvp.snowplow.event_stream es ON mt.event_hash = es.event_hash AND es.event_tstamp::DATE = CURRENT_DATE - 1
WHERE mt.attributed_user_id = '19545641'
  AND mt.event_tstamp::DATE = CURRENT_DATE - 1;


SHOW TABLES IN SCHEMA data_vault_mvp.single_customer_view_stg;


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.single_customer_view_stg.module_extracted_params_20221123 CLONE data_vault_mvp.single_customer_view_stg.module_extracted_params;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.single_customer_view_stg.module_identity_associations_20221123 CLONE data_vault_mvp.single_customer_view_stg.module_identity_associations;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.single_customer_view_stg.module_identity_stitching_20221123 CLONE data_vault_mvp.single_customer_view_stg.module_identity_stitching;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.single_customer_view_stg.module_time_diff_marker_20221123 CLONE data_vault_mvp.single_customer_view_stg.module_time_diff_marker;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.single_customer_view_stg.module_touched_app_installs_20221123 CLONE data_vault_mvp.single_customer_view_stg.module_touched_app_installs;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.single_customer_view_stg.module_touched_authorisation_events_20221123 CLONE data_vault_mvp.single_customer_view_stg.module_touched_authorisation_events;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.single_customer_view_stg.module_touched_feature_flags_20221123 CLONE data_vault_mvp.single_customer_view_stg.module_touched_feature_flags;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.single_customer_view_stg.module_touched_searches_20221123 CLONE data_vault_mvp.single_customer_view_stg.module_touched_searches;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.single_customer_view_stg.module_touched_spvs_20221123 CLONE data_vault_mvp.single_customer_view_stg.module_touched_spvs;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.single_customer_view_stg.module_touched_transactions_20221123 CLONE data_vault_mvp.single_customer_view_stg.module_touched_transactions;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.single_customer_view_stg.module_touchifiable_events_20221123 CLONE data_vault_mvp.single_customer_view_stg.module_touchifiable_events;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.single_customer_view_stg.module_touchification_20221123 CLONE data_vault_mvp.single_customer_view_stg.module_touchification;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.single_customer_view_stg.module_touch_attribution_20221123 CLONE data_vault_mvp.single_customer_view_stg.module_touch_attribution;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes_20221123 CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes_anomalous_tracker_20221123 CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes_anomalous_tracker;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel_20221123 CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.single_customer_view_stg.module_touch_utm_referrer_20221123 CLONE data_vault_mvp.single_customer_view_stg.module_touch_utm_referrer;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.single_customer_view_stg.module_unique_urls_20221123 CLONE data_vault_mvp.single_customer_view_stg.module_unique_urls;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.single_customer_view_stg.module_url_hostname_20221123 CLONE data_vault_mvp.single_customer_view_stg.module_url_hostname;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.single_customer_view_stg.module_url_params_20221123 CLONE data_vault_mvp.single_customer_view_stg.module_url_params;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.single_customer_view_stg.module_utm_referrer_marker_20221123 CLONE data_vault_mvp.single_customer_view_stg.module_utm_referrer_marker;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.single_customer_view_stg.page_screen_enrichment_20221123 CLONE data_vault_mvp.single_customer_view_stg.page_screen_enrichment;

DROP TABLE data_vault_mvp.single_customer_view_stg.module_extracted_params;
DROP TABLE data_vault_mvp.single_customer_view_stg.module_identity_associations;
DROP TABLE data_vault_mvp.single_customer_view_stg.module_identity_stitching;
DROP TABLE data_vault_mvp.single_customer_view_stg.module_time_diff_marker;
DROP TABLE data_vault_mvp.single_customer_view_stg.module_touched_app_installs;
DROP TABLE data_vault_mvp.single_customer_view_stg.module_touched_authorisation_events;
DROP TABLE data_vault_mvp.single_customer_view_stg.module_touched_feature_flags;
DROP TABLE data_vault_mvp.single_customer_view_stg.module_touched_searches;
DROP TABLE data_vault_mvp.single_customer_view_stg.module_touched_spvs;
DROP TABLE data_vault_mvp.single_customer_view_stg.module_touched_transactions;
DROP TABLE data_vault_mvp.single_customer_view_stg.module_touchifiable_events;
DROP TABLE data_vault_mvp.single_customer_view_stg.module_touchification;
DROP TABLE data_vault_mvp.single_customer_view_stg.module_touch_attribution;
DROP TABLE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;
DROP TABLE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes_anomalous_tracker;
DROP TABLE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel;
DROP TABLE data_vault_mvp.single_customer_view_stg.module_touch_utm_referrer;
DROP TABLE data_vault_mvp.single_customer_view_stg.module_unique_urls;
DROP TABLE data_vault_mvp.single_customer_view_stg.module_url_hostname;
DROP TABLE data_vault_mvp.single_customer_view_stg.module_url_params;
DROP TABLE data_vault_mvp.single_customer_view_stg.module_utm_referrer_marker;
DROP TABLE data_vault_mvp.single_customer_view_stg.page_screen_enrichment;



SELECT
    bse.affiliate,
    stmc.touch_landing_page,
    stmc.landing_page_parameters,
    fcb.transaction_id,
    stt.event_tstamp,
    bse.company,
    bse.destinationname,
    bse.salename,
    fcb.margin_gross_of_toms_cc
FROM data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel_20221123 stmc
    INNER JOIN data_vault_mvp.single_customer_view_stg.module_touched_transactions_20221123 stt ON stmc.touch_id = stt.touch_id
    INNER JOIN se.data.fact_booking fcb ON stt.booking_id = fcb.booking_id
    INNER JOIN se.data.se_booking_summary_extended bse ON fcb.booking_id = bse.booking_id
WHERE fcb.transaction_id IN
      ('A48980-25702-11504071',
       'A50522-26114-11500726',
       'A30381-19778-11496033',
       'A30381-19778-11499090',
       'A30381-19778-11499257',
       'A30381-19778-11497041',
       'A28239-19111-11504155');