SELECT eo.event_tstamp::DATE      AS impression_date,
       eo.event_hash,
       eo.send_id,
       eo.subscriber_key::VARCHAR AS subscriber_key
FROM hygiene_snapshot_vault_mvp.sfmc.events_opens_plus_inferred eo
WHERE eo.event_date >= '2020-10-06' --hard date when athena went live
  AND eo.subscriber_key = '58304093'
ORDER BY impression_date DESC;

USE WAREHOUSE pipe_xlarge;

SELECT *
FROM se.data.athena_email_reporting aer
WHERE aer.impressions IS NULL
  AND aer.clicks IS NOT NULL; -- 59933
SELECT COUNT(*)
FROM se.data.athena_email_reporting aer; --49434985

SELECT *
FROM se.data.athena_email_reporting aer
WHERE aer.se_sale_id = 'A10858';

SELECT *
FROM data_vault_mvp.dwh.athena_email_reporting aer
WHERE aer.se_sale_id = 'A10858';

SELECT COUNT(DISTINCT aer.send_id)
FROM data_vault_mvp.dwh.athena_email_reporting aer
WHERE aer.se_sale_id = 'A10858';
--this sale was included in 104 email sends
-- (
--     1216601
--     1216614
--     1216689
--     1216421
--     1231291
--     1220566
--     1227940
--     1207083
--     1228013
--     1216406
--     1228876
--     1231294
--     1220570
--     1220568
--     1210929
--     1219335
--     1212757
--     1214975
--     1219934
--     1219319
--     1206056
--     1220456
--     1226761
--     1220585
--     1225413
--     1225499
--     1207505
--     1213549
--     1220659
--     1226357
--     1210931
--     1216606
--     1227936
--     1214977
--     1225308
--     1226667
--     1219418
--     1237066
--     1216677
--     1209138
--     1226286
--     1220384
--     1237061
--     1213762
--     1216454
--     1216607
--     1225826
--     1220574
--     1219369
--     1214400
--     1229818
--     1225411
--     1216407
--     1216403
--     1225305
--     1219399
--     1213558
--     1219322
--     1220573
--     1207898
--     1220571
--     1225502
--     1225222
--     1220381
--     1210936
--     1220583
--     1228744
--     1219320
--     1216495
--     1216482
--     1231193
--     1231195
--     1237160
--     1213551
--     1231194
--     1209137
--     1215567
--     1209136
--     1229815
--     1214983
--     1226764
--     1220567
--     1237067
--     1206050
--     1226359
--     1207891
--     1220569
--     1202407
--     1213756
--     1216648
--     1212195
--     1229731
--     1228842
--     1216600
--     1228016
--     1209127
--     1212158
--     1237163
--     1211354
--     1227925
--     1227934
--     1212196
--     1212161
--     1220387
-- )


SELECT DISTINCT aer.data_source_name
FROM data_vault_mvp.dwh.athena_email_reporting aer
WHERE aer.se_sale_id = 'A10858';
--36 different data sources:
-- SEGMENT_CORE_UK_ACT_01M_ATHENA_PoC_C
-- SEGMENT_TOUT_UK_ACT_01M
-- SEGMENT_CORE_UK_ACT_01M_ATHENA_PoC_A_10
-- SEGMENT_LLUX_UK_ACT_WKLY
-- SEGMENT_TOUT_UK_ACT_06M
-- SEGMENT_CORE_UK_ACT_01M_ATHENA_PoC_D_hold
-- SEGMENT_CORE_UK_ACT_01M_ATHENA_PoC_A
-- SEGMENT_CORE_UK_ACT_01M_ATHENA_PoC_B_10
-- SEGMENT_LLUX_UK_ACT_03M
-- SEGMENT_CORE_UK_ACT_01M_ATHENA_NO_RECS_5
-- SEGMENT_CORE_UK_ACT_01M_ATHENA_PoC_B
-- SEGMENT_LLUX_UK_ACT_06M
-- SEGMENT_CORE_UK_ACT_01M_ATHENA_NO_RECS_1
-- SEGMENT_CORE_UK_ACT_01M_ATHENA_NO_RECS_3
-- SEGMENT_LLUX_UK_ACT_01M
-- SEGMENT_CORE_UK_ACT_01M_ATHENA_PoC_B_2
-- SEGMENT_CORE_UK_ACT_01M_ATHENA_PoC_A_1
-- SEGMENT_TELE_UK_ACT_01M
-- SEGMENT_CORE_UK_ACT_WKLY
-- SEGMENT_TELE_UK_ACT_06M
-- SEGMENT_CORE_UK_ACT_01M
-- SEGMENT_TOUT_UK_ACT_03M
-- SEGMENT_CORE_UK_ACT_01M_ATHENA_NO_RECS_4
-- SEGMENT_CORE_UK_ACT_15M
-- SEGMENT_CORE_UK_ACT_09M
-- SEGMENT_GUAR_UK_ACT_01M
-- SEGMENT_CORE_UK_ACT_01M_ATHENA_NO_RECS_2
-- SEGMENT_CORE_UK_ACT_01M_ATHENA_PoC_B_1
-- SEGMENT_CORE_UK_ACT_03M
-- SEGMENT_TELE_UK_ACT_03M
-- SEGMENT_CORE_UK_ACT_06M
-- SEGMENT_CORE_UK_ACT_01M_ATHENA_PoC_A_2
-- SEGMENT_GUAR_UK_ACT_06M
-- SEGMENT_CORE_UK_ACT_01M_ATHENA_PoC_D
-- SEGMENT_GUAR_UK_ACT_03M
-- SEGMENT_CORE_UK_ACT_01M_ATHENA_PoC_C_hold


SELECT DISTINCT aer.send_date
FROM data_vault_mvp.dwh.athena_email_reporting aer
WHERE aer.se_sale_id = 'A10858';
--was included in emails across 31 send dates:
-- 2021-02-24
-- 2021-03-13
-- 2021-03-18
-- 2021-03-20
-- 2021-03-22
-- 2021-03-28
-- 2021-04-06
-- 2021-04-08
-- 2021-04-12
-- 2021-04-15
-- 2021-04-19
-- 2021-04-20
-- 2021-04-23
-- 2021-04-26
-- 2021-04-29
-- 2021-05-03
-- 2021-05-04
-- 2021-05-17
-- 2021-05-20
-- 2021-05-22
-- 2021-05-23
-- 2021-06-14
-- 2021-06-15
-- 2021-06-17
-- 2021-06-19
-- 2021-06-21
-- 2021-06-27
-- 2021-07-01
-- 2021-07-06
-- 2021-07-13
-- 2021-08-10



SELECT sts.event_tstamp::DATE AS date,
       stmc.channel_category,
       COUNT(*)               AS spvs
FROM se.data.scv_touched_spvs sts
    INNER JOIN se.data.scv_touch_marketing_channel stmc ON sts.touch_id = stmc.touch_id
WHERE sts.se_sale_id = 'A10858'
  AND sts.event_tstamp <= '2021-09-04'
GROUP BY 1, 2;

--look for sessions that landed on the A10858 spv from an email
SELECT *
FROM se.data.scv_touched_spvs sts
    --join to preserve spvs from sessions that started with an spv
    INNER JOIN se.data.scv_touch_marketing_channel stmc ON sts.event_hash = stmc.touch_id
WHERE sts.se_sale_id = 'A10858'
  AND sts.event_tstamp::DATE <= '2021-09-04'
  AND stmc.channel_category = 'Email - Newsletter';

--look at list of send ids that generated this traffic
SELECT DISTINCT stmc.utm_campaign
FROM se.data.scv_touched_spvs sts
    INNER JOIN se.data.scv_touch_marketing_channel stmc ON sts.event_hash = stmc.touch_id
WHERE sts.se_sale_id = 'A10858'
  AND sts.event_tstamp::DATE <= '2021-07-06'
  AND stmc.channel_category = 'Email - Newsletter';

SELECT aer.event_date::DATE,
       SUM(aer.impressions)
       FROM se.data.athena_email_reporting aer
WHERE aer.se_sale_id = 'A10858'
GROUP BY 1;