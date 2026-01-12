CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.cms_mari_link CLONE data_vault_mvp.dwh.cms_mari_link;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.fx.rates CLONE data_vault_mvp.fx.rates;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.hotel_sale_offer CLONE hygiene_snapshot_vault_mvp.cms_mysql.hotel_sale_offer;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.offer_inclusion CLONE data_vault_mvp.dwh.offer_inclusion;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.sfsc.opportunity CLONE hygiene_snapshot_vault_mvp.sfsc.opportunity;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.rate_plan_rooms_and_rates CLONE data_vault_mvp.dwh.rate_plan_rooms_and_rates;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_offer CLONE data_vault_mvp.dwh.se_offer;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_sale CLONE data_vault_mvp.dwh.se_sale;

self_describing_task --include '/dv/dwh/fornova/se_offers_inclusions_rates.py'  --method 'run' --start '2022-01-31 00:00:00' --end '2022-01-31 00:00:00'

SELECT DISTINCT
       o.rate_plan_name,
       o.hotel_code
FROM data_vault_mvp_dev_robin.dwh.se_offers_inclusions_rates__step07__model_fornova_export o
WHERE rate_type = 'PER_NIGHT'
  AND min_length_of_stay = 99;

SELECT hotel_code,
       rate_plan_name,
       soir.rate_plan_rack_code,
       MAX(soir.min_length_of_stay)
FROM se.data.se_offers_inclusions_rates soir
WHERE min_length_of_stay = 99
GROUP BY 1, 2, 3;


SELECT al.min_length_of_stay,
       al.hotel_name,
       al.hotel_code,
       al.rate_name,
       al.rate_code,
       al.rate_id,
       al.salesforce_opportunity_id,
       al.date,
       CASE WHEN UPPER(al.rate_type) = 'PER_STAY' THEN al.min_length_of_stay ELSE NULL END AS fixed_length_of_stay,
       COALESCE(fixed_length_of_stay, al.min_length_of_stay)                               AS los,
       CASE
           WHEN UPPER(al.rate_type) = 'PER_STAY' THEN al.rate
           WHEN UPPER(al.rate_type) = 'PER_NIGHT' THEN al.rate * al.min_length_of_stay
           END                                                                             AS rate_local_calculated,
       al.rate,
       al.min_length_of_stay,
       DATEADD(DAY, -al.min_length_of_stay, al.date)                                       AS window,
       SUM(IFF(al.date >= DATEADD(DAY, -al.min_length_of_stay, al.date), al.rate, NULL))
           OVER (PARTITION BY al.rate_code, al.hotel_code ORDER BY al.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
FROM data_vault_mvp_dev_robin.dwh.se_offers_inclusions_rates__step02__rate_plan_rooms_and_rates_blow_out al
    LEFT JOIN data_vault_mvp_dev_robin.dwh.se_offers_inclusions_rates__step04__offer_inclusion_split_out_core_supplementary id
              ON id.cms_offer_id::VARCHAR = al.cms_offer_id::VARCHAR
    LEFT JOIN data_vault_mvp_dev_robin.dwh.se_offers_inclusions_rates__step05__offer_inclusion_show_all_inclusion ida
              ON ida.cms_offer_id::VARCHAR = al.cms_offer_id::VARCHAR
    LEFT JOIN data_vault_mvp_dev_robin.fx.rates fx
              ON al.currency = fx.source_currency AND fx.target_currency = 'GBP' AND fx.fx_date = CURRENT_DATE
    LEFT JOIN data_vault_mvp_dev_robin.dwh.se_offers_inclusions_rates__step06__model_deal_type dt
              ON al.salesforce_opportunity_id = dt.salesforce_opportunity_id
WHERE al.salesforce_opportunity_id = '0066900001NkYcI'
ORDER BY al.rate_code, al.date;



SELECT al.min_length_of_stay,
       al.hotel_name,
       al.hotel_code,
       al.rate_code,
       al.rate_id,
       al.salesforce_opportunity_id,
       al.date,
       al.rate,
       al.min_length_of_stay,
       rc.date
FROM data_vault_mvp_dev_robin.dwh.se_offers_inclusions_rates__step02__rate_plan_rooms_and_rates_blow_out al
    LEFT JOIN data_vault_mvp_dev_robin.dwh.se_offers_inclusions_rates__step02__rate_plan_rooms_and_rates_blow_out rc
              ON al.hotel_code = rc.hotel_code
                  AND al.rate_code = rc.rate_code
                  AND rc.date BETWEEN DATEADD(DAY, - al.min_length_of_stay + 1, al.date) AND al.date
WHERE al.rate_type = 'PER_NIGHT'
  -- TODO remove
  AND al.date = '2021-03-05'
  AND al.rate_code = 'LOFPV'
  AND al.hotel_code = '001w000001G84wd'

SELECT al.hotel_code,
       al.rate_code,
       al.date,
       SUM(rc.rate)
FROM data_vault_mvp_dev_robin.dwh.se_offers_inclusions_rates__step02__rate_plan_rooms_and_rates_blow_out al
    LEFT JOIN data_vault_mvp_dev_robin.dwh.se_offers_inclusions_rates__step02__rate_plan_rooms_and_rates_blow_out rc
              ON al.hotel_code = rc.hotel_code
                  AND al.rate_code = rc.rate_code
                  AND rc.date BETWEEN DATEADD(DAY, - al.min_length_of_stay + 1, al.date) AND al.date
WHERE al.rate_type = 'PER_NIGHT'
  -- TODO remove
  AND al.date = '2021-03-05'
  AND al.rate_code = 'LOFPV'
  AND al.hotel_code = '001w000001G84wd'
GROUP BY 1, 2, 3

WITH agg_sliding_window AS (
    SELECT al.hotel_code,
           al.rate_code,
           al.date,
           SUM(rc.rate) AS rate
    FROM data_vault_mvp_dev_robin.dwh.se_offers_inclusions_rates__step02__rate_plan_rooms_and_rates_blow_out al
        LEFT JOIN data_vault_mvp_dev_robin.dwh.se_offers_inclusions_rates__step02__rate_plan_rooms_and_rates_blow_out rc
                  ON al.hotel_code = rc.hotel_code
                      AND al.rate_code = rc.rate_code
                      AND rc.date BETWEEN DATEADD(DAY, - al.min_length_of_stay + 1, al.date) AND al.date
    WHERE al.rate_type = 'PER_NIGHT'
    GROUP BY 1, 2, 3
)

SELECT SHA2(
               CONCAT(
                       al.salesforce_opportunity_id,
                       al.rate_rack_code,
                       COALESCE(al.cms_offer_id, 0),
                       COALESCE(id.salesforce_offer_id, ''),
                       al.date
                   )
           )                                                                               AS primary_key_hash,
       al.salesforce_opportunity_id,
       al.deal_stage_rank,
       dt.deal_type                                                                        AS deal_type,
       al.hotel_id,
       al.hotel_code,
       al.hotel_name,
       id.hotel_name                                                                       AS salesforce_hotel_name,
       id.booking_com_name__c,
       'UK'                                                                                AS ota_posa, --hard coded as this is only for UK shop via fornova
       id.offer_name,
       al.room_type_name,
       al.rate_name                                                                        AS rate_plan_name,
       al.rate_rack_code                                                                   AS rate_plan_rack_code,
       al.hotel_code || ':' || al.rate_code || ':' || al.rate_rack_code                    AS hotel_rate_rack_code,
       al.total                                                                            AS no_total_rooms,
       al.available                                                                        AS no_available_rooms,
       al.min_length_of_stay,
       al.max_length_of_stay,
       al.rate_type,
       CASE WHEN UPPER(al.rate_type) = 'PER_STAY' THEN al.min_length_of_stay ELSE NULL END AS fixed_length_of_stay,
       COALESCE(fixed_length_of_stay, al.min_length_of_stay)                               AS los,
       CASE
           WHEN UPPER(al.rate_type) = 'PER_STAY' THEN al.rate
           WHEN UPPER(al.rate_type) = 'PER_NIGHT' THEN al.rate * al.min_length_of_stay
           END                                                                             AS rate_local_calculated,
       CASE
           WHEN UPPER(al.rate_type) = 'PER_STAY' THEN al.rate
           WHEN UPPER(al.rate_type) = 'PER_NIGHT' THEN asw.rate
           END                                                                             AS rate_local_calculated2,
       id.occupancy_adults,
       al.cms_offer_id,
       id.salesforce_offer_id,
       al.currency                                                                         AS currency_local,
       al.date                                                                             AS allocation_date,
       al.rate                                                                             AS rate_local,
       al.single_rate                                                                      AS single_rate_local,
       id.room_type_ota_name,
       id.board_basis,
       ida.inclusion_level_agg,
       ida.inclusion_type_agg,
       ida.currency_code_local_agg,
       ida.inclusion_value_local_agg,
       COALESCE(id.core_inclusions_value_mari, 0)                                          AS core_inclusions_mari,
       COALESCE(id.supplementary_inclusions_value_mari, 0)                                 AS supplementary_inclusions_mari,
       COALESCE(id.core_inclusions_value_mari_per_person_first_night, 0)                   AS core_per_person_first_night,
       COALESCE(id.core_inclusions_value_mari_per_person_per_stay, 0)                      AS core_per_person_per_stay,
       COALESCE(id.core_inclusions_value_mari_per_person_per_day, 0)                       AS core_per_person_per_day,
       COALESCE(id.core_inclusions_value_mari_per_room_first_night, 0)                     AS core_per_room_first_night,
       COALESCE(id.core_inclusions_value_mari_per_room_per_stay, 0)                        AS core_per_room_per_stay,
       COALESCE(id.core_inclusions_value_mari_per_room_per_day, 0)                         AS core_per_room_per_day,
       COALESCE(id.supplementary_inclusions_value_mari_per_person_first_night, 0)          AS supplementary_per_person_first_night,
       COALESCE(id.supplementary_inclusions_value_mari_per_person_per_stay, 0)             AS supplementary_per_person_per_stay,
       COALESCE(id.supplementary_inclusions_value_mari_per_person_per_day, 0)              AS supplementary_per_person_per_day,
       COALESCE(id.supplementary_inclusions_value_mari_per_room_first_night, 0)            AS supplementary_per_room_first_night,
       COALESCE(id.supplementary_inclusions_value_mari_per_room_per_stay, 0)               AS supplementary_per_room_per_stay,
       COALESCE(id.supplementary_inclusions_value_mari_per_room_per_day, 0)                AS supplementary_per_room_per_day,
       IFF(al.hotel_id IS NULL, 1, 0)                                                      AS validation_check_hotel_id,
       IFF(currency_local IS NULL, 1, 0)                                                   AS validation_check_currency_local,
       IFF(id.booking_com_name__c IS NULL, 1, 0)                                           AS validation_check_booking_com_name__c,
       IFF(ota_posa IS NULL, 1, 0)                                                         AS validation_check_ota_posa,
       IFF(los IS NULL, 1, 0)                                                              AS validation_check_los,
       IFF(rate_local_calculated IS NULL, 1, 0)                                            AS validation_check_rate_local_calculated,
       IFF(id.room_type_ota_name IS NULL, 1, 0)                                            AS validation_check_room_type_ota_name,
       IFF(allocation_date IS NULL, 1, 0)                                                  AS validation_check_allocation_date,
       IFF(deal_type IS NULL, 1, 0)                                                        AS validation_check_deal_type,
       IFF(id.board_basis IS NULL, 1, 0)                                                   AS validation_check_board_basis,

       --The fields specified in the validation map to what are the required fields by fornova
       --therefore, if any of them are missing, they will not be able to perform a successful
       --shop of booking.com. Another reason is that operations can use this dataset to
       --report on what hotels are failing validation, and fix the issue.

       IFF((validation_check_hotel_id +
            validation_check_currency_local +
            validation_check_booking_com_name__c +
            validation_check_ota_posa +
            validation_check_los +
            validation_check_rate_local_calculated +
            validation_check_room_type_ota_name +
            validation_check_allocation_date +
            validation_check_deal_type +
            validation_check_board_basis) > 0,
           'failed', 'passed')                                                             AS validation_check
FROM data_vault_mvp_dev_robin.dwh.se_offers_inclusions_rates__step02__rate_plan_rooms_and_rates_blow_out al
    LEFT JOIN data_vault_mvp_dev_robin.dwh.se_offers_inclusions_rates__step04__offer_inclusion_split_out_core_supplementary id
              ON id.cms_offer_id::VARCHAR = al.cms_offer_id::VARCHAR
    LEFT JOIN data_vault_mvp_dev_robin.dwh.se_offers_inclusions_rates__step05__offer_inclusion_show_all_inclusion ida
              ON ida.cms_offer_id::VARCHAR = al.cms_offer_id::VARCHAR
    LEFT JOIN data_vault_mvp_dev_robin.fx.rates fx
              ON al.currency = fx.source_currency AND fx.target_currency = 'GBP' AND fx.fx_date = CURRENT_DATE
    LEFT JOIN data_vault_mvp_dev_robin.dwh.se_offers_inclusions_rates__step06__model_deal_type dt
              ON al.salesforce_opportunity_id = dt.salesforce_opportunity_id
    LEFT JOIN agg_sliding_window asw ON al.rate_code = asw.rate_code AND al.hotel_code = asw.hotel_code AND al.date = asw.date;


SELECT soir.primary_key_hash,
       soir.salesforce_opportunity_id,
       soir.deal_stage_rank,
       soir.deal_type,
       soir.hotel_id,
       soir.hotel_code,
       soir.hotel_name,
       soir.salesforce_hotel_name,
       soir.booking_com_name__c,
       soir.ota_posa,
       soir.offer_name,
       soir.room_type_name,
       soir.rate_plan_name,
       soir.rate_plan_rack_code,
       soir.hotel_rate_rack_code,
       soir.no_total_rooms,
       soir.no_available_rooms,
       soir.min_length_of_stay,
       soir.max_length_of_stay,
       soir.rate_type,
       soir.fixed_length_of_stay,
       soir.los,
       soir.rate_local_calculated,
       soir.occupancy_adults,
       soir.cms_offer_id,
       soir.salesforce_offer_id,
       soir.currency_local,
       soir.allocation_date,
       soir.rate_local,
       soir.single_rate_local,
       soir.room_type_ota_name,
       soir.board_basis,
       soir.inclusion_level_agg,
       soir.inclusion_type_agg,
       soir.currency_code_local_agg,
       soir.inclusion_value_local_agg,
       soir.core_inclusions_mari,
       soir.supplementary_inclusions_mari,
       soir.core_per_person_first_night,
       soir.core_per_person_per_stay,
       soir.core_per_person_per_day,
       soir.core_per_room_first_night,
       soir.core_per_room_per_stay,
       soir.core_per_room_per_day,
       soir.supplementary_per_person_first_night,
       soir.supplementary_per_person_per_stay,
       soir.supplementary_per_person_per_day,
       soir.supplementary_per_room_first_night,
       soir.supplementary_per_room_per_stay,
       soir.supplementary_per_room_per_day,
       soir.validation_check_hotel_id,
       soir.validation_check_currency_local,
       soir.validation_check_booking_com_name__c,
       soir.validation_check_ota_posa,
       soir.validation_check_los,
       soir.validation_check_rate_local_calculated,
       soir.validation_check_room_type_ota_name,
       soir.validation_check_allocation_date,
       soir.validation_check_deal_type,
       soir.validation_check_board_basis,
       soir.validation_check
FROM data_vault_mvp_dev_robin.dwh.se_offers_inclusions_rates soir
    EXCEPT
SELECT soir.primary_key_hash,
       soir.salesforce_opportunity_id,
       soir.deal_stage_rank,
       soir.deal_type,
       soir.hotel_id,
       soir.hotel_code,
       soir.hotel_name,
       soir.salesforce_hotel_name,
       soir.booking_com_name__c,
       soir.ota_posa,
       soir.offer_name,
       soir.room_type_name,
       soir.rate_plan_name,
       soir.rate_plan_rack_code,
       soir.hotel_rate_rack_code,
       soir.no_total_rooms,
       soir.no_available_rooms,
       soir.min_length_of_stay,
       soir.max_length_of_stay,
       soir.rate_type,
       soir.fixed_length_of_stay,
       soir.los,
       soir.rate_local_calculated,
       soir.occupancy_adults,
       soir.cms_offer_id,
       soir.salesforce_offer_id,
       soir.currency_local,
       soir.allocation_date,
       soir.rate_local,
       soir.single_rate_local,
       soir.room_type_ota_name,
       soir.board_basis,
       soir.inclusion_level_agg,
       soir.inclusion_type_agg,
       soir.currency_code_local_agg,
       soir.inclusion_value_local_agg,
       soir.core_inclusions_mari,
       soir.supplementary_inclusions_mari,
       soir.core_per_person_first_night,
       soir.core_per_person_per_stay,
       soir.core_per_person_per_day,
       soir.core_per_room_first_night,
       soir.core_per_room_per_stay,
       soir.core_per_room_per_day,
       soir.supplementary_per_person_first_night,
       soir.supplementary_per_person_per_stay,
       soir.supplementary_per_person_per_day,
       soir.supplementary_per_room_first_night,
       soir.supplementary_per_room_per_stay,
       soir.supplementary_per_room_per_day,
       soir.validation_check_hotel_id,
       soir.validation_check_currency_local,
       soir.validation_check_booking_com_name__c,
       soir.validation_check_ota_posa,
       soir.validation_check_los,
       soir.validation_check_rate_local_calculated,
       soir.validation_check_room_type_ota_name,
       soir.validation_check_allocation_date,
       soir.validation_check_deal_type,
       soir.validation_check_board_basis,
       soir.validation_check
FROM data_vault_mvp.dwh.se_offers_inclusions_rates soir;

SELECT soir.hotel_code,
       soir.hotel_name,
       soir.salesforce_opportunity_id,
       soir.rate_plan_rack_code,
       soir.allocation_date,
       soir.min_length_of_stay,
       soir.currency_local,
       soir.rate_local,
       soir.rate_local_calculated
FROM data_vault_mvp_dev_robin.dwh.se_offers_inclusions_rates soir
WHERE soir.hotel_rate_rack_code = '001w000001SKPFX:EPPGS:EPPGSRACK'
ORDER BY soir.allocation_date
;



SELECT *
FROM se.data.se_offers_inclusions_rates soir


------------------------------------------------------------------------------------------------------------------------


SELECT *
FROM data_vault_mvp_dev_robin.dwh.se_offers_inclusions_rates__step07__agg_sliding_window
WHERE hotel_code = '001w000001KKhn3';

SELECT *
FROM se.data.scv_touch_basic_attributes stba;

SELECT *
FROM se.data_pii.scv_event_stream ses;


SELECT sua.email_opt_in_status,
       sua.membership_account_status,
       COUNT(*)
FROM se.data.se_user_attributes sua
GROUP BY 1, 2;

SELECT *
FROM se.data.scv_touched_searches sts
WHERE sts.event_tstamp >= CURRENT_DATE - 1
AND sts.num_results = 0;