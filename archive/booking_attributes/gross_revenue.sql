SELECT *
FROM hygiene_vault_mvp.cms_mongodb.booking_summary bs
WHERE bs.record__o:insurancePriceInBookingCurrency > 0;

SELECT *
FROM se.data.se_sale_attributes ssa
WHERE ssa.hotel_code = '0011r00002HSn01'

CREATE OR REPLACE TRANSIENT TABLE raw_vault_mvp_dev_robin.cms_mongodb.booking_summary CLONE raw_vault_mvp.cms_mongodb.booking_summary;
self_describing_task --include 'hygiene/cms_mongodb/booking_summary.py'  --method 'run' --start '2020-01-15 00:00:00' --end '2020-01-15 00:00:00'
self_describing_task --include 'hygiene/cms_mongodb/booking_summary.py'  --method 'run' --start '2020-12-03 00:00:00' --end '2020-12-03 00:00:00'


SELECT

    -- (lineage) metadata for the current job
    schedule_tstamp,
    run_tstamp,
    operation_id,
    CURRENT_TIMESTAMP()::TIMESTAMP                                                AS created_at,
    CURRENT_TIMESTAMP()::TIMESTAMP                                                AS updated_at,

    -- (lineage) original metadata columns from previous step
    row_dataset_name,
    row_dataset_source,
    row_loaded_at,
    row_schedule_tstamp,
    row_run_tstamp,
    row_filename,
    row_file_row_number,

    -- hygiened columns

    -- from previous step
    no_nights,
    rooms,
    adult_guests,
    child_guests,
    infant_guests,
    rate_to_gbp,
    cc_rate_to_sc,
    gbp_rate_to_sc,
    last_updated,
    date_time_booked,
    booking_date,
    check_in_timestamp,
    check_in_date,
    check_out_timestamp,
    check_out_date,
    booking_lead_time_days,
    is_new_model_booking,
    affiliate_user_id,
    shiro_user_id,
    device_platform,

    -- new in current step

    --include flight amount in gross booking value
    customer_total_price_cc,
    gross_booking_value_cc + flight_amount_cc                                     AS gross_booking_value_cc,
    vat_on_commission_cc,
    booking_fee_cc,
    booking_fee_net_rate_cc,
    payment_surcharge_cc,
    payment_surcharge_net_rate_cc,
    commission_ex_vat_cc,
    insurance_commission_cc,
    flight_amount_cc,
    flight_commission_cc,
    credits_used_cc,
    total_custom_tax_cc,
    atol_fee_cc,
    total_sell_rate_cc,
    insurance_price_cc,

    -- logic obtained from cube: https://docs.google.com/spreadsheets/d/1vYIL42tP0AbP4SKqrIZbM8FRyi1CmPCW7_aKakmIkT4/edit#gid=0
    gross_booking_value_cc
        + flight_amount_cc
        + booking_fee_cc
        + payment_surcharge_cc
        + total_custom_tax_cc
        + atol_fee_cc
        + insurance_price_cc
                                                                                  AS gross_revenue_cc,

    (
            booking_fee_net_rate_cc
            + payment_surcharge_net_rate_cc
            + commission_ex_vat_cc
            + insurance_commission_cc
        -- should also include flight_commission_cc however this is currently added to commission_ex_vat to
        -- replicate logic in cms
        -- + flight_commission_cc
        )                                                                         AS margin_gross_of_toms_cc,

    customer_total_price_cc * rate_to_gbp                                         AS customer_total_price_gbp,
    (gross_booking_value_cc * rate_to_gbp) + (flight_amount_cc * rate_to_gbp)     AS gross_booking_value_gbp,
    vat_on_commission_cc * rate_to_gbp                                            AS vat_on_commission_gbp,
    booking_fee_cc * rate_to_gbp                                                  AS booking_fee_gbp,
    booking_fee_net_rate_cc * rate_to_gbp                                         AS booking_fee_net_rate_gbp,
    payment_surcharge_cc * rate_to_gbp                                            AS payment_surcharge_gbp,
    payment_surcharge_net_rate_cc * rate_to_gbp                                   AS payment_surcharge_net_rate_gbp,
    commission_ex_vat_cc * rate_to_gbp                                            AS commission_ex_vat_gbp,
    insurance_commission_cc * rate_to_gbp                                         AS insurance_commission_gbp,
    flight_amount_cc * rate_to_gbp                                                AS flight_amount_gbp,
    flight_commission_cc * rate_to_gbp                                            AS flight_commission_gbp,
    credits_used_cc * rate_to_gbp                                                 AS credits_used_gbp,
    total_custom_tax_cc * rate_to_gbp                                             AS total_custom_tax_gbp,
    atol_fee_cc * rate_to_gbp                                                     AS atol_fee_gbp,
    total_sell_rate_cc * rate_to_gbp                                              AS total_sell_rate_gbp,
    insurance_price_cc * rate_to_gbp                                              AS insurance_price_gbp,

    gross_revenue_cc * rate_to_gbp                                                AS gross_revenue_gbp,
    margin_gross_of_toms_cc * rate_to_gbp                                         AS margin_gross_of_toms_gbp,

    customer_total_price_cc * cc_rate_to_sc                                       AS customer_total_price_sc,
    (gross_booking_value_cc * cc_rate_to_sc) + (flight_amount_cc * cc_rate_to_sc) AS gross_booking_value_sc,
    vat_on_commission_cc * cc_rate_to_sc                                          AS vat_on_commission_sc,
    booking_fee_cc * cc_rate_to_sc                                                AS booking_fee_sc, ,
    booking_fee_net_rate_cc * cc_rate_to_sc                                       AS booking_fee_net_rate_sc,
    payment_surcharge_cc * cc_rate_to_sc                                          AS payment_surcharge_sc,
    payment_surcharge_net_rate_cc * cc_rate_to_sc                                 AS payment_surcharge_net_rate_sc,
    commission_ex_vat_cc * cc_rate_to_sc                                          AS commission_ex_vat_sc,
    insurance_commission_cc * cc_rate_to_sc                                       AS insurance_commission_sc,
    flight_amount_cc * cc_rate_to_sc                                              AS flight_amount_sc,
    flight_commission_cc * cc_rate_to_sc                                          AS flight_commission_sc,
    credits_used_cc * cc_rate_to_sc                                               AS credits_used_sc,
    total_custom_tax_cc * cc_rate_to_sc                                           AS total_custom_tax_sc,
    atol_fee_cc * cc_rate_to_sc                                                   AS atol_fee_sc,
    total_sell_rate_cc * cc_rate_to_sc                                            AS total_sell_rate_sc,
    insurance_price_cc * cc_rate_to_sc                                            AS insurance_price_sc,

    gross_revenue_cc * cc_rate_to_sc                                              AS gross_revenue_sc,
    margin_gross_of_toms_cc * cc_rate_to_sc                                       AS margin_gross_of_toms_sc,

    -- extracted columns from json

    booking_id,
    customer_id,
    currency,
    sale_base_currency,
    territory,
    last_updated_v1,
    last_updated_v2,
    date_time_booked_v1,
    date_time_booked_v2,
    check_in_date_v1,
    check_in_date_v2,
    check_out_date_v1,
    check_out_date_v2,
    booking_type,
    no_nights__o,
    rooms__o,
    adult_guests__o,
    child_guests__o,
    infant_guests__o,
    vat_on_commission_cc_100,
    customer_total_price_cc_100,
    gross_booking_value_cc_100,
    gross_booking_value_cc                                                        AS gross_booking_value_cc__o,
    gross_booking_value_cc * rate_to_gbp                                          AS gross_booking_value_gbp__o,
    gross_booking_value_cc * cc_rate_to_sc                                        AS gross_booking_value_sc__o,
    commission_ex_vat_cc_100,
    commission_ex_vat_sc_100,
    booking_fee_net_rate_cc_100,
    payment_surcharge_net_rate_cc_100,
    insurance_commission_cc_100,
    flight_amount_cc_100,
    flight_commission_cc_100,
    rate_to_gbp_100000,
    customer_email,
    sale_product,
    sale_type,
    booking_status,
    affiliate,
    affiliate_domain,
    booking_class,
    affiliate_id,
    sale_id,
    offer_id,
    offer_name,
    transaction_id,
    bundle_id,
    unique_transaction_reference,
    has_flights,
    supplier,
    platform_name__o,
    credits_used_cc_100,
    insurance_provider,

    -- original columns that don't require any hygiene

    record__o

FROM hygiene_vault_mvp_dev_robin.cms_mongodb.booking_summary__step03__apply_hygiene

DROP TABLE hygiene_vault_mvp_dev_robin.cms_mongodb.booking_summary;
SELECT MIN(loaded_at)
FROM raw_vault_mvp_dev_robin.cms_mongodb.booking_summary bs;

SELECT *
FROM hygiene_vault_mvp_dev_robin.cms_mongodb.booking_summary bs;

SELECT *
FROM hygiene_vault_mvp_dev_robin.cms_mongodb.booking_summary bs
WHERE bs.gross_revenue_cc != bs.customer_total_price_cc
   OR bs.gross_revenue_cc != bs.gross_booking_value_cc;

SELECT bs.transaction_id,
       bs.booking_id,
       bs.date_time_booked,
       bs.gross_booking_value_cc,
       bs.gross_booking_value_gbp,
       bs.gross_revenue_cc,
       bs.gross_revenue_gbp
FROM hygiene_vault_mvp_dev_robin.cms_mongodb.booking_summary bs
WHERE bs.transaction_id IN ('A20637-15960-2774530', '112759-915028-54700866', 'A11007-12328-2831157')
    QUALIFY row_number() OVER (PARTITION BY bs.transaction_id ORDER BY bs.updated_at DESC) = 1;

DROP TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mongodb.booking_summary;

self_describing_task --include 'hygiene_snapshots/cms_mongodb/booking_summary.py'  --method 'run' --start '2020-12-03 00:00:00' --end '2020-12-03 00:00:00'


SELECT *
FROM hygiene_snapshot_vault_mvp_dev_robin.cms_mongodb.booking_summary
WHERE transaction_id IN (
                         '86130-772263-41611179',
                         'A14566-14187-2140712',
                         '100580-855043-49706655',
                         'A10908-12281-1353045',
                         '98098-842456-48760153',
                         'A11620-12761-1683992',
                         '88869-789658-43045242',
                         '89476-796634-43709253',
                         '106384-882492-51974971',
                         '100878-856259-49964749',
                         '85832-770025-41436160',
                         '101947-861840-50713845',
                         '108469-894910-54206736',
                         '94424-823986-46275242',
                         '102560-864604-50449020',
                         '86715-776114-41990123',
                         'A15295-14539-2245103',
                         '107739-888560-52818492',
                         'A13425-13634-1935869',
                         '110569-902412-54118386',
                         '100627-855944-49764349',
                         'A12888-13222-2154069',
                         '100535-854707-49923135',
                         '87432-780350-42492870',
                         'A11843-12871-2042262',
                         'A6963-788-835794',
                         '108125-890378-53078699',
                         '95667-829419-47227558',
                         '96526-835134-47631930',
                         '96878-836000-47789957',
                         '99271-848548-49207529',
                         '99773-851136-49336225',
                         '109045-895285-53968058',
                         '97645-839936-47993325',
                         '98374-845780-49015601',
                         '105106-878250-52010731',
                         '91628-806033-44780788',
                         '102357-863719-50637317',
                         '91459-805167-45208367',
                         '108483-892348-53189996',
                         'A11492-20-1568588',
                         '87085-778446-42364307',
                         '93717-818012-45894953',
                         '88535-787512-43433485',
                         '91889-808633-45002134',
                         '93572-821610-45914662',
                         '102704-865367-51016939',
                         'A11279-12540-2028962',
                         '93703-817961-45870519',
                         'A14546-14164-1969356',
                         'A10852-12238-2339237',
                         '90315-798415-44065381',
                         'TB-21890666',
                         '109643-898284-53890238',
                         '91236-803879-44725600',
                         '91016-802616-44802921',
                         '92143-809041-44774556',
                         '96397-833458-48065511',
                         '97510-839379-48542259',
                         '105729-879393-52091685',
                         '98888-846628-48786615',
                         '92611-811580-45237892',
                         '92146-809047-45071666',
                         '101756-860731-50113622',
                         '106804-884328-52320331',
                         '108598-893037-53767426',
                         '94757-824121-46567030',
                         '86071-773879-41381306',
                         '105058-876056-51565972',
                         '108877-894352-53795481',
                         '104763-874512-51662330',
                         'A10857-12246-2358560',
                         '94329-821825-46255612',
                         'A11873-12888-1990430',
                         '104849-878322-51663986',
                         '102043-862124-50390733',
                         '107668-888282-53752284',
                         '98223-843211-48600996',
                         '100556-854837-49869337',
                         '95924-830760-47136801',
                         '99708-850777-49649806',
                         '99463-849614-49544474',
                         '108896-894482-53492730',
                         '109992-900009-53834945',
                         '101010-856993-50375910',
                         '91643-806787-45027353',
                         '90402-798924-44089571',
                         '104938-875571-51422343',
                         '86597-778824-41849914',
                         '91132-809388-44960326',
                         '107619-888569-52357228',
                         '99146-848029-49035606',
                         '94571-822975-46370610',
                         '98877-846575-49116475',
                         '110519-902215-54335979',
                         '95845-830373-47162032',
                         '85391-767372-41887974',
                         '87222-779176-42611525',
                         'A14628-14156-2107325',
                         'A12121-12940-1622105'
    );

DROP TABLE data_vault_mvp_dev_robin.dwh.se_booking;

self_describing_task --include 'dv/dwh/transactional/se_booking.py'  --method 'run' --start '2020-12-03 00:00:00' --end '2020-12-03 00:00:00'

SELECT * FROM data_vault_mvp_dev_robin.dwh.se_booking sb;

self_describing_task --include 'se/data/dwh/se_booking.py'  --method 'run' --start '2020-12-03 00:00:00' --end '2020-12-03 00:00:00'
self_describing_task --include 'se/data/dwh/fact_booking.py'  --method 'run' --start '2020-12-03 00:00:00' --end '2020-12-03 00:00:00'
self_describing_task --include 'se/data/dwh/fact_complete_booking.py'  --method 'run' --start '2020-12-03 00:00:00' --end '2020-12-03 00:00:00';

SELECT * FROM se_dev_robin.data.se_booking sb;
SELECT * FROM se_dev_robin.data.fact_booking fb;
SELECT * FROM se_dev_robin.data.fact_complete_booking fcb;

self_describing_task --include 'hygiene/cms_mongodb/booking_summary.py'  --method 'run' --start '2020-12-03 00:00:00' --end '2020-12-03 00:00:00'

SELECT * FROM se.data.fact_complete_booking fcb;
SELECT * FROM se.data.fact_booking fb;
SELECT * FROM se.data.se_booking sb