-- original query from malik
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
FROM se.data.scv_touch_marketing_channel stmc
    INNER JOIN se.data.scv_touched_transactions stt ON stmc.touch_id = stt.touch_id
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

114809-935924-55629894

USE WAREHOUSE pipe_xlarge;

-- refactored the code
SELECT
    bse.affiliate,
    stmc.touch_landing_page,
    stmc.affiliate,
    stmc.landing_page_parameters,
    fb.transaction_id,
    stt.event_tstamp,
    ssa.company_name,
    ssa.destination_name,
    ds.sale_name,
    fb.margin_gross_of_toms_cc,
    fb.margin_gross_of_toms_gbp_constant_currency,
    fb.margin_gross_of_toms_gbp
FROM se.data.fact_booking fb
    INNER JOIN se.data.se_booking_summary_extended bse ON fb.booking_id = bse.booking_id
    INNER JOIN se.data.scv_touched_transactions stt ON fb.booking_id = stt.booking_id
    INNER JOIN se.data.scv_touch_marketing_channel stmc ON stt.touch_id = stmc.touch_id
    INNER JOIN se.data.dim_sale ds ON fb.se_sale_id = ds.se_sale_id
    LEFT JOIN  se.data.se_sale_attributes ssa ON fb.se_sale_id = ssa.se_sale_id
WHERE fb.transaction_id IN
      ('A48980-25702-11504071',
       'A50522-26114-11500726',
       'A30381-19778-11496033',
       'A30381-19778-11499090',
       'A30381-19778-11499257',
       'A30381-19778-11497041',
       'A28239-19111-11504155');


-- run the query to filter on booking provided by Malik
SELECT
    bse.affiliate,
    stmc.touch_landing_page,
    stmc.affiliate,
    stmc.landing_page_parameters,
    stmc.landing_page_parameters['subid']::VARCHAR AS sub_id,
    fb.transaction_id,
    stt.event_tstamp,
    ssa.company_name,
    ssa.destination_name,
    ds.sale_name,
    fb.margin_gross_of_toms_cc,
    fb.margin_gross_of_toms_gbp_constant_currency,
    fb.margin_gross_of_toms_gbp
FROM se.data.fact_booking fb
    INNER JOIN se.data.se_booking_summary_extended bse ON fb.booking_id = bse.booking_id
    INNER JOIN se.data.scv_touched_transactions stt ON fb.booking_id = stt.booking_id
    INNER JOIN se.data.scv_touch_marketing_channel stmc ON stt.touch_id = stmc.touch_id
    INNER JOIN se.data.dim_sale ds ON fb.se_sale_id = ds.se_sale_id
    LEFT JOIN  se.data.se_sale_attributes ssa ON fb.se_sale_id = ssa.se_sale_id
WHERE fb.transaction_id = '114809-935924-55629894';

-- look at the events joint to the session
SELECT *
FROM se.data.scv_touched_transactions stt
    INNER JOIN se.data_pii.scv_session_events_link ssel ON stt.touch_id = ssel.touch_id
WHERE stt.booking_id = '55629894';

--look at the session summarisation
SELECT *
FROM se.data_pii.scv_touch_basic_attributes stba
WHERE stba.touch_start_tstamp::DATE = '2022-11-20'
  AND stba.touch_id = 'c271c1bc4ae5a0d2b15e22723b7434d9f77c9fb9ff994e7e3a8737b54df406dc';

-- look at all events for this user on this date.
SELECT *
FROM se.data_pii.scv_session_events_link ssel
    INNER JOIN se.data_pii.scv_event_stream ses ON ssel.event_hash = ses.event_hash
WHERE ssel.event_tstamp::DATE = '2022-11-20'
  AND ssel.attributed_user_id = '55629894';

-- check the user id on the booking
SELECT *
FROM se.data.fact_booking fb
WHERE fb.booking_id = '55629894';

SELECT *
FROM se.data.se_booking sb
WHERE sb.booking_id = '55629894';

SELECT *,
       --logic for if is transaction
       ses.collector_tstamp >= '2020-02-28 00:00:00'
           AND ses.event_name = 'page_view'
           AND ses.v_tracker LIKE 'java-%' --SE
           AND
       ses.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR IS NOT DISTINCT FROM 'transaction complete' AS would_be_seen_as_transaction
FROM se.data_pii.scv_event_stream ses
    LEFT JOIN se.data_pii.scv_session_events_link ssel ON ses.event_hash = ssel.event_hash
WHERE ssel.attributed_user_id IN ('joanna3@hotmail.de', '55629894')
  AND ses.event_tstamp::DATE = '2022-11-20';

SELECT
    fcb.booking_completed_date,
    ssa.data_model,
    COUNT(*)
FROM se.data.fact_complete_booking fcb
    INNER JOIN se.data.se_sale_attributes ssa ON fcb.se_sale_id = ssa.se_sale_id
WHERE fcb.shiro_user_id IS NULL
  AND fcb.tech_platform = 'SECRET_ESCAPES'
GROUP BY 1, 2;


SELECT *
FROM se.data_pii.scv_session_events_link ssel
WHERE ssel.event_tstamp::DATE = '2022-11-20'
  AND ssel.attributed_user_id = 'joanna3@hotmail.de';


SELECT
    COUNT(*)                                                   AS total_bookings,
    SUM(IFF(mtt.event_subcategory = 'backfill_booking', 1, 0)) AS backfill_bookings,
    backfill_bookings / total_bookings
FROM data_vault_mvp.single_customer_view_stg.module_touched_transactions mtt
WHERE mtt.event_tstamp >= '2022-01-01'