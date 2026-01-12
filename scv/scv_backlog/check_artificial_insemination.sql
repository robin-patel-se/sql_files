SELECT DISTINCT booking_id
FROM se.data.fact_complete_booking fcb
WHERE fcb.tech_platform = 'SECRET_ESCAPES'
  AND fcb.booking_completed_date >= '2020-01-01'

EXCEPT

SELECT DISTINCT stt.booking_id
FROM se.data.scv_touched_transactions stt
WHERE stt.event_subcategory IN ('se platform transaction',
                                'backfill_booking')
  AND stt.event_tstamp >= '2020-01-01';