SET report_start = '2021-11-29';
SET report_end = '2021-12-05';

WITH opens AS (
    SELECT j.mapped_territory,
           CASE
               WHEN j.mapped_campaign = 'AbandonBasketV3' THEN 'AbandonBasket'
               WHEN j.mapped_campaign IN ('Daily', 'AlertV02') THEN 'AbandonBrowseDaily'
               WHEN j.mapped_campaign = 'AbandonBrowseWeeklyReview' THEN 'AbandonBrowseWeekly'
               ELSE j.mapped_campaign END AS mapped_campaign,
           cal.se_week                    AS se_week,
           COUNT(c.event_hash)            AS opens
    FROM se.data.crm_events_opens c
        JOIN se.data.crm_jobs_list j ON c.email_id = j.email_id
        JOIN se.data.se_calendar cal ON cal.date_value = c.event_date
    WHERE c.event_date BETWEEN $report_start AND $report_end
      AND j.mapped_territory IN ('UK', 'DE')
      AND j.mapped_objective = 'AME'
    GROUP BY 1, 2, 3
),
     clicks AS (
         SELECT j.mapped_territory,
                CASE
                    WHEN j.mapped_campaign = 'AbandonBasketV3' THEN 'AbandonBasket'
                    WHEN j.mapped_campaign IN ('Daily', 'AlertV02') THEN 'AbandonBrowseDaily'
                    WHEN j.mapped_campaign = 'AbandonBrowseWeeklyReview' THEN 'AbandonBrowseWeekly'
                    ELSE j.mapped_campaign END AS mapped_campaign,
                cal.se_week                    AS se_week,
                COUNT(c.event_hash)            AS clicks
         FROM se.data.crm_events_clicks c
             JOIN se.data.crm_jobs_list j ON c.send_id = j.send_id
             JOIN se.data.se_calendar cal ON cal.date_value = c.event_date
         WHERE c.event_date BETWEEN $report_start AND $report_end
           AND j.mapped_territory IN ('UK', 'DE')
           AND j.mapped_objective = 'AME'
         GROUP BY 1, 2, 3
     ),
     sends AS (
         SELECT j.mapped_territory,
                CASE
                    WHEN j.mapped_campaign = 'AbandonBasketV3' THEN 'AbandonBasket'
                    WHEN j.mapped_campaign IN ('Daily', 'AlertV02') THEN 'AbandonBrowseDaily'
                    WHEN j.mapped_campaign = 'AbandonBrowseWeeklyReview' THEN 'AbandonBrowseWeekly'
                    ELSE j.mapped_campaign END AS mapped_campaign,
                cal.se_week                    AS se_week,
                COUNT(c.event_hash)            AS sends
         FROM se.data.crm_events_sends c
             JOIN se.data.crm_jobs_list j ON c.send_id = j.send_id
             JOIN se.data.se_calendar cal ON cal.date_value = c.event_date
         WHERE c.event_date BETWEEN $report_start AND $report_end
           AND j.mapped_territory IN ('UK', 'DE')
           AND j.mapped_objective = 'AME'
         GROUP BY 1, 2, 3
     ),
     bookings AS (
         SELECT fcb.territory                     AS mapped_territory,
                CASE
                    WHEN stmc.utm_campaign IN ('abandoned-browse', 'ame-browse-daily_de', 'ame-browse-daily_uk') THEN 'AbandonBrowseDaily'
                    WHEN stmc.utm_campaign IN ('abandoned-basket', 'abandoned_basket_') THEN 'AbandonBasket'
                    WHEN stmc.utm_campaign IN ('ame_keyword_search_de', 'ame_keyword_search_uk') THEN 'KeywordSearch'
                    WHEN stmc.utm_campaign IN ('ame-browse-weekly_de', 'ame-browse-weekly_uk', 'AbandonBrowseWeeklyReview') THEN 'AbandonBrowseWeekly'
                    WHEN stmc.utm_campaign IN ('deal_improvement_uk', 'deal_improvement_de') THEN 'DealImprovement'
                    WHEN stmc.utm_campaign IN ('reactivation_cancellation_uk', 'reactivation_cancellation_de') THEN 'Cancellation'
                    WHEN stmc.utm_campaign LIKE 'welcome_0%' THEN 'WelcomeJourney'
                    WHEN stmc.utm_campaign = 'keyword-wishlist' THEN 'Wishlist'
                    ELSE stmc.utm_campaign END    AS mapped_campaign,
                sec.se_week                       AS se_week,
                COUNT(fcb.booking_id)             AS bookings,
                SUM(fcb.margin_gross_of_toms_gbp) AS margin
         FROM se.data.scv_touch_marketing_channel stmc
             JOIN se.data.scv_touch_attribution att ON att.touch_id = stmc.touch_id AND att.attribution_model = 'last non direct'
             JOIN se.data.scv_touched_transactions stt ON att.attributed_touch_id = stt.touch_id
             JOIN se.data.fact_complete_booking fcb ON fcb.booking_id = stt.booking_id AND fcb.booking_transaction_completed_date BETWEEN $report_start AND $report_end
             JOIN se.data.se_calendar sec ON sec.date_value = fcb.booking_transaction_completed_date
         WHERE fcb.territory IN ('UK', 'DE')
           AND stmc.utm_source = 'ame'
         GROUP BY 1, 2, 3
     ),
     pages AS (
         SELECT stmc.touch_affiliate_territory AS mapped_territory,
                CASE
                    WHEN stmc.utm_campaign IN ('abandoned-browse', 'ame-browse-daily_de', 'ame-browse-daily_uk') THEN 'AbandonBrowseDaily'
                    WHEN stmc.utm_campaign IN ('abandoned-basket', 'abandoned_basket_') THEN 'AbandonBasket'
                    WHEN stmc.utm_campaign IN ('ame_keyword_search_de', 'ame_keyword_search_uk') THEN 'KeywordSearch'
                    WHEN stmc.utm_campaign IN ('ame-browse-weekly_de', 'ame-browse-weekly_uk', 'AbandonBrowseWeeklyReview') THEN 'AbandonBrowseWeekly'
                    WHEN stmc.utm_campaign IN ('deal_improvement_uk', 'deal_improvement_de') THEN 'DealImprovement'
                    WHEN stmc.utm_campaign IN ('reactivation_cancellation_uk', 'reactivation_cancellation_de') THEN 'Cancellation'
                    WHEN stmc.utm_campaign LIKE 'welcome_0%' THEN 'WelcomeJourney'
                    WHEN stmc.utm_campaign = 'keyword-wishlist' THEN 'Wishlist'
                    ELSE stmc.utm_campaign END AS mapped_campaign,
                sec.se_week                    AS se_week,
                COUNT(stt.event_hash)          AS spvs
         FROM se.data.scv_touch_marketing_channel stmc
             JOIN se.data.scv_touch_attribution att ON att.touch_id = stmc.touch_id AND att.attribution_model = 'last non direct'
             JOIN se.data.scv_touched_spvs stt ON att.attributed_touch_id = stt.touch_id AND stt.event_tstamp BETWEEN $report_start AND $report_end
             JOIN se.data.se_calendar sec ON sec.date_value = stt.event_tstamp::DATE
         WHERE stmc.touch_affiliate_territory IN ('UK', 'DE')
           AND stmc.utm_source = 'ame'
         GROUP BY 1, 2, 3
     )
SELECT o.mapped_territory,
       o.mapped_campaign,
       o.se_week,
       s.sends,
       o.opens,
       c.clicks,
       p.spvs,
       b.bookings,
       b.margin
FROM opens o
    LEFT JOIN clicks c ON o.mapped_territory = c.mapped_territory AND o.mapped_campaign = c.mapped_campaign AND o.se_week = c.se_week
    LEFT JOIN sends s ON o.mapped_territory = s.mapped_territory AND o.mapped_campaign = s.mapped_campaign AND o.se_week = s.se_week
    LEFT JOIN pages p ON o.mapped_territory = p.mapped_territory AND o.mapped_campaign = p.mapped_campaign AND o.se_week = p.se_week
    LEFT JOIN bookings b ON o.mapped_territory = b.mapped_territory AND o.mapped_campaign = b.mapped_campaign AND o.se_week = b.se_week;



SELECT CASE
           WHEN j.mapped_campaign = 'AbandonBasketV3' THEN 'AbandonBasket'
           WHEN j.mapped_campaign IN ('Daily', 'AlertV02') THEN 'AbandonBrowseDaily'
           WHEN j.mapped_campaign = 'AbandonBrowseWeeklyReview' THEN 'AbandonBrowseWeekly'
           ELSE j.mapped_campaign END AS mapped_campaign,
       cal.se_week                    AS se_week,
       *
FROM se.data.crm_events_sends c
    JOIN se.data.crm_jobs_list j ON c.send_id = j.send_id
    JOIN se.data.se_calendar cal ON cal.date_value = c.event_date
WHERE c.event_date BETWEEN $report_start AND $report_end
  AND j.mapped_territory IN ('UK', 'DE')
  AND j.mapped_objective = 'AME'
  AND j.crm_platform = 'iterable';

------------------------------------------------------------------------------------------------------------------------
