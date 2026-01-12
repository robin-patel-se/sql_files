SET report_start = '2021-12-27';
SET report_end = '2022-01-02';

WITH opens AS (
    SELECT sua.current_affiliate_territory AS mapped_territory,
           CASE
               WHEN j.email_name = 'AME_Abandon_Basket' THEN 'AbandonBasket'
               WHEN j.email_name = 'AME_Abandon_Browse_Daily' THEN 'AbandonBrowseDaily'
               WHEN j.email_name = 'AME_Abandon_Browse_Weekly' THEN 'AbandonBrowseWeekly'
               WHEN j.email_name = 'AME_Welcome_01_Sign_Up' THEN 'WelcomeSignUp'
               ELSE j.email_name END       AS mapped_campaign,
           cal.se_week                     AS se_week,
           COUNT(c.event_hash)             AS opens
    FROM se.data.crm_events_opens c
        INNER JOIN se.data.crm_jobs_list j ON c.email_id = j.email_id
        INNER JOIN se.data.se_calendar cal ON cal.date_value = c.event_date
        INNER JOIN se.data.se_user_attributes sua ON c.shiro_user_id = sua.shiro_user_id
    WHERE c.event_date BETWEEN $report_start AND $report_end
      AND j.email_name IN ('AME_Welcome_01_Sign_Up',
                           'AME_Abandon_Basket',
                           'AME_Abandon_Browse_Daily',
                           'AME_Abandon_Browse_Weekly'
        )
      AND sua.current_affiliate_territory IN ('UK', 'DE')
    GROUP BY 1, 2, 3
),
     clicks AS (
         SELECT sua.current_affiliate_territory AS mapped_territory,
                CASE
                    WHEN j.email_name = 'AME_Abandon_Basket' THEN 'AbandonBasket'
                    WHEN j.email_name = 'AME_Abandon_Browse_Daily' THEN 'AbandonBrowseDaily'
                    WHEN j.email_name = 'AME_Abandon_Browse_Weekly' THEN 'AbandonBrowseWeekly'
                    WHEN j.email_name = 'AME_Welcome_01_Sign_Up' THEN 'WelcomeSignUp'
                    ELSE j.email_name END       AS mapped_campaign,
                cal.se_week                     AS se_week,
                COUNT(c.event_hash)             AS clicks
         FROM se.data.crm_events_clicks c
             INNER JOIN se.data.crm_jobs_list j ON c.email_id = j.email_id
             INNER JOIN se.data.se_calendar cal ON cal.date_value = c.event_date
             INNER JOIN se.data.se_user_attributes sua ON c.shiro_user_id = sua.shiro_user_id
         WHERE c.event_date BETWEEN $report_start AND $report_end
           AND j.email_name IN ('AME_Welcome_01_Sign_Up',
                                'AME_Abandon_Basket',
                                'AME_Abandon_Browse_Daily',
                                'AME_Abandon_Browse_Weekly'
             )
           AND sua.current_affiliate_territory IN ('UK', 'DE')
         GROUP BY 1, 2, 3
     ),
     sends AS (
         SELECT sua.current_affiliate_territory AS mapped_territory,
                CASE
                    WHEN j.email_name = 'AME_Abandon_Basket' THEN 'AbandonBasket'
                    WHEN j.email_name = 'AME_Abandon_Browse_Daily' THEN 'AbandonBrowseDaily'
                    WHEN j.email_name = 'AME_Abandon_Browse_Weekly' THEN 'AbandonBrowseWeekly'
                    WHEN j.email_name = 'AME_Welcome_01_Sign_Up' THEN 'WelcomeSignUp'
                    ELSE j.email_name END       AS mapped_campaign,
                cal.se_week                     AS se_week,
                COUNT(c.event_hash)             AS sends
         FROM se.data.crm_events_sends c
             INNER JOIN se.data.crm_jobs_list j ON c.email_id = j.email_id
             INNER JOIN se.data.se_calendar cal ON cal.date_value = c.event_date
             INNER JOIN se.data.se_user_attributes sua ON c.shiro_user_id = sua.shiro_user_id
         WHERE c.event_date BETWEEN $report_start AND $report_end
           AND j.email_name IN ('AME_Welcome_01_Sign_Up',
                                'AME_Abandon_Basket',
                                'AME_Abandon_Browse_Daily',
                                'AME_Abandon_Browse_Weekly'
             )
           AND sua.current_affiliate_territory IN ('UK', 'DE')
         GROUP BY 1, 2, 3
     ),
     bookings AS (
         SELECT fcb.territory                         AS mapped_territory,
                CASE
                    WHEN LOWER(stmc.utm_campaign) LIKE 'ame-browse-daily_%' THEN 'AbandonBrowseDaily'
                    WHEN LOWER(stmc.utm_campaign) LIKE 'abandoned-basket%' THEN 'AbandonBasket'
                    WHEN LOWER(stmc.utm_campaign) IN ('ame_keyword_search', 'keyword-search') THEN 'KeywordSearch'
                    WHEN LOWER(stmc.utm_campaign) LIKE 'ame-browse-weekly_%' THEN 'AbandonBrowseWeekly'
                    WHEN LOWER(stmc.utm_campaign) LIKE 'deal_improvement_%' THEN 'DealImprovement'
                    WHEN LOWER(stmc.utm_campaign) LIKE 'reactivation_cancellation_%' THEN 'Cancellation'
                    WHEN LOWER(stmc.utm_campaign) LIKE 'welcome_0%' THEN 'WelcomeJourney'
                    WHEN LOWER(stmc.utm_campaign) LIKE 'ame_wishlist' THEN 'Wishlist'
                    WHEN LOWER(stmc.utm_campaign) LIKE 'search-date' THEN 'DateMatch'
                    WHEN LOWER(stmc.utm_campaign) LIKE 'welcome_01_signup_%' THEN 'WelcomeSignUp'
                    ELSE LOWER(stmc.utm_campaign) END AS mapped_campaign,
                sec.se_week                           AS se_week,
                COUNT(fcb.booking_id)                 AS bookings,
                SUM(fcb.margin_gross_of_toms_gbp)     AS margin
         FROM se.data.scv_touch_marketing_channel stmc
             INNER JOIN se.data.scv_touch_attribution att ON att.touch_id = stmc.touch_id AND att.attribution_model = 'last non direct'
             INNER JOIN se.data.scv_touched_transactions stt ON att.attributed_touch_id = stt.touch_id
             INNER JOIN se.data.fact_complete_booking fcb ON fcb.booking_id = stt.booking_id AND fcb.booking_transaction_completed_date BETWEEN $report_start AND $report_end
             INNER JOIN se.data.se_calendar sec ON sec.date_value = fcb.booking_transaction_completed_date
         WHERE fcb.territory IN ('UK', 'DE')
           AND stmc.utm_source = 'ame'
         GROUP BY 1, 2, 3
     ),
     pages AS (
         SELECT stmc.touch_affiliate_territory        AS mapped_territory,
                CASE
                    WHEN LOWER(stmc.utm_campaign) LIKE 'ame-browse-daily_%' THEN 'AbandonBrowseDaily'
                    WHEN LOWER(stmc.utm_campaign) LIKE 'abandoned-basket%' THEN 'AbandonBasket'
                    WHEN LOWER(stmc.utm_campaign) IN ('ame_keyword_search', 'keyword-search') THEN 'KeywordSearch'
                    WHEN LOWER(stmc.utm_campaign) LIKE 'ame-browse-weekly_%' THEN 'AbandonBrowseWeekly'
                    WHEN LOWER(stmc.utm_campaign) LIKE 'deal_improvement_%' THEN 'DealImprovement'
                    WHEN LOWER(stmc.utm_campaign) LIKE 'reactivation_cancellation_%' THEN 'Cancellation'
                    WHEN LOWER(stmc.utm_campaign) LIKE 'welcome_0%' THEN 'WelcomeJourney'
                    WHEN LOWER(stmc.utm_campaign) LIKE 'ame_wishlist' THEN 'Wishlist'
                    WHEN LOWER(stmc.utm_campaign) LIKE 'search-date' THEN 'DateMatch'
                    ELSE LOWER(stmc.utm_campaign) END AS mapped_campaign,
                sec.se_week                           AS se_week,
                COUNT(stt.event_hash)                 AS spvs
         FROM se.data.scv_touch_marketing_channel stmc
             INNER JOIN se.data.scv_touch_attribution att ON att.touch_id = stmc.touch_id AND att.attribution_model = 'last non direct'
             INNER JOIN se.data.scv_touched_spvs stt ON att.attributed_touch_id = stt.touch_id AND stt.event_tstamp BETWEEN $report_start AND $report_end
             INNER JOIN se.data.se_calendar sec ON sec.date_value = stt.event_tstamp::DATE
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
    LEFT JOIN bookings b ON o.mapped_territory = b.mapped_territory AND o.mapped_campaign = b.mapped_campaign AND o.se_week = b.se_week
;



SELECT DISTINCT LOWER(stmc.utm_campaign)
FROM se.data.scv_touch_marketing_channel stmc
    INNER JOIN se.data.scv_touch_basic_attributes stba ON stmc.touch_id = stba.touch_id
WHERE stmc.touch_mkt_channel LIKE 'Email%'
  AND stba.touch_start_tstamp >= CURRENT_DATE - 3
  AND stmc.touch_affiliate_territory IN ('UK', 'DE')
  AND stmc.utm_source = 'ame';

