SET start_date = {ts '2021-06-04 00:00:00'};
SET end_date = {ts '2021-06-06 00:00:00'};
SELECT x.*
FROM (
         SELECT r.mapped_territory,
                WEEK(r.event_date) + 1     AS week,
                DAYNAME(r.event_date)      AS day,
                r.event_date,
                r.send_id,
                r.data_source_name,
                s.segment,
                l.email_name,
                l.mapped_objective,
                l.mapped_campaign,
                r.sale_type,
                r.sale_product,
                r.posu_country,
                CASE
                    WHEN r.mapped_territory = 'SE' AND r.posu_country IN ('Sweden') THEN 'Domestic'
                    WHEN r.mapped_territory = 'DK' AND r.posu_country IN ('Denmark') THEN 'Domestic'
                    ELSE 'NonDomestic' END AS destination_type,
                r.sale_position_group,
                SUM(r.impressions)         AS impressions,
                SUM(r.clicks)              AS clicks
         FROM se.data.athena_email_reporting r
                  JOIN se.data.crm_jobs_list l ON r.send_id = l.send_id AND l.sent_date = r.event_date
                  JOIN se.data.crm_email_segments s ON r.send_id = s.send_id AND r.data_source_name = s.data_source_name
         WHERE r.event_date BETWEEN $start_date AND $end_date
           AND r.mapped_territory IN ('SE', 'DK')
           AND r.data_source_name LIKE ANY ('%CORE_SE_ACT%', '%CORE_DK_ACT%')
           AND l.mapped_objective = 'CORE'
         GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
     ) x
WHERE (x.impressions != 0 OR x.clicks != 0);