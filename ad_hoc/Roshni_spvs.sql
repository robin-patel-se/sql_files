SET startdate = {ts '2021-06-07 00:00:00'};
SET enddate = {ts '2021-06-20 00:00:00'};

SELECT c.se_year,
       c.se_week,
       c.day_of_week,
       stmc.touch_hostname_territory,
       se.data.channel_category(stmc.touch_mkt_channel)              AS channel,
       se.data.se_sale_travel_type(s.posa_territory, s.posu_country) AS destination_type,
       COUNT(stt.event_hash)                                         AS spvs
FROM se.data.scv_touched_spvs stt
         INNER JOIN se.data.scv_touch_attribution sta
                    ON sta.touch_id = stt.touch_id
                        AND sta.attribution_model = 'last non direct'
         INNER JOIN se.data.scv_touch_marketing_channel stmc
                    ON sta.attributed_touch_id = stmc.touch_id
                        AND stmc.touch_hostname = 'dk.secretescapes.com'
         INNER JOIN se.data.se_calendar c
                    ON stt.event_tstamp::DATE = c.date_value
         INNER JOIN se.data.dim_sale s
                    ON s.se_sale_id = stt.se_sale_id
WHERE stt.event_tstamp::DATE BETWEEN $startdate AND $enddate
GROUP BY 1, 2, 3, 4, 5, 6;

SELECT DISTINCT stmc.touch_hostname
FROM se.data.scv_touch_marketing_channel stmc;


'dk.secretescapes.com',
'dk.sales.secretescapes.com',
'sales.travelbird.dk',
'travelbird.dk',
'admin.dk.sales.secretescapes.com',
'admin.sales.travelbird.dk',
'se-sales-dk.darkbluehq.com',
'www.secretescapes.dk',
