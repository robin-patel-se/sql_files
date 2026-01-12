USE WAREHOUSE pipe_xlarge;
SELECT stmc.touch_hostname,
       stmc.referrer_medium,
       COUNT(*)
FROM se.data.scv_touch_marketing_channel stmc
         INNER JOIN se.data.scv_touch_basic_attributes stba ON stmc.touch_id = stba.touch_id
WHERE stmc.touch_mkt_channel = 'Other'
  AND stba.touch_start_tstamp >= '2021-01-01'
  AND stba.touch_hostname IN (
                              'escapes.timeout.com',
                              'www.lateluxury.com',
                              'www.hand-picked.telegraph.co.uk',
                              'www.guardianescapes.com',
                              'escapes.radiotimes.com',
                              'www.eveningstandardescapes.com',
                              'www.independentescapes.com',
                              'www.confidentialescapes.co.uk',
                              'escapes.planetradiooffers.co.uk',
                              'escapes.campadre.com',
                              'luxusreiseclub.urlaubsplus.de',
                              'escapes.travelbook.de'
    )
GROUP BY 1,2