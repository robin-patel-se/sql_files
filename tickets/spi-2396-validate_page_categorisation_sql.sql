--ALTER SESSION SET week_start = 1; --set start of week to Monday

SET (from_date, to_date)= ('2022-05-01', '2022-05-31');

WITH sess_bookings AS (
    SELECT
        stt.touch_id,
        COUNT(DISTINCT fcb.booking_id)                      AS bookings,
        SUM(fcb.margin_gross_of_toms_gbp_constant_currency) AS margin
    FROM se.data.scv_touch_basic_attributes stba
        INNER JOIN se.data.scv_touched_transactions stt ON stba.touch_id = stt.touch_id
        INNER JOIN se.data.fact_complete_booking fcb ON stt.booking_id = fcb.booking_id
    WHERE stba.touch_start_tstamp >= $from_date
      AND stba.touch_start_tstamp <= $to_date
    GROUP BY 1
)
        ,
     sess_spvs AS (
         SELECT
             stba.touch_id,
             COUNT(*) AS spvs
         FROM se.data.scv_touch_basic_attributes stba
             LEFT JOIN se.data.scv_touched_spvs sts ON stba.touch_id = sts.touch_id
         WHERE stba.touch_start_tstamp >= $from_date
           AND stba.touch_start_tstamp <= $to_date
         GROUP BY 1
     ),

     url_split AS (
         SELECT
             stba.touch_id,
             SPLIT_PART(stmc.touch_landing_page, '?', 1) AS part_one,
             SPLIT_PART(stmc.touch_landing_page, '?', 2) AS part_two,
             stmc.touch_landing_page

         FROM se.data.scv_touch_basic_attributes stba
             INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id

         WHERE stba.touch_start_tstamp >= $from_date
           AND stba.touch_start_tstamp <= $to_date
           AND stmc.touch_affiliate_territory IN ('UK', 'DE')
     ),

     grouped_pages AS (
         SELECT
             DATE_TRUNC(WEEK, stba.touch_start_tstamp)                                                                  AS week_start,
             stmc.touch_mkt_channel,
             stmc.touch_affiliate_territory                                                                             AS touch_hostname_territory,

             CASE
                 WHEN (stmc.touch_landing_page IS NULL AND stba.touch_experience LIKE '%app%') THEN 'App-Landing-Page'
                 WHEN (stmc.touch_landing_page IS NULL AND stba.touch_experience NOT LIKE '%app%') THEN 'No-Landing_page'
                 WHEN usp.part_one LIKE '%filter%' THEN 'Filter_page'
                 WHEN (usp.part_one LIKE '%current-sales%' OR usp.part_one LIKE '%aktuelle-angebote' OR usp.part_one LIKE '%aktuelle-angebote%'
                     OR usp.part_one LIKE '%current-sales' OR usp.part_one LIKE '%currentSales'
                     OR usp.part_one LIKE 'https://www.secretescapes.com/' OR usp.part_one LIKE 'https://secretescapes.com/' OR usp.part_one LIKE 'https://www.secretescapes.com'
                     OR usp.part_one LIKE 'https://www.secretescapes.com/#' OR usp.part_one LIKE 'https://www.secretescapes.com/aktuelle-angebote'
                     OR usp.part_one LIKE 'https://www.secretescapes.de/' OR usp.part_one LIKE 'https://www.secretescapes.de/aktuelle-angebote'
                     OR usp.part_one LIKE 'https://www.secretescapes.de/#' OR usp.part_one LIKE 'https://www.secretescapes.de' OR usp.part_one LIKE '%offerte-in-corso' OR
                       usp.part_one LIKE '%offerte-in-corso%'
                     ) THEN 'Current_sales'

                 WHEN (usp.part_one LIKE '%instant-access%' OR usp.part_one LIKE '%/instantAccess/%') THEN 'Instant-access'
                 WHEN (usp.part_one LIKE '%search/search%' OR usp.part_one LIKE '%mbSearch/mbSearch%' OR usp.part_one LIKE '%/search') THEN 'Search'
                 WHEN (usp.part_one LIKE '%/booking%' OR usp.part_one LIKE '%/bookings' OR usp.part_one LIKE '%/buchungen' OR usp.part_one LIKE '%/buchungen%') THEN 'booking_page'
                 WHEN (usp.part_one LIKE '%/accounts/%' OR usp.part_one LIKE '%/your-account%' OR usp.part_one LIKE '%konto%' OR usp.part_one LIKE '%konto' OR usp.part_one LIKE '%konto%')
                     THEN 'Account_page'
                 WHEN (usp.part_one LIKE '%hotelSale%' OR usp.part_one LIKE '%sale-hotel%' OR usp.part_one LIKE '%hoteldetail%') THEN 'hotelSale'
                 WHEN (usp.part_one LIKE '%magazine.secretescapes%' OR usp.part_one LIKE '%/magazine-de/%') THEN 'magazine.secretescapes'
                 WHEN usp.part_one LIKE '%secretescapes.perfectstay%' THEN 'perfectstay'
                 WHEN (usp.part_one LIKE '%/sale' OR usp.part_one LIKE '%/sale%' OR usp.part_one LIKE '%/offerta%' OR usp.part_one LIKE '%/offerta'
                     OR usp.part_one LIKE '%co.uk.sales%' OR usp.part_one LIKE '%de.sales%') THEN 'sale'
                 WHEN (usp.part_one LIKE '%voucher%' OR usp.part_one LIKE '%vouchers%' OR usp.part_one LIKE '%voucher' OR usp.part_one LIKE '%geschenkgutscheine%' OR
                       usp.part_one LIKE '%geschenkgutscheine') THEN 'vouchers'
                 WHEN usp.part_one LIKE '%mp.secretescapes%' THEN 'mp.secretescapes'
                 WHEN (usp.part_one LIKE '%terms-and-conditions%' OR usp.part_one LIKE '%/agb') THEN 'terms-and-conditions'
                 WHEN usp.part_one LIKE '%jetlineholidays%' THEN 'jetlineholidays'
                 WHEN usp.part_one LIKE '%lateluxury%' THEN 'lateluxury'
                 WHEN (usp.part_one LIKE '%/contact%' OR usp.part_one LIKE '%/contact' OR usp.part_one LIKE '%/kontakt' OR usp.part_one LIKE '%/kontakt%') THEN 'contact'
                 WHEN (usp.part_one LIKE '%/faq' OR usp.part_one LIKE '%mobile-faq' OR usp.part_one LIKE '%mobile-faq%' OR usp.part_one LIKE '%faq%') THEN 'FAQ'
                 WHEN (usp.part_one LIKE '%travelbird.com%' OR usp.part_one LIKE '%travelbird.de%') THEN 'travelbird.com'
                 WHEN usp.part_one LIKE '%secretescapes.exoticca%' THEN 'exoticca'
                 WHEN usp.part_one LIKE '%your-subscriptions' THEN 'subscriptions_page'
                 WHEN (usp.part_one LIKE '%my-favourites' OR usp.part_one LIKE '%meine-favoriten' OR usp.part_one LIKE '%my-favourites%' OR usp.part_one LIKE '%meine-favoriten%') THEN 'my-favourites'
                 WHEN (usp.part_one LIKE '%privacy-policy' OR usp.part_one LIKE '%privacy-policy%') THEN 'privacy-policy'
                 WHEN (usp.part_one LIKE '%auth/login' OR usp.part_one LIKE '%auth/login%' OR usp.part_one LIKE '%/login') THEN 'Login'
                 WHEN (usp.part_one LIKE '%credits' OR usp.part_one LIKE '%credits%') THEN 'credits'
                 WHEN (usp.part_one LIKE '%/reservation' OR usp.part_one LIKE '%/reservation%') THEN 'reservation'
                 WHEN (usp.part_one LIKE '%/payment/' OR usp.part_one LIKE '%/payment/%' OR usp.part_one LIKE '%/zahlungsinformationen/%' OR usp.part_one LIKE '%/zahlungsinformationen') THEN 'payment'
                 WHEN (usp.part_one LIKE '%/ueber-uns' OR usp.part_one LIKE '%/about-us' OR usp.part_one LIKE '%/about-us%') THEN 'About_us'
                 WHEN (usp.part_one LIKE '%telegraph' OR usp.part_one LIKE '%telegraph%') THEN 'telegraph'
                 WHEN (usp.part_one LIKE '%www.hlx.com%' OR usp.part_one LIKE 'www.hlx.com%') THEN 'hlx'
                 WHEN (usp.part_one LIKE '%flightFinder' OR usp.part_one LIKE '%flightFinder') THEN 'flight_finder'
                 WHEN (usp.part_one LIKE '%forgottenpassword' OR usp.part_one LIKE '%forgottenpassword%') THEN 'forgottenpassword'
                 WHEN (usp.part_one LIKE '%freunde-einladen' OR usp.part_one LIKE '%invite_friends') THEN 'invite__friends'
                 WHEN usp.part_one LIKE '%datenschutzerklaerung' THEN 'Data_Protection'
                 WHEN (usp.part_one LIKE '%www.secretescapes.group' OR usp.part_one LIKE '%www.secretescapes.group%') THEN 'Group'
                 WHEN usp.part_one LIKE '%homeliving%' THEN 'homeliving'
                 WHEN usp.part_one LIKE '%/media' THEN 'Media'
                 WHEN (usp.part_one LIKE '%work-with-us' OR usp.part_one LIKE '%workWithUs') THEN 'workWithUs'
                 WHEN usp.part_one LIKE 'https://escapes%' THEN 'Escapes'
                 WHEN usp.part_one LIKE '%eurowings.com%' THEN 'eurowings.com'

                 WHEN RIGHT(usp.part_one, 1) = '=' THEN 'Broken_URL'

                 ELSE 'Other' END                                                                                       AS landing_page,


             COALESCE(SUM(s.spvs), 0)                                                                                   AS spvs,
             COUNT(DISTINCT CASE WHEN stba.stitched_identity_type = 'se_user_id' THEN stba.attributed_user_id_hash END) AS logged_in_users,
             COALESCE(SUM(b.bookings), 0)                                                                               AS bookings,
             COALESCE(SUM(b.margin), 0)                                                                                 AS margin

         FROM se.data.scv_touch_basic_attributes stba
             JOIN       url_split usp ON usp.touch_id = stba.touch_id
             INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
             LEFT JOIN  sess_bookings b ON stba.touch_id = b.touch_id
             LEFT JOIN  sess_spvs s ON stba.touch_id = s.touch_id

         WHERE stba.touch_start_tstamp >= $from_date
           AND stba.touch_start_tstamp <= $to_date
           AND stmc.touch_affiliate_territory IN ('UK', 'DE')

         GROUP BY 1, 2, 3, 4
     )


SELECT

--week_start,
touch_mkt_channel,
touch_hostname_territory,

CASE
    WHEN landing_page LIKE 'Search' THEN 'Search'
    WHEN (landing_page LIKE 'magazine.secretescapes'
        OR landing_page LIKE 'mp.secretescapes') THEN 'Media_and_Magazine'
    WHEN (landing_page LIKE 'Broken_URL'
        OR landing_page LIKE 'No-Landing_page'
        OR landing_page LIKE 'Other') THEN 'Other'
    WHEN (landing_page LIKE 'exoticca'
        OR landing_page LIKE 'hlx'
        OR landing_page LIKE 'homeliving'
        OR landing_page LIKE 'jetlineholidays'
        OR landing_page LIKE 'lateluxury'
        OR landing_page LIKE 'perfectstay'
        OR landing_page LIKE 'telegraph'
        OR landing_page LIKE 'eurowings.com'
        OR landing_page LIKE 'travelbird.com'
        OR landing_page LIKE 'Escapes') THEN '3rd Party'
    WHEN (landing_page LIKE 'About_us'
        OR landing_page LIKE 'Data_Protection'
        OR landing_page LIKE 'FAQ'
        OR landing_page LIKE 'Group'
        OR landing_page LIKE 'invite__friends'
        OR landing_page LIKE 'privacy-policy'
        OR landing_page LIKE 'workWithUs'
        OR landing_page LIKE 'Account_page'
        OR landing_page LIKE 'terms-and-conditions'
        OR landing_page LIKE 'contact'
        OR landing_page LIKE 'forgottenpassword'
        OR landing_page LIKE 'subscriptions_page'
        OR landing_page LIKE 'my-favourites'
        OR landing_page LIKE 'credits'
        OR landing_page LIKE 'vouchers'
        OR landing_page LIKE 'flight_finder') THEN 'Self_Service_Pages'
    WHEN (landing_page LIKE 'Current_sales'
        OR landing_page LIKE 'hotelSale'
        OR landing_page LIKE 'Instant-access'
        OR landing_page LIKE 'sale') THEN 'Sale_Pages'
    WHEN landing_page LIKE 'App-Landing-Page' THEN 'App'
    WHEN landing_page LIKE 'Login' THEN 'Login'
    WHEN (landing_page LIKE 'booking_page'
        OR landing_page LIKE 'payment'
        OR landing_page LIKE 'reservation') THEN 'Booking_Flow'
    ELSE 'Filter' END AS landing_page_group,

SUM(spvs)             AS spvs,
SUM(logged_in_users)  AS logged_in_users,
SUM(bookings)         AS bookings,
SUM(margin)           AS margin

FROM grouped_pages

GROUP BY 1, 2, 3
--,4


------------------------------------------------------------------------------------------------------------------------
SET (from_date, to_date)= ('2022-05-01', '2022-05-31');
WITH categories AS (
    SELECT
        stba.touch_id,
        stba.touch_landing_pagepath,
        stba.touch_landing_page,
        stmc.touch_mkt_channel,
        stmc.touch_affiliate_territory,
        stmc.touch_hostname,
        CASE
            WHEN (stba.touch_landing_page IS NULL AND stba.touch_experience LIKE '%app%') THEN 'app landing page'
            WHEN (stba.touch_landing_page IS NULL AND stba.touch_experience NOT LIKE '%app%') THEN 'no landing page'
            WHEN stba.touch_landing_pagepath LIKE '%filter%' THEN 'filter page'
            WHEN (stba.touch_landing_pagepath LIKE ANY ('%/current-sales%', '%currentSales', '/', '/#', '%aktuelle-angebote%', '%offerte-in-corso%')
                OR stba.touch_landing_pagepath IS NULL
                ) THEN 'current sales'
            WHEN stba.touch_landing_pagepath LIKE ANY ('%instant-access%', '%instantAccess%') THEN 'instant access'
            WHEN stba.touch_landing_pagepath LIKE ANY ('%search/search%', '%mbSearch/mbSearch%', '%/search') THEN 'search'
            WHEN stba.touch_landing_pagepath LIKE ANY ('%/booking%', '%/bookings', '%/buchungen%') THEN 'booking page'
            WHEN stba.touch_landing_pagepath LIKE ANY ('%/accounts/%', '%/your-account%', '%konto%') THEN 'account page'
            WHEN stba.touch_landing_pagepath LIKE ANY ('%hotelSale%', '%sale-hotel%', '%hoteldetail%') THEN 'hotel sale'
            WHEN (
                        stba.touch_landing_pagepath LIKE ANY ('%/sale%', '%/offerta', '%/offerta%')
                    OR stba.touch_hostname LIKE ANY ('%co.uk.sales%', '%de.sales%')
                ) THEN 'sale' --RP: is the separation of Hotel only vs everything else for sale page okay?
            WHEN (
                        stba.touch_hostname LIKE '%magazine.secretescapes%'
                    OR stba.touch_landing_pagepath LIKE '%/magazine-de/%'
                ) THEN 'magazine secretescapes'
            WHEN stba.touch_landing_pagepath LIKE ANY ('%terms-and-conditions%', '%/agb') THEN 'terms and conditions'
            WHEN stba.touch_landing_pagepath LIKE ANY ('%/contact%', '%/contact', '%/kontakt', '%/kontakt%') THEN 'contact'
            WHEN stba.touch_landing_pagepath LIKE ANY ('%/faq', '%mobile-faq', '%mobile-faq%', '%faq%') THEN 'faq'
            WHEN stba.touch_landing_pagepath LIKE ANY ('%voucher%', '%geschenkgutscheine%') THEN 'vouchers'
            WHEN stba.touch_hostname LIKE '%mp.secretescapes%' THEN 'mp.secretescapes'
            WHEN stba.touch_hostname LIKE '%secretescapes.perfectstay%' THEN 'perfect stay'
            WHEN stba.touch_hostname LIKE '%jetlineholidays%' THEN 'jetline holidays'
            WHEN stba.touch_hostname LIKE '%lateluxury%' THEN 'lateluxury'
            WHEN stba.touch_hostname LIKE ANY ('%travelbird.com%', '%travelbird.de%') THEN 'travelbird.com'
            WHEN stba.touch_hostname LIKE '%secretescapes.exoticca%' THEN 'exoticca'
            WHEN stba.touch_hostname LIKE ANY ('%www.hlx.com%', 'www.hlx.com%') THEN 'hlx'
            WHEN stba.touch_landing_pagepath LIKE '%your-subscriptions' THEN 'subscriptions page'
            WHEN stba.touch_landing_pagepath LIKE ANY ('%my-favourites', '%meine-favoriten', '%my-favourites%', '%meine-favoriten%') THEN 'my favourites'
            WHEN stba.touch_landing_pagepath LIKE ANY ('%privacy-policy', '%privacy-policy%') THEN 'privacy policy'
            WHEN stba.touch_landing_pagepath LIKE ANY ('%auth/login', '%auth/login%', '%/login') THEN 'login'
            WHEN stba.touch_landing_pagepath LIKE ANY ('%credits', '%credits%') THEN 'credits'
            WHEN stba.touch_landing_pagepath LIKE ANY ('%/reservation', '%/reservation%') THEN 'reservation'
            WHEN stba.touch_landing_pagepath LIKE ANY ('%/payment/', '%/payment/%', '%/zahlungsinformationen/%', '%/zahlungsinformationen') THEN 'payment'
            WHEN stba.touch_landing_pagepath LIKE ANY ('%/ueber-uns', '%/about-us', '%/about-us%') THEN 'about us'
            WHEN stba.touch_landing_pagepath LIKE ANY ('%telegraph', '%telegraph%') THEN 'telegraph'
            WHEN stba.touch_landing_pagepath LIKE ANY ('%flightFinder', '%flightFinder') THEN 'flight finder'
            WHEN stba.touch_landing_pagepath LIKE ANY ('%forgottenpassword', '%forgottenpassword%') THEN 'forgotten password'
            WHEN stba.touch_landing_pagepath LIKE ANY ('%freunde-einladen', '%invite_friends') THEN 'invite friends'
            WHEN stba.touch_landing_pagepath LIKE '%datenschutzerklaerung' THEN 'data protection'
            WHEN stba.touch_landing_pagepath LIKE '%homeliving%' THEN 'home living'
            WHEN stba.touch_landing_pagepath LIKE '%/media%' THEN 'media'
            WHEN stba.touch_landing_pagepath LIKE ANY ('%work-with-us%', '%workWithUs') THEN 'work with us'
            WHEN stba.touch_hostname LIKE ANY ('%www.secretescapes.group', '%www.secretescapes.group%') THEN 'group'
            WHEN stba.touch_hostname LIKE 'escapes.%' THEN 'escapes'
            WHEN stba.touch_hostname LIKE '%eurowings.com%' THEN 'eurowings.com'
            ELSE 'other'
            END AS landing_page_categorisation
    FROM se.data.scv_touch_basic_attributes stba
        INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
    WHERE stba.touch_start_tstamp >= $from_date
      AND stba.touch_start_tstamp <= $to_date
      AND stmc.touch_affiliate_territory IN ('UK', 'DE')
)
SELECT
    c.landing_page_categorisation,
    COUNT(*)
FROM categories c
GROUP BY 1



------------------------------------------------------------------------------------------------------------------------

--ALTER SESSION SET week_start = 1; --set start of week to Monday

SET (from_date, to_date)= ('2022-05-01', '2022-05-31');

WITH sess_bookings AS (
    SELECT
        stt.touch_id,
        COUNT(DISTINCT fcb.booking_id)                      AS bookings,
        SUM(fcb.margin_gross_of_toms_gbp_constant_currency) AS margin
    FROM se.data.scv_touch_basic_attributes stba
        INNER JOIN se.data.scv_touched_transactions stt ON stba.touch_id = stt.touch_id
        INNER JOIN se.data.fact_complete_booking fcb ON stt.booking_id = fcb.booking_id
    WHERE stba.touch_start_tstamp >= $from_date
      AND stba.touch_start_tstamp <= $to_date
    GROUP BY 1
)
        ,
     sess_spvs AS (
         SELECT
             stba.touch_id,
             COUNT(*) AS spvs
         FROM se.data.scv_touch_basic_attributes stba
             LEFT JOIN se.data.scv_touched_spvs sts ON stba.touch_id = sts.touch_id
         WHERE stba.touch_start_tstamp >= $from_date
           AND stba.touch_start_tstamp <= $to_date
         GROUP BY 1
     ),
     landing_page_categorisation AS (
         SELECT
             stba.touch_id,
             stba.touch_landing_page,
             stba.touch_landing_pagepath,
             stmc.touch_hostname,
             stmc.touch_affiliate_territory,
             stba.touch_start_tstamp,
             stba.touch_end_tstamp,
             stmc.touch_mkt_channel,
             stba.stitched_identity_type,
             stba.attributed_user_id_hash,
             CASE
                 WHEN (stba.touch_landing_page IS NULL AND stba.touch_experience LIKE '%app%') THEN 'app landing page'
                 WHEN (stba.touch_landing_page IS NULL AND stba.touch_experience NOT LIKE '%app%') THEN 'no landing page'
                 WHEN stba.touch_landing_pagepath LIKE '%filter%' THEN 'filter page'
                 WHEN (stba.touch_landing_pagepath LIKE ANY ('%/current-sales%', '%currentSales', '/', '/#', '%aktuelle-angebote%', '%offerte-in-corso%')
                     OR stba.touch_landing_pagepath IS NULL
                     ) THEN 'current sales'
                 WHEN stba.touch_landing_pagepath LIKE ANY ('%instant-access%', '%instantAccess%') THEN 'instant access'
                 WHEN stba.touch_landing_pagepath LIKE ANY ('%search/search%', '%mbSearch/mbSearch%', '%/search') THEN 'search'
                 WHEN stba.touch_landing_pagepath LIKE ANY ('%/booking%', '%/bookings', '%/buchungen%') THEN 'booking page'
                 WHEN stba.touch_landing_pagepath LIKE ANY ('%/accounts/%', '%/your-account%', '%konto%') THEN 'account page'
                 WHEN stba.touch_landing_pagepath LIKE ANY ('%hotelSale%', '%sale-hotel%', '%hoteldetail%') THEN 'hotel sale'
                 WHEN (
                             stba.touch_landing_pagepath LIKE ANY ('%/sale%', '%/offerta', '%/offerta%')
                         OR stba.touch_hostname LIKE ANY ('%co.uk.sales%', '%de.sales%')
                     ) THEN 'sale' --RP: is the separation of Hotel only vs everything else for sale page okay?
                 WHEN (
                             stba.touch_hostname LIKE '%magazine.secretescapes%'
                         OR stba.touch_landing_pagepath LIKE '%/magazine-de/%'
                     ) THEN 'magazine secretescapes'
                 WHEN stba.touch_landing_pagepath LIKE ANY ('%terms-and-conditions%', '%/agb') THEN 'terms and conditions'
                 WHEN stba.touch_landing_pagepath LIKE ANY ('%/contact%', '%/contact', '%/kontakt', '%/kontakt%') THEN 'contact'
                 WHEN stba.touch_landing_pagepath LIKE ANY ('%/faq', '%mobile-faq', '%mobile-faq%', '%faq%') THEN 'faq'
                 WHEN stba.touch_landing_pagepath LIKE ANY ('%voucher%', '%geschenkgutscheine%') THEN 'vouchers'
                 WHEN stba.touch_hostname LIKE '%mp.secretescapes%' THEN 'mp.secretescapes'
                 WHEN stba.touch_hostname LIKE '%secretescapes.perfectstay%' THEN 'perfect stay'
                 WHEN stba.touch_hostname LIKE '%jetlineholidays%' THEN 'jetline holidays'
                 WHEN stba.touch_hostname LIKE '%lateluxury%' THEN 'lateluxury'
                 WHEN stba.touch_hostname LIKE ANY ('%travelbird.com%', '%travelbird.de%') THEN 'travelbird.com'
                 WHEN stba.touch_hostname LIKE '%secretescapes.exoticca%' THEN 'exoticca'
                 WHEN stba.touch_hostname LIKE ANY ('%www.hlx.com%', 'www.hlx.com%') THEN 'hlx'
                 WHEN stba.touch_landing_pagepath LIKE '%your-subscriptions' THEN 'subscriptions page'
                 WHEN stba.touch_landing_pagepath LIKE ANY ('%my-favourites', '%meine-favoriten', '%my-favourites%', '%meine-favoriten%') THEN 'my favourites'
                 WHEN stba.touch_landing_pagepath LIKE ANY ('%privacy-policy', '%privacy-policy%') THEN 'privacy policy'
                 WHEN stba.touch_landing_pagepath LIKE ANY ('%auth/login', '%auth/login%', '%/login') THEN 'login'
                 WHEN stba.touch_landing_pagepath LIKE ANY ('%credits', '%credits%') THEN 'credits'
                 WHEN stba.touch_landing_pagepath LIKE ANY ('%/reservation', '%/reservation%') THEN 'reservation'
                 WHEN stba.touch_landing_pagepath LIKE ANY ('%/payment/', '%/payment/%', '%/zahlungsinformationen/%', '%/zahlungsinformationen') THEN 'payment'
                 WHEN stba.touch_landing_pagepath LIKE ANY ('%/ueber-uns', '%/about-us', '%/about-us%') THEN 'about us'
                 WHEN stba.touch_landing_pagepath LIKE ANY ('%telegraph', '%telegraph%') THEN 'telegraph'
                 WHEN stba.touch_landing_pagepath LIKE ANY ('%flightFinder', '%flightFinder') THEN 'flight finder'
                 WHEN stba.touch_landing_pagepath LIKE ANY ('%forgottenpassword', '%forgottenpassword%') THEN 'forgotten password'
                 WHEN stba.touch_landing_pagepath LIKE ANY ('%freunde-einladen', '%invite_friends') THEN 'invite friends'
                 WHEN stba.touch_landing_pagepath LIKE '%datenschutzerklaerung' THEN 'data protection'
                 WHEN stba.touch_landing_pagepath LIKE '%homeliving%' THEN 'home living'
                 WHEN stba.touch_landing_pagepath LIKE '%/media%' THEN 'media'
                 WHEN stba.touch_landing_pagepath LIKE ANY ('%work-with-us%', '%workWithUs') THEN 'work with us'
                 WHEN stba.touch_hostname LIKE ANY ('%www.secretescapes.group', '%www.secretescapes.group%') THEN 'group'
                 WHEN stba.touch_hostname LIKE 'escapes.%' THEN 'escapes'
                 WHEN stba.touch_hostname LIKE '%eurowings.com%' THEN 'eurowings.com'
                 ELSE 'other'
                 END AS landing_page_categorisation

         FROM se.data.scv_touch_basic_attributes stba
             INNER JOIN se.data.scv_touch_marketing_channel stmc
                        ON stba.touch_id = stmc.touch_id

         WHERE stba.touch_start_tstamp >= $from_date
           AND stba.touch_start_tstamp <= $to_date
           AND stmc.touch_affiliate_territory IN ('UK', 'DE')
     )
        ,
     session_agg AS (
         SELECT
             DATE_TRUNC(WEEK, lpc.touch_start_tstamp)                                                                 AS week_start,
             lpc.touch_mkt_channel,
             lpc.touch_affiliate_territory                                                                            AS touch_hostname_territory,
             landing_page_categorisation,
             COALESCE(SUM(s.spvs), 0)                                                                                 AS spvs,
             COUNT(DISTINCT CASE WHEN lpc.stitched_identity_type = 'se_user_id' THEN lpc.attributed_user_id_hash END) AS logged_in_users,
             COALESCE(SUM(b.bookings), 0)                                                                             AS bookings,
             COALESCE(SUM(b.margin), 0)                                                                               AS margin
         FROM landing_page_categorisation lpc
             LEFT JOIN sess_bookings b ON lpc.touch_id = b.touch_id
             LEFT JOIN sess_spvs s ON lpc.touch_id = s.touch_id
         GROUP BY 1, 2, 3, 4
     )

SELECT
    touch_mkt_channel,
    touch_hostname_territory,
    CASE
        WHEN landing_page_categorisation = 'search' THEN 'Search'
        WHEN landing_page_categorisation IN ('magazine secretescapes', 'mp.secretescapes') THEN 'Media and Magazine'
        WHEN landing_page_categorisation IN ('no landing page', 'other') THEN 'Other'
        WHEN landing_page_categorisation IN ('exoticca', 'hlx', 'homeliving', 'jetlineholidays', 'lateluxury', 'perfectstay', 'telegraph', 'eurowings.com', 'travelbird.com', 'escapes')
            THEN '3rd Party'
        WHEN landing_page_categorisation IN
             ('about us', 'data protection', 'faq', 'group', 'invite friends', 'privacy policy', 'work with us', 'account page', 'terms and conditions', 'contact', 'forgotten password',
              'subscriptions_page', 'my favourites', 'credits', 'vouchers', 'flight finder'
                 ) THEN 'Self Service Pages'
        WHEN landing_page_categorisation IN ('current sales', 'hotel sale', 'instant access', 'sale') THEN 'Sale Pages'
        WHEN landing_page_categorisation LIKE 'app landing page' THEN 'App'
        WHEN landing_page_categorisation LIKE 'login' THEN 'Login'
        WHEN landing_page_categorisation IN ('booking page', 'payment', 'reservation') THEN 'Booking Flow'
        ELSE 'Filter' END AS landing_page_group,
    SUM(spvs)             AS spvs,
    SUM(logged_in_users)  AS logged_in_users,
    SUM(bookings)         AS bookings,
    SUM(margin)           AS margin

FROM session_agg sa
GROUP BY 1, 2, 3;


------------------------------------------------------------------------------------------------------------------------
USE WAREHOUSE pipe_xlarge;

SET (from_date, to_date)= ('2022-05-01', '2022-05-31');

WITH sess_bookings AS (
    SELECT
        stt.touch_id,
        COUNT(DISTINCT fcb.booking_id)                      AS bookings,
        SUM(fcb.margin_gross_of_toms_gbp_constant_currency) AS margin
    FROM se.data.scv_touch_basic_attributes stba
        INNER JOIN se.data.scv_touched_transactions stt ON stba.touch_id = stt.touch_id
        INNER JOIN se.data.fact_complete_booking fcb ON stt.booking_id = fcb.booking_id
    WHERE stba.touch_start_tstamp >= $from_date
      AND stba.touch_start_tstamp <= $to_date
    GROUP BY 1
),
     sess_spvs AS (
         SELECT
             stba.touch_id,
             COUNT(*) AS spvs
         FROM se.data.scv_touch_basic_attributes stba
             LEFT JOIN se.data.scv_touched_spvs sts ON stba.touch_id = sts.touch_id
         WHERE stba.touch_start_tstamp >= $from_date
           AND stba.touch_start_tstamp <= $to_date
         GROUP BY 1
     ),
     landing_page_categorisation AS (
         SELECT
             stba.touch_id,
             stba.touch_logged_in,
             stba.touch_landing_page,
             stba.touch_landing_pagepath,
             stmc.touch_hostname,
             stmc.touch_affiliate_territory,
             stba.touch_start_tstamp,
             stba.touch_end_tstamp,
             stmc.touch_mkt_channel,
             stba.stitched_identity_type,
             stba.attributed_user_id_hash,
             CASE
                 WHEN (stba.touch_landing_page IS NULL AND stba.touch_experience LIKE '%app%') THEN 'app landing page'
                 WHEN (stba.touch_landing_page IS NULL AND stba.touch_experience NOT LIKE '%app%') THEN 'no landing page'
                 WHEN stba.touch_landing_pagepath LIKE '%filter%' THEN 'filter page'
                 WHEN (stba.touch_landing_pagepath LIKE ANY ('%/current-sales%', '%currentSales', '%aktuelle-angebote%', '%offerte-in-corso%')
                     OR stba.touch_landing_pagepath IS NULL
                     ) THEN 'current sales'
                 WHEN stba.touch_landing_pagepath LIKE ANY ('%instant-access%', '%instantAccess%', '/', '/#', '%auth/login', '%auth/login%', '%/login') THEN 'instant access'
                 WHEN stba.touch_landing_pagepath LIKE ANY ('%search/search%', '%mbSearch/mbSearch%', '%/search') THEN 'search'
                 WHEN stba.touch_landing_pagepath LIKE ANY ('%/reservation', '%/reservation%', '%/sale/book%') THEN 'reservation'
                 WHEN (stba.touch_landing_pagepath LIKE ANY ('%hotelSale%', '%sale-hotel%', '%hoteldetail%', '%/sale-offers%', '%/sale%', '%/offerta', '%/offerta%',
                                                             '%/booking%', '%/buchungen%')
                     OR stba.touch_hostname LIKE ANY ('%co.uk.sales%', '%de.sales%')
                     ) THEN 'sale'
                 WHEN stba.touch_landing_pagepath LIKE ANY ('%/payment/', '%/payment/%', '%/zahlungsinformationen/%', '%/zahlungsinformationen') THEN 'payment'
                 WHEN (stba.touch_hostname LIKE '%magazine.secretescapes%'
                     OR stba.touch_landing_pagepath LIKE '%/magazine-de/%') THEN 'magazine secretescapes'
                 WHEN stba.touch_landing_pagepath LIKE ANY ('%terms-and-conditions%', '%/agb') THEN 'terms and conditions'
                 WHEN stba.touch_landing_pagepath LIKE ANY ('%/contact%', '%/contact', '%/kontakt', '%/kontakt%') THEN 'contact'
                 WHEN stba.touch_landing_pagepath LIKE ANY ('%/faq', '%mobile-faq', '%mobile-faq%', '%faq%') THEN 'faq'
                 WHEN stba.touch_landing_pagepath LIKE ANY ('%voucher%', '%geschenkgutscheine%') THEN 'vouchers'
                 WHEN stba.touch_hostname LIKE '%mp.secretescapes%' THEN 'mp.secretescapes'
                 WHEN stba.touch_hostname LIKE '%secretescapes.perfectstay%' THEN 'perfect stay'
                 WHEN stba.touch_hostname LIKE '%jetlineholidays%' THEN 'jetline holidays'
                 WHEN stba.touch_hostname LIKE '%lateluxury%' THEN 'lateluxury'
                 WHEN stba.touch_hostname LIKE ANY ('%travelbird.com%', '%travelbird.de%') THEN 'travelbird.com'
                 WHEN stba.touch_hostname LIKE '%secretescapes.exoticca%' THEN 'exoticca'
                 WHEN stba.touch_hostname LIKE ANY ('%www.hlx.com%', 'www.hlx.com%') THEN 'hlx'
                 WHEN stba.touch_hostname LIKE ANY ('%telegraph', '%telegraph%') THEN 'telegraph'
                 WHEN stba.touch_landing_pagepath LIKE '%your-subscriptions' THEN 'subscriptions page'
                 WHEN stba.touch_landing_pagepath LIKE ANY ('%my-favourites', '%meine-favoriten', '%my-favourites%', '%meine-favoriten%') THEN 'my favourites'
                 WHEN stba.touch_landing_pagepath LIKE ANY ('%privacy-policy', '%privacy-policy%') THEN 'privacy policy'
                 WHEN stba.touch_landing_pagepath LIKE ANY ('%credits', '%credits%') THEN 'credits'
                 WHEN stba.touch_landing_pagepath LIKE ANY ('%/ueber-uns', '%/about-us', '%/about-us%') THEN 'about us'
                 WHEN stba.touch_landing_pagepath LIKE ANY ('%flightFinder', '%flightFinder') THEN 'flight finder'
                 WHEN stba.touch_landing_pagepath LIKE ANY ('%forgottenpassword', '%forgottenpassword%') THEN 'forgotten password'
                 WHEN stba.touch_landing_pagepath LIKE ANY ('%freunde-einladen', '%invite_friends') THEN 'invite friends'
                 WHEN stba.touch_landing_pagepath LIKE '%datenschutzerklaerung' THEN 'data protection'
                 WHEN stba.touch_landing_pagepath LIKE '%homeliving%' THEN 'home living'
                 WHEN stba.touch_landing_pagepath LIKE '%/media%' THEN 'media'
                 WHEN stba.touch_landing_pagepath LIKE ANY ('%work-with-us%', '%workWithUs') THEN 'work with us'
                 WHEN stba.touch_hostname LIKE ANY ('%www.secretescapes.group', '%www.secretescapes.group%') THEN 'group'
                 WHEN stba.touch_hostname LIKE 'escapes.%' THEN 'escapes'
                 WHEN stba.touch_hostname LIKE '%eurowings.com%' THEN 'eurowings.com'
                 WHEN stba.touch_landing_pagepath LIKE ANY ('%/bookings') THEN 'my_booking'
                 WHEN stba.touch_landing_pagepath LIKE ANY ('%/accounts/%', '%/your-account%', '%konto%') THEN 'account page'
                 ELSE 'other'
                 END AS landing_page_categorisation

         FROM se.data.scv_touch_basic_attributes stba
             INNER JOIN se.data.scv_touch_marketing_channel stmc
                        ON stba.touch_id = stmc.touch_id

         WHERE stba.touch_start_tstamp >= $from_date
           AND stba.touch_start_tstamp <= $to_date
           AND stmc.touch_affiliate_territory IN ('UK', 'DE')
     ),
     session_agg AS (
         SELECT
             DATE_TRUNC(WEEK, lpc.touch_start_tstamp)                                                          AS week_start,
             lpc.touch_mkt_channel,
             lpc.touch_affiliate_territory                                                                     AS touch_hostname_territory,
             landing_page_categorisation,
             COALESCE(SUM(s.spvs), 0)                                                                          AS spvs,
             COUNT(DISTINCT lpc.attributed_user_id_hash)                                                       AS users,
             COUNT(DISTINCT IFF(lpc.stitched_identity_type = 'se_user_id', lpc.attributed_user_id_hash, NULL)) AS se_users,         --sessions that can be attributed to a user
             COUNT(DISTINCT IFF(lpc.stitched_identity_type = 'se_user_id', lpc.touch_id, NULL))                AS se_user_sessions, --sessions that can be attributed to a user
             COUNT(DISTINCT IFF(lpc.touch_logged_in, lpc.attributed_user_id_hash, NULL))                       AS se_logged_in_users,
             COUNT(DISTINCT IFF(lpc.touch_logged_in, lpc.touch_id, NULL))                                      AS se_logged_in_sessions,
             COALESCE(SUM(b.bookings), 0)                                                                      AS bookings,
             COALESCE(SUM(b.margin), 0)                                                                        AS margin,
             COUNT(DISTINCT lpc.touch_id)                                                                      AS sessions
         FROM landing_page_categorisation lpc
             LEFT JOIN sess_bookings b ON lpc.touch_id = b.touch_id
             LEFT JOIN sess_spvs s ON lpc.touch_id = s.touch_id
         GROUP BY 1, 2, 3, 4
     )

SELECT
    touch_mkt_channel,
    touch_hostname_territory,
    CASE
        WHEN landing_page_categorisation = 'search' THEN 'Search'
        WHEN landing_page_categorisation IN ('magazine secretescapes', 'mp.secretescapes') THEN 'Media and Magazine'
        WHEN landing_page_categorisation IN ('no landing page', 'other') THEN 'Other'
        WHEN landing_page_categorisation IN ('exoticca', 'hlx', 'homeliving', 'jetlineholidays', 'lateluxury', 'perfectstay', 'telegraph', 'eurowings.com', 'travelbird.com', 'escapes')
            THEN '3rd Party'
        WHEN landing_page_categorisation IN
             ('about us', 'data protection', 'faq', 'group', 'invite friends', 'privacy policy', 'work with us', 'account page', 'terms and conditions', 'contact', 'forgotten password',
              'subscriptions_page', 'my favourites', 'credits', 'vouchers', 'flight finder', 'my_booking'
                 ) THEN 'Self Service Pages'
        WHEN landing_page_categorisation IN ('current sales') THEN 'Current Sales'
        WHEN landing_page_categorisation IN ('sale') THEN 'Sale'
        WHEN landing_page_categorisation IN ('instant access') THEN 'Login'
        WHEN landing_page_categorisation LIKE 'app landing page' THEN 'App'
        WHEN landing_page_categorisation IN ('booking page', 'payment', 'reservation') THEN 'Booking Flow'
        ELSE 'Filter' END         AS landing_page_group,
    SUM(spvs)                     AS spvs,
    SUM(se_users)                 AS se_users,
    SUM(se_user_sessions)         AS se_user_sessions,   --distinct count users for any session we can associate to a member
    SUM(se_logged_in_users)       AS logged_in_users,    --distinct count of users that logged in during each session
    SUM(sa.se_logged_in_sessions) AS logged_in_sessions, --distinct count of sessions where a user has logged in
    SUM(bookings)                 AS bookings,
    SUM(margin)                   AS margin,
    SUM(users)                    AS users,              --distinct count of unknown identifiers eg unique users of the site
    SUM(sessions)                 AS sessions
FROM session_agg sa
GROUP BY 1, 2, 3
;



SELECT * FROM se.data_pii.scv_session_events_link ssel;

