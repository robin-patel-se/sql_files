--get a distinct list of users and the test group they are in
SELECT DISTINCT --ses.page_url,
       ssel.attributed_user_id,
--        PARSE_URL(ses.page_url)         AS parse_url,
       TRY_TO_NUMBER(PARSE_URL(ses.page_url):parameters:gce_rbf::VARCHAR) AS test_group
FROM se.data_pii.scv_event_stream ses
         INNER JOIN se.data_pii.scv_session_events_link ssel ON ses.event_hash = ssel.event_hash AND ssel.stitched_identity_type = 'se_user_id'
WHERE ses.event_tstamp >= '2021-07-07'
  AND ses.page_url LIKE '%gce_rbf%'
  AND test_group IS NOT NULL
;

--gce_rbf
SELECT DISTINCT
       ssel.touch_id,
       ssel.attributed_user_id,
       TRY_TO_NUMBER(PARSE_URL(ses.page_url)['parameters']['gce_rbf']::VARCHAR) AS test_group,
       stt.booking_id,
       sb.territory,
       sb.se_sale_id,
       sb.total_sell_rate_cc,
       sb.gross_revenue_cc,
       -- regex translates to either a 5 or 9 before decimal and then any combination of 4 digits after decimal
       IFF(sb.gross_revenue_cc REGEXP '.*[5|9]\\.....', TRUE, FALSE)            AS zero_pennies,
       sb.customer_total_price_cc,
       sb.gross_booking_value_cc,
       ssa.product_type
FROM se.data_pii.scv_event_stream ses
         INNER JOIN se.data_pii.scv_session_events_link ssel ON ses.event_hash = ssel.event_hash AND ssel.stitched_identity_type = 'se_user_id'
         INNER JOIN se.data.scv_touched_transactions stt ON ssel.touch_id = stt.touch_id
         INNER JOIN se.data.se_booking sb ON stt.booking_id = sb.booking_id AND sb.booking_status = 'COMPLETE'
         INNER JOIN se.data.se_sale_attributes ssa ON sb.se_sale_id = ssa.se_sale_id AND ssa.product_type = 'Hotel'
WHERE ses.event_tstamp >= '2021-07-07' --since the 7th of July
  AND ses.page_url LIKE '%gce_rbf%'
  AND test_group IS NOT NULL;



------------------------------------------------------------------------------------------------------------------------
--Gianni code:
--TO DO Refactor users activity to include what sales they actually saw, by using content viewed
USE WAREHOUSE pipe_large;
CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.user_table AS
    (
        SELECT --ses.page_url,
               ssel.attributed_user_id,
               MAX(IFF(TRY_TO_NUMBER(PARSE_URL(ses.page_url): PARAMETERS:gce_rbf::VARCHAR) = 0, 1, 0))     AS control_0,
               MAX(IFF(TRY_TO_NUMBER(PARSE_URL(ses.page_url): PARAMETERS:gce_rbf::VARCHAR) = 1, 1, 0))     AS control_1,
               MAX(IFF(TRY_TO_NUMBER(PARSE_URL(ses.page_url): PARAMETERS:gce_rbf::VARCHAR) IS NULL, 1, 0)) AS no_gce
        FROM se.data_pii.scv_event_stream ses
                 INNER JOIN se.data_pii.scv_session_events_link ssel ON ses.event_hash = ssel.event_hash AND ssel.stitched_identity_type = 'se_user_id'
                 INNER JOIN se.data.se_user_attributes ua ON ua.shiro_user_id::VARCHAR = ssel.attributed_user_id
        WHERE ses.event_tstamp >= '2021-07-07'
          AND ua.original_affiliate_territory = 'DE'
          --AND ses.device_platform NOT IN('native app ios','native app android') --remove wrapped mweb
          AND (
                (
                    ses.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR = 'sale'--this denotes that it is a sale page
                    --AND
                    --ses.mkt_medium IS NULL --filter out if they have a utm medium on the sale page (to match how the test is set up)
                    )
                OR
                RLIKE(ses.page_urlpath, '.*/aktuelle-angebote.?', 'i')
                OR
                ses.page_urlpath LIKE ('/current-sales%')
                OR
                ses.page_urlpath LIKE ('/search/search%')
                OR
                LOWER(ses.page_urlpath) LIKE ('/search/mbsearch%')
            )
        GROUP BY 1
    );
/*
Urls included in the exp: (some are regex)
https://www.secretescapes.de/current-sales
https://www.secretescapes.de/aktuelle-angebote
https://www.secretescapes.de/[^/]+/sale-hotel
https://www.secretescapes.de/search/search
https://www.secretescapes.de/search/mbSearch
And also don't contain:
sale-hotel.*utm_medium
*/
--top level analysis of user split
SELECT SUM(CASE WHEN control_0 = 1 AND control_1 = 0 AND no_gce = 0 THEN 1 ELSE 0 END) AS control_group_0,
       SUM(CASE WHEN control_1 = 1 AND control_0 = 0 AND no_gce = 0 THEN 1 ELSE 0 END) AS control_group_1,
       control_group_0 + control_group_1                                               AS total_users_in_test,
       COUNT(*)                                                                        AS all_users,
       all_users - total_users_in_test                                                 AS total_users_not_in_test,
       1 - (total_users_in_test / all_users)                                           AS percentage_diff
FROM scratch.robinpatel.user_table



SELECT ut.control_0,
       ut.control_1,
       COUNT(DISTINCT ut.attributed_user_id)
FROM scratch.robinpatel.user_table ut
GROUP BY 1, 2









