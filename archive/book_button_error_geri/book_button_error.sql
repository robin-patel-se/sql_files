CREATE TABLE scratch.robinpatel.logs_book_button_error
(
    log_timestamp timestamp,
    error_message VARCHAR
)
;
USE DATABASE scratch;
USE SCHEMA robinpatel


PUT file:///Users/robin/myrepos/sql_files/book_button_error_geri/error_logs.csv @%logs_book_button_error;

COPY INTO scratch.robinpatel.logs_book_button_error
    FILE_FORMAT = (
        TYPE = CSV
            FIELD_DELIMITER = ','
            SKIP_HEADER = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
            RECORD_DELIMITER = '\\n'
        );

SELECT *
FROM scratch.robinpatel.logs_book_button_error log;

WITH failed_bookings AS (
    SELECT
        log.log_timestamp,
        log.error_message,
        'A' || REGEXP_SUBSTR(log.error_message, 'id=(.*)\\)', 1, 1, 'e') AS booking_id
    FROM scratch.robinpatel.logs_book_button_error log
)
SELECT *
FROM se.data.fact_booking fb
    INNER JOIN failed_bookings flb ON fb.booking_id = flb.booking_id;

USE WAREHOUSE pipe_xlarge

WITH failed_bookings AS (
    SELECT
        log.log_timestamp,
        log.error_message,
        'A' || REGEXP_SUBSTR(log.error_message, 'id=(.*)\\)', 1, 1, 'e') AS booking_id
    FROM scratch.robinpatel.logs_book_button_error log
)

SELECT *
FROM failed_bookings fb
    LEFT JOIN se.data_pii.scv_event_stream ses ON fb.booking_id = ses.booking_id AND ses.event_tstamp >= '2022-09-22'
WHERE ses.booking_id IS NULL;



WITH failed_bookings AS (
    SELECT
        log.log_timestamp,
        log.error_message,
        'A' || REGEXP_SUBSTR(log.error_message, 'id=(.*)\\)', 1, 1, 'e') AS booking_id
    FROM scratch.robinpatel.logs_book_button_error log
),
     unique_browsers_for_failed_bookers AS (
         SELECT DISTINCT
             ses.unique_browser_id
         FROM failed_bookings fb
             INNER JOIN se.data_pii.scv_event_stream ses ON fb.booking_id = ses.booking_id AND ses.event_tstamp >= '2022-09-22'
     )
SELECT
    f.booking_status,
    s.*
FROM se.data_pii.scv_event_stream s
    INNER JOIN unique_browsers_for_failed_bookers ub ON s.unique_browser_id = ub.unique_browser_id
    LEFT JOIN  se.data.fact_booking f ON s.booking_id = f.booking_id
WHERE s.event_tstamp >= '2022-09-22';


WITH failed_bookings AS (
    SELECT
        log.log_timestamp,
        log.error_message,
        'A' || REGEXP_SUBSTR(log.error_message, 'id=(.*)\\)', 1, 1, 'e') AS booking_id
    FROM scratch.robinpatel.logs_book_button_error log
),
     unique_browsers_for_failed_bookers AS (
         SELECT DISTINCT
             ses.unique_browser_id
         FROM failed_bookings fb
             INNER JOIN se.data_pii.scv_event_stream ses ON fb.booking_id = ses.booking_id AND ses.event_tstamp >= '2022-09-22'
     )
SELECT
    f.booking_status,
    flb.booking_id IS NOT NULL AS failed_booking,
    s.booking_id,
    s.se_user_id,
    s.unique_browser_id,
    s.event_tstamp
FROM se.data_pii.scv_event_stream s
    INNER JOIN unique_browsers_for_failed_bookers ub ON s.unique_browser_id = ub.unique_browser_id
    LEFT JOIN  failed_bookings flb ON s.booking_id = flb.booking_id
    LEFT JOIN  se.data.fact_booking f ON s.booking_id = f.booking_id
WHERE s.event_tstamp >= '2022-09-22'
  AND s.booking_id IS NOT NULL;


WITH failed_bookings AS (
    SELECT
        log.log_timestamp,
        log.error_message,
        'A' || REGEXP_SUBSTR(log.error_message, 'id=(.*)\\)', 1, 1, 'e') AS booking_id
    FROM scratch.robinpatel.logs_book_button_error log
),
     unique_browsers_for_failed_bookers AS (
         SELECT DISTINCT
             ses.unique_browser_id
         FROM failed_bookings fb
             INNER JOIN se.data_pii.scv_event_stream ses ON fb.booking_id = ses.booking_id AND ses.event_tstamp >= '2022-09-22'
     )
SELECT DISTINCT
    f.booking_status,
    f.booking_created_date,
    flb.booking_id IS NOT NULL AS failed_booking,
    s.booking_id,
    s.se_user_id,
    s.unique_browser_id
FROM se.data_pii.scv_event_stream s
    INNER JOIN unique_browsers_for_failed_bookers ub ON s.unique_browser_id = ub.unique_browser_id
    LEFT JOIN  failed_bookings flb ON s.booking_id = flb.booking_id
    LEFT JOIN  se.data.fact_booking f ON s.booking_id = f.booking_id
WHERE s.event_tstamp >= '2022-09-22'
  AND s.booking_id IS NOT NULL
ORDER BY s.se_user_id, f.booking_created_date;


------------------------------------------------------------------------------------------------------------------------
WITH error_bookings AS (
    SELECT
        column1 AS error_booking_id
    FROM
    VALUES ('A11097892'),
           ('A11096484'),
           ('A11096407'),
           ('A11096348'),
           ('A11096317'),
           ('A11096235'),
           ('A11096067'),
           ('A11095998'),
           ('A11095530'),
           ('A11091840'),
           ('A11090665'),
           ('A11090585'),
           ('A11090196'),
           ('A11088269'),
           ('A11087260'),
           ('A11087067'),
           ('A11085921'),
           ('A11085391'),
           ('A11083716'),
           ('A11082630'),
           ('A11080140'),
           ('A11079593'),
           ('A11079153'),
           ('A11077220'),
           ('A11077204'),
           ('A11077132'),
           ('A11075289'),
           ('A11073831'),
           ('A11071131'),
           ('A11070965'),
           ('A11070373'),
           ('A11066162'),
           ('A11066078'),
           ('A11065236'),
           ('A11062784'),
           ('A11059878'),
           ('A11057649'),
           ('A11057178'),
           ('A11057121'),
           ('A11057068'),
           ('A11056291'),
           ('A11056248'),
           ('A11053920'),
           ('A11053905'),
           ('A11053468'),
           ('A11053429'),
           ('A11051962'),
           ('A11051619'),
           ('A11051591'),
           ('A11050418'),
           ('A11050260'),
           ('A11050051'),
           ('A11048643'),
           ('A11048116'),
           ('A11045123'),
           ('A11044609'),
           ('A11043153'),
           ('A11043020'),
           ('A11042974'),
           ('A11042910'),
           ('A11042411'),
           ('A11042319'),
           ('A11041832'),
           ('A11039122'),
           ('A11034221'),
           ('A11033878'),
           ('A11027383'),
           ('A11027372'),
           ('A11026765'),
           ('A11026467'),
           ('A11025553'),
           ('A11025046'),
           ('A11025031'),
           ('A11022699'),
           ('A11018604'),
           ('A11017050'),
           ('A11016795'),
           ('A11015671'),
           ('A11015227'),
           ('A11014860'),
           ('A11012474'),
           ('A11012426'),
           ('A11012337'),
           ('A11012006'),
           ('A10965112'),
           ('A10965039'),
           ('A10965007'),
           ('A10964965'),
           ('A10964651'),
           ('A10964623')
)
SELECT *
FROM se.data.se_booking sb
    INNER JOIN error_bookings eb ON sb.booking_id = eb.error_booking_id;
-- all bookings matched are in status abandoned

-- look for users of these bookings and what they did after
WITH error_bookings AS (
    SELECT
        column1 AS error_booking_id
    FROM
    VALUES ('A11097892'),
           ('A11096484'),
           ('A11096407'),
           ('A11096348'),
           ('A11096317'),
           ('A11096235'),
           ('A11096067'),
           ('A11095998'),
           ('A11095530'),
           ('A11091840'),
           ('A11090665'),
           ('A11090585'),
           ('A11090196'),
           ('A11088269'),
           ('A11087260'),
           ('A11087067'),
           ('A11085921'),
           ('A11085391'),
           ('A11083716'),
           ('A11082630'),
           ('A11080140'),
           ('A11079593'),
           ('A11079153'),
           ('A11077220'),
           ('A11077204'),
           ('A11077132'),
           ('A11075289'),
           ('A11073831'),
           ('A11071131'),
           ('A11070965'),
           ('A11070373'),
           ('A11066162'),
           ('A11066078'),
           ('A11065236'),
           ('A11062784'),
           ('A11059878'),
           ('A11057649'),
           ('A11057178'),
           ('A11057121'),
           ('A11057068'),
           ('A11056291'),
           ('A11056248'),
           ('A11053920'),
           ('A11053905'),
           ('A11053468'),
           ('A11053429'),
           ('A11051962'),
           ('A11051619'),
           ('A11051591'),
           ('A11050418'),
           ('A11050260'),
           ('A11050051'),
           ('A11048643'),
           ('A11048116'),
           ('A11045123'),
           ('A11044609'),
           ('A11043153'),
           ('A11043020'),
           ('A11042974'),
           ('A11042910'),
           ('A11042411'),
           ('A11042319'),
           ('A11041832'),
           ('A11039122'),
           ('A11034221'),
           ('A11033878'),
           ('A11027383'),
           ('A11027372'),
           ('A11026765'),
           ('A11026467'),
           ('A11025553'),
           ('A11025046'),
           ('A11025031'),
           ('A11022699'),
           ('A11018604'),
           ('A11017050'),
           ('A11016795'),
           ('A11015671'),
           ('A11015227'),
           ('A11014860'),
           ('A11012474'),
           ('A11012426'),
           ('A11012337'),
           ('A11012006'),
           ('A10965112'),
           ('A10965039'),
           ('A10965007'),
           ('A10964965'),
           ('A10964651'),
           ('A10964623')
),
     get_users AS (
         SELECT
             sb.shiro_user_id,
             MIN(sb.booking_created_date) AS first_error_booking_date
         FROM se.data.se_booking sb
             INNER JOIN error_bookings eb ON sb.booking_id = eb.error_booking_id
         GROUP BY 1
     )
SELECT
    eb.error_booking_id IS NOT NULL AS is_error_booking,
    s.booking_id,
    s.shiro_user_id,
    s.transaction_id,
    s.booking_status,
    s.booking_created_date,
    s.booking_completed_date
FROM se.data.se_booking s
    INNER JOIN get_users gu ON s.shiro_user_id = gu.shiro_user_id AND s.booking_created_date >= gu.first_error_booking_date
    LEFT JOIN  error_bookings eb ON s.booking_id = eb.error_booking_id
ORDER BY shiro_user_id, s.booking_created_timestamp;

------------------------------------------------------------------------------------------------------------------------

WITH error_bookings AS (
    SELECT
        log.log_timestamp,
        log.error_message,
        'A' || REGEXP_SUBSTR(log.error_message, 'id=(.*)\\)', 1, 1, 'e') AS booking_id,
        sb.shiro_user_id
    FROM scratch.robinpatel.logs_book_button_error log
        LEFT JOIN se.data.se_booking sb ON 'A' || REGEXP_SUBSTR(log.error_message, 'id=(.*)\\)', 1, 1, 'e') = sb.booking_id
),
     compute_rebookings AS (
         SELECT
             sb.booking_status,
             sb.booking_id,
             eb.shiro_user_id
         FROM se.data.se_booking sb
             INNER JOIN error_bookings eb ON sb.shiro_user_id = eb.shiro_user_id AND sb.booking_completed_date >= eb.log_timestamp::DATE
         WHERE sb.booking_status IN ('COMPLETE', 'REFUNDED')
           AND sb.booking_id NOT IN (
             SELECT DISTINCT
                 eb2.booking_id
             FROM error_bookings eb2
             WHERE eb2.booking_id IS NOT NULL
         )
     ),
     aggregate_rebookers AS (
         SELECT
             cr.shiro_user_id,
             COUNT(DISTINCT cr.booking_id)    AS rebookings,
             LISTAGG(cr.booking_status, ', ') AS rebooking_booking_status_list,
             LISTAGG(cr.booking_id, ', ')     AS rebooking_booking_id_list
         FROM compute_rebookings cr
         GROUP BY 1
     )

SELECT
    eb.log_timestamp,
    eb.booking_id,
    eb.shiro_user_id,
    ar.shiro_user_id IS NOT NULL AS is_rebooker,
    ar.rebookings,
    ar.rebooking_booking_status_list,
    ar.rebooking_booking_id_list
FROM error_bookings eb
    LEFT JOIN aggregate_rebookers ar ON eb.shiro_user_id = ar.shiro_user_id
;

/*They requested the below:
User fiends: email
email opt in status
main affiliate id
country
Sale fields:
Se_sale_id
destination
Margin value of intended booking*/


WITH error_bookings AS (
    SELECT
        log.log_timestamp,
        log.error_message,
        'A' || REGEXP_SUBSTR(log.error_message, 'id=(.*)\\)', 1, 1, 'e') AS booking_id,
        sb.shiro_user_id
    FROM scratch.robinpatel.logs_book_button_error log
        LEFT JOIN se.data.se_booking sb ON 'A' || REGEXP_SUBSTR(log.error_message, 'id=(.*)\\)', 1, 1, 'e') = sb.booking_id
),
     compute_rebookings AS (
         SELECT
             sb.booking_status,
             sb.booking_id,
             eb.shiro_user_id
         FROM se.data.se_booking sb
             INNER JOIN error_bookings eb ON sb.shiro_user_id = eb.shiro_user_id AND sb.booking_completed_date >= eb.log_timestamp::DATE
         WHERE sb.booking_status IN ('COMPLETE', 'REFUNDED')
           AND sb.booking_id NOT IN (
             SELECT DISTINCT
                 eb2.booking_id
             FROM error_bookings eb2
             WHERE eb2.booking_id IS NOT NULL
         )
     ),
     aggregate_rebookers AS (
         SELECT
             cr.shiro_user_id,
             COUNT(DISTINCT cr.booking_id)    AS rebookings,
             LISTAGG(cr.booking_status, ', ') AS rebooking_booking_status_list,
             LISTAGG(cr.booking_id, ', ')     AS rebooking_booking_id_list
         FROM compute_rebookings cr
         GROUP BY 1
     ),
     rebooked_bookings AS (
         SELECT
             eb.log_timestamp,
             eb.booking_id,
             eb.shiro_user_id,
             ar.shiro_user_id IS NOT NULL AS is_rebooker,
             ar.rebookings,
             ar.rebooking_booking_status_list,
             ar.rebooking_booking_id_list
         FROM error_bookings eb
             LEFT JOIN aggregate_rebookers ar ON eb.shiro_user_id = ar.shiro_user_id
     )
SELECT *
FROM rebooked_bookings rb
WHERE rb.is_rebooker = FALSE
;
-- from the list of bookings where the user didn't rebook what sale were they trying to book?

WITH bookings_that_didnt_rebook AS (
    SELECT
        column1 AS bookings_that_did_not_rebook
    FROM
    VALUES ('A11026467'),
           ('A11041832'),
           ('A11042319'),
           ('A11043020'),
           ('A11043153'),
           ('A11048116'),
           ('A11048643'),
           ('A11050051'),
           ('A11050418'),
           ('A11051591'),
           ('A11051619'),
           ('A11053429'),
           ('A11053468'),
           ('A11056291'),
           ('A11062784'),
           ('A11065236'),
           ('A11077132'),
           ('A11077204'),
           ('A11077220'),
           ('A11079153'),
           ('A11080140'),
           ('A11083716'),
           ('A11088269'),
           ('A11090585'),
           ('A11090665'),
           ('A11096235'),
           ('A11096348')
),
     unique_browsers_for_failed_bookers AS (
         SELECT DISTINCT
             ses.unique_browser_id
         FROM bookings_that_didnt_rebook fb
             INNER JOIN se.data_pii.scv_event_stream ses ON fb.bookings_that_did_not_rebook = ses.booking_id AND ses.event_tstamp >= '2022-09-22'
     )
SELECT
    f.booking_status,
    s.*
FROM se.data_pii.scv_event_stream s
    INNER JOIN unique_browsers_for_failed_bookers ub ON s.unique_browser_id = ub.unique_browser_id
    LEFT JOIN  se.data.fact_booking f ON s.booking_id = f.booking_id
WHERE s.event_tstamp >= '2022-09-22';
;



WITH bookings_that_didnt_rebook AS (
    SELECT
        column1 AS bookings_that_did_not_rebook
    FROM
    VALUES ('A11026467'),
           ('A11041832'),
           ('A11042319'),
           ('A11043020'),
           ('A11043153'),
           ('A11048116'),
           ('A11048643'),
           ('A11050051'),
           ('A11050418'),
           ('A11051591'),
           ('A11051619'),
           ('A11053429'),
           ('A11053468'),
           ('A11056291'),
           ('A11062784'),
           ('A11065236'),
           ('A11077132'),
           ('A11077204'),
           ('A11077220'),
           ('A11079153'),
           ('A11080140'),
           ('A11083716'),
           ('A11088269'),
           ('A11090585'),
           ('A11090665'),
           ('A11096235'),
           ('A11096348')
)
SELECT DISTINCT
    s.se_sale_id,
    s.booking_id,
    s.se_user_id,
    sua.email,
    sua.email_opt_in_status,
    sua.main_affiliate_id,
    sua.country
FROM se.data_pii.scv_event_stream s
    INNER JOIN bookings_that_didnt_rebook bdr ON s.booking_id = bdr.bookings_that_did_not_rebook
    LEFT JOIN  se.data_pii.se_user_attributes sua ON s.se_user_id = sua.shiro_user_id
WHERE s.event_tstamp >= '2022-09-22';
;


SELECT
    fb.se_sale_id,
    AVG(fb.margin_gross_of_toms_gbp_constant_currency)
FROM se.data.fact_complete_booking fb
-- WHERE fb.booking_completed_date >= '2022-01-01'
WHERE fb.se_sale_id IN (
                        'A32381',
                        'A17170',
                        'A10796',
                        'A24650',
                        'A24650',
                        'A53299',
                        'A49200',
                        'A17170',
                        'A10796',
                        'A10796',
                        'A11037',
                        'A11037',
                        'A48195',
                        'A45686',
                        'A48504',
                        'A33305',
                        'A33305',
                        'A33305',
                        'A16574',
                        'A30801',
                        'A49656',
                        'A35838',
                        'A35838',
                        'A35838',
                        'A22169',
                        'A22169'
    )
GROUP BY 1;


SELECT
    sua.email,
    sua.email_opt_in_status,
    sua.main_affiliate_id,
    sua.country
FROM se.data_pii.se_user_attributes sua
WHERE sua.shiro_user_id IN (
                            '2770849',
                            '5801497',
                            '32798448',
                            '37340046',
                            '43258632',
                            '52678272',
                            '72244976',
                            '73226945',
                            '76812328',
                            '76903958',
                            '77401410',
                            '78035018',
                            '78203191',
                            '78227102',
                            '78434229',
                            '78448059',
                            '78448550',
                            '78450111',
                            '78464369'
    );
;


SELECT sua.shiro_user_id,
       sua.original_affiliate_name,
       sua.original_affiliate_territory,
       sua.current_affiliate_name,
       sua.current_affiliate_territory,
       sua.membership_account_status,
       sua.email_opt_in_status
FROM se.data.se_user_attributes sua
WHERE sua.current_affiliate_territory = 'US'
