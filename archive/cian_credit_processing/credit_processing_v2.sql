------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------
--  welcome back campaign
-- bookings made before 1/9/20 since 8/6/2020
-- trips can take place on any date during 2020
-- £50 credit will be applied to the member's account within one week of their return from the trip
-- credit is valid for 6 months
-- this promotion can only be claimed once per person


-- customer id,
-- booking date
-- amount gbp and eur
-- check in check out


CREATE OR REPLACE VIEW collab.refund_credits.welcome_back_campaign_bookings AS
SELECT sb.booking_id,
       sb.transaction_id,
       sb.booking_completed_date,
       sb.shiro_user_id,
       sb.check_in_date,
       sb.check_out_date,
       sb.rebooked,
       sb.booking_status,
       sb.margin_gross_of_toms_gbp,
       sb.gross_booking_value_gbp
FROM se.data.se_booking sb
         LEFT JOIN se.data.se_user_attributes sua ON sb.shiro_user_id = sua.shiro_user_id
WHERE sb.booking_status = 'COMPLETE'
  AND sb.check_out_date >= '2020-06-08'
  AND sb.check_out_date < current_date          --only care about bookings once the check out date has past
  AND sb.check_out_date <= '2020-12-31'         --check out date in 2020
  AND sb.booking_completed_date >= '2020-06-08' --start of campaign
  AND sb.booking_completed_date < '2020-09-01'  --hard deadline of bookings made up until sep 2020
  AND sb.currency IN ('GBP', 'EUR')             --transacting in gbp or eur
  AND sua.original_affiliate_territory IN ('UK', 'DE') --uk or de customers
;



CREATE OR REPLACE TABLE collab.refund_credits.welcome_back_campaign_users
(
    processed_date      DATE,
    shiro_user_id       INT PRIMARY KEY NOT NULL,
    affiliate_territory VARCHAR,
    email               VARCHAR,
    bookings            INT,
    booking_id_list     VARCHAR,
    total_margin_gbp    DOUBLE,
    no_credits          INT,
    credit_type_list    VARCHAR
);

SELECT sua.original_affiliate_territory
FROM se.data.se_user_attributes sua;


CREATE OR REPLACE PROCEDURE collab.refund_credits.insert_welcome_campaign_users_procedure()
    RETURNS VARCHAR
    LANGUAGE JAVASCRIPT
    RETURNS NULL ON NULL INPUT AS
$$

        var sql_command = `

    MERGE INTO collab.refund_credits.welcome_back_campaign_users target
    USING (
        WITH campaign_users AS (
            SELECT cb.shiro_user_id,
                   COUNT(cb.booking_id)             AS bookings,
                   LISTAGG(cb.booking_id, ', ')     AS booking_id_list,
                   SUM(cb.margin_gross_of_toms_gbp) AS total_margin_gbp
                   -- only show users with bookings processed today
            FROM collab.refund_credits.welcome_back_campaign_bookings cb
            GROUP BY 1
        ),
             user_credits AS (
                 SELECT scm.user_id,
                        scm.credit_currency,
                        SUM(scm.credit_amount)         AS total_credit,
                        count(*)                       AS no_credits,
                        LISTAGG(scm.credit_type, ', ') AS credit_type_list
                 FROM se.data.se_credit_model scm
                 WHERE LOWER(scm.credit_status) = 'active'
                   AND (scm.credit_expires_on IS NULL OR scm.credit_expires_on >= current_date)
                 GROUP BY 1, 2
             )
        SELECT current_date AS processed_date,
               cu.shiro_user_id,
               sua.original_affiliate_territory AS affiliate_territory,
               sua.email,
               cu.bookings,
               cu.booking_id_list,
               cu.total_margin_gbp,
               uc.no_credits,
               uc.credit_type_list
        FROM campaign_users cu
                 LEFT JOIN se.data_pii.se_user_attributes sua ON cu.shiro_user_id = sua.shiro_user_id
                 LEFT JOIN user_credits uc ON cu.shiro_user_id = uc.user_id
    ) AS batch ON target.shiro_user_id = batch.shiro_user_id
    WHEN MATCHED
        THEN UPDATE SET
        target.email = batch.email,
        target.affiliate_territory = batch.affiliate_territory,
        target.bookings = batch.bookings,
        target.booking_id_list = batch.booking_id_list,
        target.total_margin_gbp = batch.total_margin_gbp,
        target.no_credits = batch.no_credits,
        target.credit_type_list = batch.credit_type_list
    WHEN NOT MATCHED
        THEN INSERT VALUES (batch.processed_date,
                            batch.shiro_user_id,
                            batch.affiliate_territory,
                            batch.email,
                            batch.bookings,
                            batch.booking_id_list,
                            batch.total_margin_gbp,
                            batch.no_credits,
                            batch.credit_type_list);

        ;`

        var stmt = snowflake.createStatement( {sqlText: sql_command} );
        stmt.execute();
        return "Rows inserted"
    $$;


--process data
CALL collab.refund_credits.insert_welcome_campaign_users_procedure();

SELECT *
FROM collab.refund_credits.welcome_back_campaign_users wbcu
WHERE wbcu.processed_date = current_date;

SELECT *
FROM collab.refund_credits.welcome_back_campaign_bookings wbcb;



------------------------------------------------------------------------------------------------------------------------
--work out which users were missed from current process.
WITH missing_users AS (
    SELECT wbcu.shiro_user_id::INT AS shiro_user_id
    FROM collab.refund_credits.welcome_back_campaign_users wbcu
    MINUS
    SELECT DISTINCT pb.customerid::INT AS shiro_user_id
    FROM collab.refund_credits.processed_bookings pb
)
SELECT *
FROM collab.refund_credits.welcome_back_campaign_users wbcu
         INNER JOIN missing_users mu ON wbcu.shiro_user_id = mu.shiro_user_id;


WITH missing_users AS (
    SELECT wbcu.shiro_user_id::INT AS shiro_user_id
    FROM collab.refund_credits.welcome_back_campaign_users wbcu
    MINUS
    SELECT DISTINCT pb.customerid::INT AS shiro_user_id
    FROM collab.refund_credits.processed_bookings pb
)
SELECT *
FROM collab.refund_credits.welcome_back_campaign_bookings wbcb
         INNER JOIN missing_users mu ON wbcb.shiro_user_id = mu.shiro_user_id;



GRANT USAGE ON SCHEMA collab.refund_credits TO ROLE personal_role__cianweeresinghe;
GRANT SELECT ON ALL TABLES IN SCHEMA collab.refund_credits TO ROLE personal_role__cianweeresinghe;
GRANT SELECT ON ALL VIEWS IN SCHEMA collab.refund_credits TO ROLE personal_role__cianweeresinghe;
GRANT USAGE ON PROCEDURE collab.refund_credits.insert_welcome_campaign_users_procedure() TO ROLE personal_role__cianweeresinghe;
GRANT USAGE ON SCHEMA collab.refund_credits TO ROLE personal_role__radujosan;
GRANT SELECT ON ALL TABLES IN SCHEMA collab.refund_credits TO ROLE personal_role__radujosan;
GRANT SELECT ON ALL VIEWS IN SCHEMA collab.refund_credits TO ROLE personal_role__radujosan;
GRANT USAGE ON PROCEDURE collab.refund_credits.insert_welcome_campaign_users_procedure() TO ROLE personal_role__radujosan;

GRANT USAGE ON SCHEMA collab.refund_credits TO ROLE personal_role__sophieserunjogi;
GRANT SELECT ON ALL TABLES IN SCHEMA collab.refund_credits TO ROLE personal_role__sophieserunjogi;
GRANT SELECT ON ALL VIEWS IN SCHEMA collab.refund_credits TO ROLE personal_role__sophieserunjogi;
GRANT USAGE ON PROCEDURE collab.refund_credits.insert_welcome_campaign_users_procedure() TO ROLE personal_role__sophieserunjogi;

GRANT USAGE ON SCHEMA collab.refund_credits TO ROLE personal_role__kirstengrieve;
GRANT SELECT ON ALL TABLES IN SCHEMA collab.refund_credits TO ROLE personal_role__kirstengrieve;
GRANT SELECT ON ALL VIEWS IN SCHEMA collab.refund_credits TO ROLE personal_role__kirstengrieve;
GRANT USAGE ON PROCEDURE collab.refund_credits.insert_welcome_campaign_users_procedure() TO ROLE personal_role__kirstengrieve;

GRANT USAGE ON SCHEMA collab.refund_credits TO ROLE personal_role__gianniraftis;
GRANT SELECT ON ALL TABLES IN SCHEMA collab.refund_credits TO ROLE personal_role__gianniraftis;
GRANT SELECT ON ALL VIEWS IN SCHEMA collab.refund_credits TO ROLE personal_role__gianniraftis;
GRANT USAGE ON PROCEDURE collab.refund_credits.insert_welcome_campaign_users_procedure() TO ROLE personal_role__gianniraftis


SELECT *
FROM collab.refund_credits.welcome_back_campaign_users wbcu
WHERE wbcu.processed_date = CURRENT_DATE;

DELETE
FROM collab.refund_credits.welcome_back_campaign_users
WHERE welcome_back_campaign_users.processed_date = CURRENT_DATE;


self_describing_task --include 'staging/hygiene/worldpay/transaction_summary.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
self_describing_task --include 'se/data/se_hotel_rooms_and_rates.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'



SELECT *
FROM collab.refund_credits.welcome_back_campaign_users WHERE shiro_user_id = '63056168';

SELECT * FROM collab.refund_credits.welcome_back_campaign_bookings;