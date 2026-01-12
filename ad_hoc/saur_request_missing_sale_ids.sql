USE WAREHOUSE pipe_xlarge;

SELECT booking_status,
       count(*)
FROM collab.finance.se_booking_summary_v b
         LEFT JOIN se.data.dim_sale s
                   ON s.se_sale_id::VARCHAR = b.sale_id::VARCHAR
WHERE b.sale_id IS NULL
GROUP BY 1
ORDER BY 1 DESC;

SELECT *
FROM collab.finance.se_booking_v
WHERE sale_id IS NULL; -- 36,180 null sale ids
SELECT *
FROM collab.finance.se_reservation_v
WHERE sale_id IS NULL; -- no null sale ids

SELECT b.id,
       ba.allocation_id,
       a.id,
       o.id,
       s.id
FROM data_vault_mvp.cms_mysql_snapshots.booking_snapshot b
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.booking_allocations_snapshot ba ON b.id = ba.booking_allocations_id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.allocation_snapshot a ON ba.allocation_id = a.id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.offer_snapshot o
                   ON o.id = a.offer_id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.sale_snapshot s
                   ON s.id = o.sale_id
WHERE b.id IN (SELECT booking_id
               FROM collab.finance.se_booking_summary_v b
                        LEFT JOIN se.data.dim_sale s
                                  ON s.se_sale_id::VARCHAR = b.sale_id::VARCHAR
               WHERE b.sale_id IS NULL);

SELECT *
FROM data_vault_mvp.cms_mysql_snapshots.booking_allocations_snapshot
WHERE booking_allocations_id IN (SELECT booking_id
                                 FROM collab.finance.se_booking_summary_v b
                                          LEFT JOIN se.data.dim_sale s
                                                    ON s.se_sale_id::VARCHAR = b.sale_id::VARCHAR
                                 WHERE b.sale_id IS NULL);
--se_booking_summary_v

SELECT *
FROM collab.finance.se_booking_v
UNION
SELECT *
FROM collab.finance.se_reservation_v;

--se_booking_v
CREATE OR REPLACE VIEW collab.finance.se_booking_v
    COPY GRANTS
AS
WITH credit_sum AS (
    SELECT bc.booking_credits_used_id,
           SUM(c.amount) AS amount
    FROM data_vault_mvp.cms_mysql_snapshots.booking_credit_snapshot bc
             LEFT JOIN data_vault_mvp.cms_mysql_snapshots.credit_snapshot c
                       ON bc.credit_id = c.id
    GROUP BY 1
),
     booking_allocation_max_allocation_id AS (
         SELECT booking_allocations_id,
                MAX(allocation_id) AS allocation_id
         FROM data_vault_mvp.cms_mysql_snapshots.booking_allocations_snapshot
         GROUP BY 1
     ),
     booking_allocations AS (
         SELECT ba.booking_allocations_id,
                ba.allocation_id,
                al.offer_id
         FROM booking_allocation_max_allocation_id ba
                  INNER JOIN data_vault_mvp.cms_mysql_snapshots.allocation_snapshot al
                             ON al.id = ba.allocation_id
     )
SELECT b.id::VARCHAR                  AS booking_id,
       o.id                           AS offer_id,
       s.id::VARCHAR                  AS sale_id,
       b.unique_transaction_reference AS order_code,
       b.user_id                      AS user_id,
       b.affiliate_user_id            AS affiliate_user_id,
       b.payment_id                   AS payment_id,
       b.date_created                 AS date_created,
       b.completion_date              AS completion_date,
       b.last_updated                 AS last_updated,
       COALESCE(au.date_created,
                su.date_created
           )                          AS user_join_date,
       b.type                         AS booking_type,
       b.status                       AS booking_status,
       b.hold_id                      AS hold_id,
       p.type                         AS payment_type,
       p.status                       AS payment_status,
       tau.name                       AS affiliate_territory,
       tsu.name                       AS se_territory,
       b.currency                     AS currency,
       b.booking_fee                  AS booking_fee,
       b.atol_fee                     AS atol_fee,
       p.surcharge                    AS payment_surcharge,
       bc.amount                      AS credit_amount,
       p.amount                       AS payment_amount
FROM data_vault_mvp.cms_mysql_snapshots.booking_snapshot b
         LEFT JOIN booking_allocations ba
                   ON ba.booking_allocations_id = b.id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.offer_snapshot o
                   ON o.id = ba.offer_id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.sale_snapshot s
                   ON s.id = o.sale_id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.payment_snapshot p
                   ON p.id = b.payment_id
         LEFT JOIN credit_sum bc
                   ON bc.booking_credits_used_id = b.id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.affiliate_user_snapshot au
                   ON au.id = b.affiliate_user_id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.shiro_user_snapshot su
                   ON su.id = b.user_id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.affiliate_snapshot aau
                   ON aau.id = au.affiliate_id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.affiliate_snapshot asu
                   ON asu.id = su.affiliate_id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.territory_snapshot tau
                   ON tau.id = aau.territory_id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.territory_snapshot tsu
                   ON tsu.id = asu.territory_id;

WITH credit_sum AS (
    SELECT id,
           SUM(amount) AS amount
    FROM data_vault_mvp.cms_mysql_snapshots.credit_snapshot
    GROUP BY 1
)
SELECT 'A' || r.id::VARCHAR           AS booking_id,
       bof.id                         AS offer_id,
       r.sale_id                      AS sale_id,
       r.unique_transaction_reference AS order_code,
       r.user_id                      AS user_id,
       r.affiliate_user_id            AS affiliate_user_id,
       r.payment_id                   AS payment_id,
       r.date_created                 AS date_created,
       r.completion_date              AS completion_date,
       r.last_updated                 AS last_updated,
       COALESCE(
               au.date_created,
               su.date_created
           )                          AS user_join_date,
       CASE
           WHEN r.type = 'BOOKING'
               THEN 'RESERVATION'
           ELSE r.type
           END                        AS booking_type,
       r.status                       AS booking_status,
       NULL                           AS hold_id,
       p.type                         AS payment_type,
       p.status                       AS payment_status,
       tau.name                       AS affiliate_territory,
       tsu.name                       AS se_territory,
       r.currency                     AS currency,
       r.booking_fee                  AS booking_fee,
       0::FLOAT                       AS atol_fee,
       p.surcharge                    AS payment_surcharge,
       c.amount                       AS credit_amount,
       p.amount                       AS payment_amount
FROM hygiene_snapshot_vault_mvp.cms_mysql.reservation r
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.base_sale_snapshot bsa
                   ON bsa.id = r.sale_id__o
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.base_offer_snapshot bof
                   ON bof.id = bsa.default_hotel_offer_id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.payment_snapshot p
                   ON p.id = r.payment_id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.reservation_credit_snapshot rc
                   ON rc.reservation_credits_used_id = r.id
         LEFT JOIN credit_sum c
                   ON c.id = rc.credit_id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.affiliate_user_snapshot au
                   ON au.id = r.affiliate_user_id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.shiro_user_snapshot su
                   ON su.id = r.user_id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.affiliate_snapshot aau
                   ON aau.id = au.affiliate_id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.affiliate_snapshot asu
                   ON asu.id = su.affiliate_id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.territory_snapshot tau
                   ON tau.id = aau.territory_id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.territory_snapshot tsu
                   ON tsu.id = asu.territory_id;


WITH a AS (
    SELECT t.id                    AS territory_id
         , b.sale_id               AS deal_id
         , a.attributed_user_id    AS user_id
         , 'order'                 AS evt_name
         , to_date(e.event_tstamp) AS evt_date
         , max(e.event_tstamp)     AS max_event_ts
    FROM data_vault_mvp.single_customer_view_stg.module_touched_transactions e
             JOIN data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes a
                  ON a.touch_id = e.touch_id
             JOIN se.data.fact_complete_booking b
                  ON b.booking_id = e.booking_id
             JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel mc
                  ON mc.touch_id = e.touch_id
             JOIN
         (SELECT DISTINCT id, name FROM data_vault_mvp.cms_mysql_snapshots.territory_snapshot) t
         ON t.name = mc.touch_hostname_territory
    WHERE e.event_tstamp >= CURRENT_DATE - 365
      AND a.stitched_identity_type = 'se_user_id'
      --and a.touch_hostname like '%secretescapes%'
      AND a.attributed_user_id IS NOT NULL
      AND e.event_tstamp IS NOT NULL
      AND mc.touch_hostname_territory IS NOT NULL
      AND b.sale_id IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5)
SELECT territory_id,
       count(*)
FROM a
GROUP BY 1
ORDER BY 2 DESC
;
WITH a AS (
    SELECT t.id                    AS territory_id
         , t.name                  AS territory_name
         , b.sale_id               AS deal_id
         , a.attributed_user_id    AS user_id
         , 'order'                 AS evt_name
         , to_date(e.event_tstamp) AS evt_date
         , max(e.event_tstamp)     AS max_event_ts
    FROM data_vault_mvp.single_customer_view_stg.module_touched_transactions e
             JOIN data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes a
                  ON a.touch_id = e.touch_id
             JOIN se.data.fact_complete_booking b
                  ON b.booking_id = e.booking_id
             JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel mc
                  ON mc.touch_id = e.touch_id
             JOIN
         (SELECT DISTINCT id, name FROM data_vault_mvp.cms_mysql_snapshots.territory_snapshot) t
         ON t.name = mc.touch_affiliate_territory
    WHERE e.event_tstamp >= CURRENT_DATE - 365
      AND a.stitched_identity_type = 'se_user_id'
      --and a.touch_hostname like '%secretescapes%'
      AND a.attributed_user_id IS NOT NULL
      AND e.event_tstamp IS NOT NULL
      AND mc.touch_hostname_territory IS NOT NULL
      AND b.sale_id IS NOT NULL
      AND b.tech_platform = 'TRAVELBIRD'
    GROUP BY 1, 2, 3, 4, 5, 6
)
SELECT territory_id,
       territory_name,
       count(*)
FROM a
GROUP BY 1, 2
;