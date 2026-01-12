WITH traffic AS (
    SELECT stba.attributed_user_id_hash,
           stmc.touch_landing_page,
           stmc.landing_page_parameters,
           stmc.utm_content,
           stmc.landing_page_parameters:gce_rec::VARCHAR AS test_group
    FROM se.data.scv_touch_basic_attributes stba
             INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
    WHERE stmc.landing_page_parameters:gce_rec IS NOT NULL
      AND stba.touch_start_tstamp >= CURRENT_DATE - 1
)
SELECT t.test_group,
       COUNT(DISTINCT t.attributed_user_id_hash) AS users
FROM traffic t
;


SELECT booking_id,
       date_created         AS bk_cnx_date,
       last_updated         AS bk_cnx_last_updated,
       fault                AS bk_cnx_fault,
       reason               AS bk_cnx_reason,
       refund_channel       AS bk_cnx_refund_channel,
       refund_type          AS bk_cnx_refund_type,
       who_pays             AS bk_cnx_who_pays,
       cancel_with_provider AS bk_cnx_cancel_with_provider
FROM data_vault_mvp.cms_mysql_snapshots.booking_cancellation_snapshot bcs
    QUALIFY ROW_NUMBER() OVER (PARTITION BY booking_id ORDER BY last_updated DESC) = 1;


SELECT * FROM se.data.se_room_rates srr;