USE WAREHOUSE pipe_xlarge;

WITH sess_bookings AS (
--aggregate bookings up to session, because sessions _can_ have multiple bookings
    SELECT stt.touch_id,
           COUNT(*) AS bookings
    FROM se.data.scv_touched_transactions stt
    GROUP BY 1
)
   , sess_spvs AS (
    SELECT s.touch_id,
           COUNT(*)                                                          AS spvs,
           COUNT(DISTINCT s.se_sale_id)                                      AS unique_spvs,
           SUM(CASE WHEN ds.product_configuration = 'Hotel' THEN 1 END)      AS ho_spvs,
           SUM(CASE WHEN ds.product_configuration = 'Hotel Plus' THEN 1 END) AS hp_spvs,
           SUM(CASE WHEN ds.product_configuration = 'Package' THEN 1 END)    AS p_spvs,
           SUM(CASE WHEN ds.product_configuration = '3PP' THEN 1 END)        AS "3pp_spvs"
    FROM se.data.scv_touched_spvs s
             LEFT JOIN se.data.dim_sale ds ON s.se_sale_id = ds.se_sale_id
    GROUP BY 1
)
SELECT stba.touch_start_tstamp::DATE                                 AS day,
       stmc.touch_mkt_channel,
       stba.touch_experience,
       stba.touch_hostname_territory,
       COUNT(DISTINCT stba.touch_id)                                 AS sessions,
       COUNT(DISTINCT stba.attributed_user_id_hash)                  AS users,
       COUNT(DISTINCT CASE
                          WHEN stba.stitched_identity_type = 'se_user_id'
                              THEN stba.attributed_user_id_hash END) AS logged_in_users,
       COALESCE(SUM(b.bookings), 0)                                  AS bookings,
       COALESCE(SUM(s.spvs), 0 ) as spvs,
       COALESCE(SUM(s.unique_spvs), 0 ) as unique_spvs,
       COALESCE(SUM(s.ho_spvs), 0 ) as ho_spvs,
       COALESCE(SUM(s.hp_spvs), 0 ) as hp_spvs,
       COALESCE(SUM(s.p_spvs), 0 ) as p_spvs,
       COALESCE(SUM(s."3pp_spvs"), 0 ) as "3pp_spvs"


FROM se.data.scv_touch_basic_attributes stba
         INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
         LEFT JOIN sess_bookings b ON stba.touch_id = b.touch_id
         LEFT JOIN sess_spvs s ON stba.touch_id = s.touch_id

WHERE
      stba.touch_start_tstamp >= '2020-01-01'
  AND stba.touch_start_tstamp < current_date
GROUP BY 1, 2, 3, 4;
