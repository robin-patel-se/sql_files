SET (start_dt,end_dt) = (DATE '2021-02-01', DATE '2021-02-02');
SELECT $start_dt, $end_dt;
USE WAREHOUSE pipe_xlarge;
WITH bookings AS (
    --work out input users based on bookings that have occurred within our target criteria
    SELECT DISTINCT
           att.touch_id
         , att.touch_end_tstamp
         , att.touch_start_tstamp
         , stmc.touch_mkt_channel
         , att.attributed_user_id_hash
    FROM se.data.scv_touch_basic_attributes att
             JOIN se.data.scv_touch_marketing_channel stmc ON stmc.touch_id = att.touch_id
             JOIN se.data.scv_touched_transactions stt ON stt.touch_id = att.touch_id
    WHERE att.touch_start_tstamp BETWEEN $start_dt AND $end_dt
      AND att.attributed_user_id_hash = '1367445c75cf3151983a75c962b24de7ad3b89b339226a4afefa1e51c6f3eaf3'
),
     session_channel AS (
         --compute session index for all sessions of users within our bookings input criteria
         SELECT att.touch_id
              , att.touch_start_tstamp
              , stmc.touch_mkt_channel
              , ROW_NUMBER() OVER (PARTITION BY att.attributed_user_id_hash ORDER BY att.touch_start_tstamp) AS session_index
              , att.attributed_user_id_hash
         FROM se.data.scv_touch_basic_attributes att
                  JOIN se.data.scv_touch_marketing_channel stmc ON stmc.touch_id = att.touch_id
         WHERE att.attributed_user_id_hash IN (
             SELECT attributed_user_id_hash
             FROM bookings
         )
     ),
     conversion_marker AS (
         --create booking flag partition marker to help set conversion index
         --sessions can have more than one conversion so need to characterise any session with at least one conversion as a converted session (not using booking ids)
         SELECT sc.touch_id,
                sc.attributed_user_id_hash,
                sc.touch_mkt_channel AS session_channel,
                sc.session_index,
                sc.touch_start_tstamp,
                bc.touch_id          AS booking_touch_id,
                IFF(LAG(bc.touch_id)
                        OVER (PARTITION BY sc.attributed_user_id_hash ORDER BY sc.touch_start_tstamp ) IS NOT NULL, 1,
                    0)               AS booking_flag_marker
         FROM session_channel sc
                  LEFT JOIN bookings bc ON bc.touch_id = sc.touch_id
     ),
     conversion_index AS (
         --attach a conversion index to each session
         SELECT cm.touch_id,
                cm.attributed_user_id_hash,
                cm.session_channel,
                cm.session_index,
                cm.touch_start_tstamp,
                cm.booking_touch_id,
                cm.booking_flag_marker,
                SUM(cm.booking_flag_marker)
                    OVER (PARTITION BY cm.attributed_user_id_hash ORDER BY cm.touch_start_tstamp) AS conversion_index
         FROM conversion_marker cm
     )
--calculate the conversion touch id each session should be attributed to
SELECT ci.touch_id,
       ci.attributed_user_id_hash,
       ci.session_channel,
       ci.session_index,
       ci.touch_start_tstamp,
       ci.booking_touch_id,
       ci.booking_flag_marker,
       ci.conversion_index,
       LAST_VALUE(ci.booking_touch_id) IGNORE NULLS
           OVER (PARTITION BY ci.attributed_user_id_hash, conversion_index ORDER BY ci.touch_start_tstamp) AS attributed_conversion_touch_id
FROM conversion_index ci
;

USE WAREHOUSE pipe_xlarge;



SELECT att.attributed_user_id_hash
     , COUNT(*)
FROM se.data.scv_touch_basic_attributes att
         JOIN se.data.scv_touch_marketing_channel stmc ON stmc.touch_id = att.touch_id
         JOIN se.data.scv_touched_transactions stt ON stt.touch_id = att.touch_id
WHERE att.touch_start_tstamp BETWEEN $start_dt AND $end_dt
GROUP BY 1
ORDER BY 2 DESC;