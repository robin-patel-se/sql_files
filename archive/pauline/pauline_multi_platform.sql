WITH number_of_platforms AS (
    SELECT stba.attributed_user_id_hash,
           COUNT(DISTINCT
                 IFF(stba.touch_experience IN ('mobile wrap android', 'mobile wrap ios'), 'app', 'web')) AS platform_count
    FROM se.data.scv_touch_basic_attributes stba
    WHERE stba.stitched_identity_type = 'se_user_id'
      AND stba.touch_start_tstamp >= DATEADD(MONTH, -6, CURRENT_DATE) --change if necessary
    GROUP BY 1
)
SELECT nop.platform_count,
       COUNT(DISTINCT nop.attributed_user_id_hash) AS users
FROM number_of_platforms nop
GROUP BY 1
;

WITH number_of_platforms AS (
    SELECT stba.attributed_user_id_hash,
           COUNT(DISTINCT
                 IFF(stba.touch_experience IN ('mobile wrap android', 'mobile wrap ios'), 'app', stba.touch_experience)) AS platform_count
    FROM se.data.scv_touch_basic_attributes stba
    WHERE stba.stitched_identity_type = 'se_user_id'
      AND stba.touch_start_tstamp >= DATEADD(MONTH, -6, CURRENT_DATE) --change if necessary
    GROUP BY 1
)
SELECT nop.platform_count,
       COUNT(DISTINCT nop.attributed_user_id_hash) AS users
FROM number_of_platforms nop
GROUP BY 1
;