USE WAREHOUSE pipe_xlarge;
ALTER SESSION SET QUERY_TAG = 'MAU investigation';


SELECT
    DATE_TRUNC(MONTH, stba.touch_start_tstamp)                                                          AS month,
    COUNT(*)                                                                                            AS sessions,
    COUNT(DISTINCT stba.attributed_user_id_hash)                                                        AS users,
    SUM(IFF(stba.stitched_identity_type = 'se_user_id', 1, 0))                                          AS member_sessions,
    COUNT(DISTINCT IFF(stba.stitched_identity_type = 'se_user_id', stba.attributed_user_id_hash, NULL)) AS members
FROM se.data.scv_touch_basic_attributes stba
WHERE stba.touch_start_tstamp >= '2019-01-01'
GROUP BY 1
