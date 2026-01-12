-- original query
SELECT
	---date_trunc(month, date(touch_start_tstamp)) month
	COUNT(DISTINCT stba.attributed_user_id_hash) AS total_maus
FROM se.data.scv_touch_basic_attributes stba
	INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
WHERE stba.stitched_identity_type = 'se_user_id'
  AND stmc.touch_affiliate_territory IN ('UK', 'Conde Nast UK', 'Guardian - UK')
  AND stba.touch_start_tstamp::date BETWEEN ('2022-08-01') AND ('2022-08-31')
;

-- 692994


-- adjusted territory for user's current territory
SELECT
	---date_trunc(month, date(touch_start_tstamp)) month
	COUNT(DISTINCT stba.attributed_user_id) AS total_maus
FROM se.data_pii.scv_touch_basic_attributes stba
	INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
	INNER JOIN se.data.se_user_attributes sua ON TRY_TO_NUMBER(stba.attributed_user_id) = sua.shiro_user_id
WHERE stba.stitched_identity_type = 'se_user_id'
--   AND stmc.touch_affiliate_territory IN ('UK', 'Conde Nast UK', 'Guardian - UK')
  AND sua.current_affiliate_territory = 'UK'
  AND stba.touch_start_tstamp::date BETWEEN ('2022-08-02') AND ('2022-08-31')
;

-- 674206


-- pulling one day from demand model
SELECT
	SUM(mau.mau)
FROM data_vault_mvp.bi.monthly_active_users mau
WHERE mau.date = '2022-08-31'
  AND mau.current_affiliate_territory = 'UK'
;


SELECT
	SUM(mau.mau)
FROM data_vault_mvp.bi.monthly_active_users mau
WHERE mau.date = '2022-08-01'
  AND mau.current_affiliate_territory = 'UK'
-- WHERE mau.date BETWEEN ('2022-08-01') AND ('2022-08-31')
;

-- 651587


-- pulling one day from query
SELECT
	---date_trunc(month, date(touch_start_tstamp)) month
	COUNT(DISTINCT stba.attributed_user_id) AS total_maus
FROM se.data_pii.scv_touch_basic_attributes stba
	INNER JOIN se.data.se_user_attributes sua
			   ON TRY_TO_NUMBER(stba.attributed_user_id) = sua.shiro_user_id AND sua.current_affiliate_territory = 'UK'
WHERE stba.stitched_identity_type = 'se_user_id'
  AND stba.touch_start_tstamp::date = '2022-08-01'
;


-- territory
-- mau in demand model is a 30d lookback


SELECT
-- 	stmc.touch_affiliate_territory,
-- 	sua.current_affiliate_territory,
sua.current_affiliate_territory = stmc.touch_affiliate_territory AS match,
COUNT(*)
FROM se.data_pii.scv_touch_basic_attributes stba
	INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
	INNER JOIN se.data.se_user_attributes sua ON TRY_TO_NUMBER(stba.attributed_user_id) = sua.shiro_user_id
WHERE stba.touch_start_tstamp::DATE BETWEEN '2023-09-01' AND '2023-09-30'
GROUP BY 1
;

SELECT
	SUM(IFF(mis.created_at BETWEEN '2023-10-01' AND '2023-10-31', 1, 0)) AS created_in_month,
	SUM(IFF(mis.created_at IS DISTINCT FROM mis.updated_at, 1, 0))       AS updated_in_month
FROM data_vault_mvp.single_customer_view_stg.module_identity_stitching mis
WHERE mis.updated_at BETWEEN '2023-10-01' AND '2023-10-31'
;



SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_identity_associations mia
;