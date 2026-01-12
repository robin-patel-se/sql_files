SELECT
	stmc.touch_affiliate_territory,
	COUNT(*) AS spvs
FROM se.data.scv_touched_spvs sts
INNER JOIN se.data.scv_touch_basic_attributes stba
	ON sts.touch_id = stba.touch_id
INNER JOIN se.data.scv_touch_marketing_channel stmc
	ON stba.touch_id = stmc.touch_id
WHERE sts.se_sale_id = 'A60139'
GROUP BY 1
;


USE WAREHOUSE pipe_xlarge
;

CREATE OR REPLACE TABLE scratch.robinpatel.a59874_spvs AS
SELECT
	sts.*,
	stba.* EXCLUDE (touch_id), stmc.*
		   EXCLUDE (touch_id, touch_start_tstamp, touch_landing_page, touch_hostname, touch_hostname_territory, app_push_open_context),
FROM se.data.scv_touched_spvs sts
INNER JOIN se.data_pii.scv_touch_basic_attributes stba
	ON sts.touch_id = stba.touch_id
	AND stba.touch_start_tstamp >= CURRENT_DATE - 90
INNER JOIN se.data.scv_touch_marketing_channel stmc
	ON stba.touch_id = stmc.touch_id
	AND stmc.touch_start_tstamp >= CURRENT_DATE - 90
WHERE sts.se_sale_id = 'A59874'
  AND stmc.touch_affiliate_territory = 'DE'
  AND sts.event_tstamp >= CURRENT_DATE - 90
;


SELECT
	a59874_spvs.touch_mkt_channel,
	COUNT(*)                                                                             AS total_spvs,
	SUM(IFF(a59874_spvs.stitched_identity_type = 'se_user_id', 1, 0))                    AS member_spv,
	SUM(IFF(a59874_spvs.stitched_identity_type IS DISTINCT FROM 'se_user_id', 1, 0)) AS non_member_spv
FROM scratch.robinpatel.a59874_spvs
GROUP BY ALL