WITH
	device_data AS (
		SELECT
			stba.attributed_user_id                          AS user_id,
			stba.touch_experience,
			stba.touch_experience LIKE 'native app%'         AS app_device,
			stba.os_family,
			stba.touch_experience || ' - ' || stba.os_family AS device_platform,
			MIN(stba.touch_start_tstamp)                     AS first_tstamp,
			MIN(stba.touch_start_tstamp)                     AS last_tstamp
		FROM se.data_pii.scv_touch_basic_attributes stba
		WHERE stba.stitched_identity_type = 'se_user_id'
		GROUP BY 1, 2, 3, 4
	)
SELECT
	dd.user_id,
	OBJECT_AGG(dd.device_platform::VARCHAR, dd.first_tstamp::VARIANT)                      AS first_device_tstamp,
	COUNT(DISTINCT dd.device_platform)                                                     AS no_of_devices,
	IFF(no_of_devices > 1, 'multiple', ANY_VALUE(dd.device_platform))                      AS device_group,
	LISTAGG(dd.device_platform, ', ') WITHIN GROUP (ORDER BY dd.device_platform )          AS device_list,
	COUNT(DISTINCT IFF(app_device, dd.device_platform, NULL))                              AS no_of_app_devices,
	IFF(no_of_app_devices > 1, 'multiple', MAX(IFF(app_device, dd.device_platform, NULL))) AS app_device_group,
	LISTAGG(IFF(app_device, dd.device_platform, NULL), ', ')
			WITHIN GROUP ( ORDER BY dd.device_platform )                                   AS app_device_list
FROM device_data dd
GROUP BY 1
;



SELECT
	stba.os_family,
	COUNT(*)
FROM se.data_pii.scv_touch_basic_attributes stba
GROUP BY 1
;

SELECT *
FROM hygiene_snapshot_vault_mvp.travelbird_mysql.flights_flightsearchtracker_alternative_flights ffaf
;