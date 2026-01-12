-- demo of data grip to tj

SELECT
	schedule_tstamp,
	run_tstamp,
	operation_id,
	created_at,
	updated_at,
	tvl_user_id,
	territory_name,
	territory_id,
	first_name,
	last_name,
	username,
	is_superuser,
	is_active,
	is_staff,
	email,
	last_login,
	signup_tstamp,
	registration_ip,
	external_user_id,
	authentication_token,
	account_profile_updated_at,
	account_profile_created_at,
	member_id,
	email_subscription_status,
	email_subscription_date,
	email_subscription_frequency_value,
	email_subscription_frequency,
	tenant_id,
	email_subscription_updated_at,
	se_brand
FROM data_vault_mvp.dwh.tvl_user_attributes tua
;


-- showing multiple carets
SELECT
	YEAR(fcb.booking_completed_timestamp)               AS year,
	SUM(fcb.margin_gross_of_toms_gbp_constant_currency) AS margin_gross_of_toms_gbp_constant_currency,
	SUM(fcb.gross_revenue_gbp_constant_currency)        AS gross_revenue_gbp_constant_currency
FROM se.data.fact_complete_booking fcb
WHERE YEAR(fcb.booking_completed_timestamp)  != 2023
GROUP BY ALL;

