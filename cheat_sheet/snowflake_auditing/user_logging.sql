WITH
	live_snowflake_users AS (
		SELECT
			u.email,
			u.has_password,
			SPLIT_PART(u.email, '@', -1)                        AS email_domain,
			u.created_on,
			DATEDIFF(MONTH, u.created_on, CURRENT_DATE)         AS months_since_creation,
			u.last_success_login,
			u.last_success_login IS NULL                        AS has_never_logged_in,
			DATEDIFF(MONTH, u.last_success_login, CURRENT_DATE) AS months_since_login
		FROM snowflake.account_usage.users u
		WHERE u.disabled = FALSE
		  AND u.deleted_on IS NULL
	)
-- SELECT
-- 	lsu.email_domain,
-- 	lsu.has_never_logged_in,
-- 	COUNT(*) AS users
-- FROM live_snowflake_users lsu
-- GROUP BY 1, 2
-- ;

-- 80 snowflake users that have never logged in

-- SELECT
-- 	lsu.months_since_creation,
-- 	COUNT(*) AS users
-- FROM live_snowflake_users lsu
-- WHERE lsu.has_never_logged_in
--   AND lsu.email_domain = 'secretescapes.com'
-- GROUP BY 1

-- 54 users that have not logged in in the last 2 years

/*MONTHS_SINCE_CREATION	USERS
66	1
52	1
47	1
46	1
45	4
43	1
40	3
37	16
36	6
35	5
34	2
33	3
30	1
29	2
28	1
27	3
26	1
25	2*/

SELECT *
FROM live_snowflake_users lsu
WHERE lsu.has_never_logged_in
  AND lsu.email_domain = 'secretescapes.com'
  AND lsu.months_since_creation >= 25

--


CREATE OR REPLACE VIEW collab.muse.snowflake_users COPY GRANTS AS
(

SELECT
	SHA2(u.email)                                       AS email_hash,
	u.disabled,
	IFF(u.deleted_on IS NOT NULL, TRUE, FALSE)          AS user_deleted,
	u.has_password,
	SPLIT_PART(u.email, '@', -1)                        AS email_domain,
	u.created_on,
	DATEDIFF(MONTH, u.created_on, CURRENT_DATE)         AS months_since_creation,
	u.last_success_login,
	u.last_success_login IS NULL                        AS has_never_logged_in,
	DATEDIFF(MONTH, u.last_success_login, CURRENT_DATE) AS months_since_login,
	u.password_last_set_time
FROM snowflake.account_usage.users u
	);

GRANT SELECT ON TABLE collab.muse.snowflake_users TO ROLE tableau;

SELECT * FROM latest_vault.cro_gsheets.key_dates_definition kdd;