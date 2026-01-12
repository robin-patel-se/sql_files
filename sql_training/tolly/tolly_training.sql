-- SELECT
--
-- Columns
--
-- FROM
--
-- LEFT JOIN
--
-- WHERE

------------------------------------------------------------------------------------------------------------------------

-- Orders
--
-- Aggregations
--
-- Other Joins
--
-- Functions
--
-- Logical Expressions
--
-- Agg FUNCTIONS
--
-- CTEs (sub queries)

-- warehouse objects

SELECT DATE_TRUNC(MONTH, CURRENT_DATE - 365)
;

SELECT
	PARSE_URL('https://co.uk.sales.secretescapes.com/114236/the-best-of-porto-portugal/?token=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJyb2Jpbi5wYXRlbEBnbWFpbC5jb20iLCJhdWQiOiJ0YiIsImFmZmlsaWF0ZU5hbWUiOiJnb29nbGUgUFBDIiwiY2hlY2tJbiI6IiIsImlzcyI6InNlIiwib3JpZ2luYWxBZmZpbGlhdGVOYW1lIjoiZ29vZ2xlIFBQQyIsImNoZWNrT3V0IjoiIiwiZXhwIjoxNjg1MDA2MjQ5LCJ1c2VySWQiOjEwNTI3NTM3LCJ1YmlkIjoiMDEyYmYyMDAtY2EwZC00ODg3LWJkMzItMzQ3YzExZjgxZmUyIiwidm9uYWdlSWQiOiIifQ.yGhhTYSZ5MVKQCrVj-cDjd-XHns_DEnIS9va1HHPmlM&source=swp&urlSlug=a-taste-of-porto-with-river-cruise-porto-portugal')
;

USE WAREHOUSE pipe_xlarge
;

USE ROLE accountadmin
;


SELECT
	fcb.booking_id,
	fcb.margin_gross_of_toms_gbp_constant_currency,
	sua.signup_tstamp
FROM se.data.fact_complete_booking fcb -- booking table of live bookings
	LEFT JOIN se.data.se_user_attributes sua ON fcb.shiro_user_id = sua.shiro_user_id
WHERE fcb.shiro_user_id IS NULL
;

-- how joins explode data

------------------------------------------------------------------------------------------------------------------------
-- order bys
-- expensive, avoid if you can
-- using numbers is a shorthand to reference columns defined in SELECT statement, based on their position/order
SELECT
	fcb.booking_completed_date,
	fcb.check_in_date,
	fcb.check_out_date
FROM se.data.fact_complete_booking fcb
WHERE fcb.booking_completed_date >= CURRENT_DATE - 5
ORDER BY fcb.booking_completed_date DESC, fcb.check_in_date, fcb.check_out_date DESC
;

-- same query using column numbers
SELECT
	fcb.booking_completed_date,
	fcb.check_in_date,
	fcb.check_out_date
FROM se.data.fact_complete_booking fcb
WHERE fcb.booking_completed_date >= CURRENT_DATE - 5
ORDER BY 1 DESC, 2, 3 DESC
;

------------------------------------------------------------------------------------------------------------------------


/*
 Homework 2: get a list of UK current affiliate territory members from the se_user_attributes table that have joined since the 1st of May, attach on fact_complete_booking with a LEFT JOIN to get a list of bookings each member has made. Output columns should be
shiro user id
current affiliate territory
sign up tstamp
booking id
booking completed date
margin
Show query results but also answer the question, "If a user hasn't made a booking, will they be in the results? and why?"
 */

USE WAREHOUSE pipe_xlarge
;
;

SELECT *
FROM se.data.se_user_attributes sua
	INNER JOIN se.data.se_booking sb ON sua.shiro_user_id = sb.shiro_user_id
WHERE sua.shiro_user_id = 20029060
;


------------------------------------------------------------------------------------------------------------------------


-- next steps into aggregations


SELECT
	col1,
	col2,
-- 	FUNCTION(field)
	SUM(),
	AVG(),
	MAX(),
	MIN()
FROM table xx
-- GROUP BY col1, col2
GROUP BY 1, 2
;


SELECT
	COUNT(*),
	SUM(sb.margin_gross_of_toms_gbp_constant_currency),
	AVG(sb.margin_gross_of_toms_gbp_constant_currency)
FROM se.data.se_booking sb
;

SELECT
	sb.booking_status,
	COUNT(*),
	COUNT(sb.booking_completed_date),
	COUNT(DISTINCT sb.booking_completed_date)
-- 	SUM(sb.margin_gross_of_toms_gbp_constant_currency),
-- 	AVG(sb.margin_gross_of_toms_gbp_constant_currency)
FROM se.data.se_booking sb
GROUP BY 1
;

------------------------------------------------------------------------------------------------------------------------
/*
homework 3:
- Task 1: aggregate the fact_complete_booking table to show me bookings and margin by territory for the last week.
- Task 2: split task 1 down further by adding booking_completed_date
 */


SELECT
	column1,
	column2,
	column3,
	column4,
	column5::DATE                      AS column5,
	column1 + column2                  AS col1_plus_col2,
	column1 - column2                  AS col1_minus_col2,
	column1 * column2                  AS col1_times_col2,
	column1 / column2                  AS col1_divide_col2,
	GREATEST(column1, column2)         AS greatest_col1_or_col2,
	CONCAT(column1, column2)           AS concat_col1_and_col2,
	CONCAT(column1, column2, column3)  AS concat_col1_and_col2_and_col3,
	column3 || column4                 AS pipe_concat_col3_and_col4,
	DATEADD('month', 1, column5::DATE) AS date_add_1_month_col5,
	DATEADD('day', -1, CURRENT_DATE)   AS yesterday

FROM
VALUES (10, 11, 'ABC', 'red', '2023-01-01'),
	   (35, 2345, 'BCD', 'blue', '2023-01-05'),
	   (30, 58, 'XYZ', 'yellow', '2023-01-01'),
	   (15, 232, 'XYZ', 'yellow', '2023-02-11'),
	   (70, 45, 'THZ', 'yellow', '2023-06-08'),
	   (30, 56, 'FYI', 'yellow', '2023-03-06')

------------------------------------------------------------------------------------------------------------------------

SELECT *
FROM se.data.fact_booking fb
WHERE fb.booking_created_date >= CURRENT_DATE - 1 AND
	  (
				  fb.booking_status = 'COMPLETE'
			  OR fb.device_platform = 'web'
			  OR fb.device_platform = 'A58644'
		  )
;

------------------------------------------------------------------------------------------------------------------------
SELECT
	column3,
	IFF(column3 = 'ABC', 'Yes', 'No')                                   AS column_3_has_abc,
	IFF(column3 = 'ABC', 'Hell Yes', IFF(column3 = 'XYZ', 'Yes', 'No')) AS column_3_has_abc_or_xyz,
	IFF(column3 = 'XYZ', 'Yes', IFF(column3 = 'ABC', 'Hell Yes', 'No')) AS column_3_has_abc_or_xyz,
	CASE
		WHEN column3 = 'ABC' THEN 'Hell Yes'
		WHEN column3 = 'XYZ' THEN 'Yes'
		ELSE 'No'
	END                                                                 AS column3_has_abc_or_xyz_using_case

FROM
VALUES (10, 11, 'ABC', 'red', '2023-01-01'),
	   (35, 2345, 'BCD', 'blue', '2023-01-05'),
	   (30, 58, 'XYZ', 'yellow', '2023-01-01'),
	   (15, 232, 'XYZ', 'yellow', '2023-02-11'),
	   (70, 45, 'THZ', 'yellow', '2023-06-08'),
	   (30, 56, 'FYI', 'yellow', '2023-03-06')

------------------------------------------------------------------------------------------------------------------------

-- Homework 3: create a case when statement on the current affiliate territory within the se_user_attributes table. That groups territories in the following way,
-- UK
-- DACH
-- EUROPE OTHER
-- ASIA
-- ROW

SELECT
	sua.current_affiliate_territory,
	CASE
		WHEN sua.current_affiliate_territory = 'UK' THEN 'UK'
		WHEN sua.current_affiliate_territory IN ('DE', 'AT', 'CH') THEN 'DACH'
		WHEN sua.current_affiliate_territory IN ('BE', 'SG', 'ES', 'NL') THEN 'OTHER EUROPE'
-- 					ELSE 'ROW'
	END AS territory_grouping
FROM se.data.se_user_attributes sua
;


SELECT
	ct.territory_category,
	COUNT(*)
FROM (
	SELECT
		sua.current_affiliate_territory,
		CASE
			WHEN sua.current_affiliate_territory = 'UK' THEN 'UK'
			WHEN sua.current_affiliate_territory = 'Gaurdian - UK' THEN 'UK'
			WHEN sua.current_affiliate_territory IN ('DE', 'CH', 'AT') THEN 'DACH'
			WHEN sua.current_affiliate_territory IN ('PH', 'MY', 'HK') THEN 'ASIA'
			WHEN sua.current_affiliate_territory IN ('HU', 'IT', 'DK', 'CZ', 'PL', 'BE', 'NO', 'FR', 'SE', 'ES', 'IE')
				THEN 'EUROPE_OTHER'
			ELSE 'ROW'
		END AS territory_category
	FROM se.data.se_user_attributes AS sua
) AS ct
GROUP BY 1
;


WITH
	user_categorised_territores AS (
		SELECT
			sua.current_affiliate_territory,
			CASE
				WHEN sua.current_affiliate_territory = 'UK' THEN 'UK'
				WHEN sua.current_affiliate_territory = 'Gaurdian - UK' THEN 'UK'
				WHEN sua.current_affiliate_territory IN ('DE', 'CH', 'AT') THEN 'DACH'
				WHEN sua.current_affiliate_territory IN ('PH', 'MY', 'HK') THEN 'ASIA'
				WHEN sua.current_affiliate_territory IN
					 ('HU', 'IT', 'DK', 'CZ', 'PL', 'BE', 'NO', 'FR', 'SE', 'ES', 'IE')
					THEN 'EUROPE_OTHER'
				ELSE 'ROW'
			END AS territory_category
		FROM se.data.se_user_attributes AS sua
	),
	user_agg AS (
		SELECT
			ct.territory_category,
			COUNT(*) AS users
		FROM user_categorised_territores ct
		GROUP BY 1
	),
	sale_categorised_territories AS (
		SELECT
			ds.posa_territory,
			CASE
				WHEN ds.posa_territory = 'UK' THEN 'UK'
				WHEN ds.posa_territory = 'Gaurdian - UK' THEN 'UK'
				WHEN ds.posa_territory IN ('DE', 'CH', 'AT') THEN 'DACH'
				WHEN ds.posa_territory IN ('PH', 'MY', 'HK') THEN 'ASIA'
				WHEN ds.posa_territory IN
					 ('HU', 'IT', 'DK', 'CZ', 'PL', 'BE', 'NO', 'FR', 'SE', 'ES', 'IE')
					THEN 'EUROPE_OTHER'
				ELSE 'ROW'
			END AS territory_category
		FROM se.data.dim_sale ds
		WHERE ds.data_model = 'New Data Model'
		  AND ds.sale_active
	),
	sale_agg AS (
		SELECT
			sct.territory_category,
			COUNT(*) AS sales
		FROM sale_categorised_territories sct
		GROUP BY 1
	)
SELECT
	ua.territory_category,
	ua.users,
	sa.sales
FROM user_agg ua
	LEFT JOIN sale_agg sa ON ua.territory_category = sa.territory_category
;


/*
<query 3 FROM (
	<query 2 FROM (
		<query 1 FROM table>
	)
)
*/


/*
WITH cte_name1 AS (
	query 1 FROM table
), cte_name2 AS (
	query 2 FROM cte_name1
)
query 3
FROM cte_name2
LEFT JOIN cte_name2 ON x=y

*/

SELECT
	sua.current_affiliate_territory,
	COUNT(DISTINCT booking_id) AS bookings
FROM se.data.fact_booking fb
	INNER JOIN se.data.se_user_attributes sua ON fb.shiro_user_id = sua.shiro_user_id
GROUP BY 1
;

USE WAREHOUSE pipe_large
;

WITH
	bookings AS (
		SELECT
			fcb.booking_completed_date     AS date,
			COUNT(DISTINCT fcb.booking_id) AS bookings
		FROM se.data.fact_complete_booking fcb
		WHERE fcb.booking_completed_date >= CURRENT_DATE - 7
		GROUP BY 1
	),
	spvs AS (
		SELECT
			sts.event_tstamp::DATE         AS date,
			COUNT(DISTINCT sts.event_hash) AS spvs
		FROM se.data.scv_touched_spvs sts
		WHERE sts.event_tstamp >= CURRENT_DATE - 7
		GROUP BY 1
	)
SELECT
	b.date,
	b.bookings,
	s.spvs,
	b.bookings / s.spvs AS spv_cvr
FROM bookings b
	LEFT JOIN spvs s ON b.date = s.date
;


------------------------------------------------------------------------------------------------------------------------

-- data objects

-- sale

-- dim sale - list of every sale on every platform ever in SE group
SELECT *
FROM se.data.dim_sale ds
;

-- se_sale_attributes, tb_offer, tvl_sale and chiasma sales make dim_sale

SELECT *
FROM se.data.se_sale_attributes ssa
;

SELECT *
FROM se.data.tb_offer t
;


-- union all
SELECT
	fcb.booking_completed_date::DATE AS date,
	COUNT(*)                         AS bookings
FROM se.data.fact_complete_booking fcb
WHERE fcb.booking_completed_date = CURRENT_DATE - 1
GROUP BY 1

UNION ALL

SELECT
	fcb.booking_completed_date::DATE AS date,
	COUNT(*)                         AS bookings
FROM se.data.fact_complete_booking fcb
WHERE fcb.booking_completed_date >= CURRENT_DATE - 2
GROUP BY 1
;


-- union
SELECT
	fcb.booking_completed_date::DATE AS date,
	COUNT(*)                         AS bookings
FROM se.data.fact_complete_booking fcb
WHERE fcb.booking_completed_date = CURRENT_DATE - 1
GROUP BY 1

UNION

SELECT
	fcb.booking_completed_date::DATE AS date,
	COUNT(*)                         AS bookings
FROM se.data.fact_complete_booking fcb
WHERE fcb.booking_completed_date >= CURRENT_DATE - 2
GROUP BY 1

------------------------------------------------------------------------------------------------------------------------

SELECT *
FROM se.data.dim_sale ds
;

SELECT DISTINCT
	booking_status,
	fb.tech_platform,
	fb.booking_status_type
FROM se.data.fact_booking fb
WHERE fb.tech_platform IN ('TRAVELBIRD', 'SECRET_ESCAPES')
  AND fb.booking_status_type = 'live'
;

SELECT *
FROM se.data.fact_complete_booking fcb
;


SELECT *
FROM se.data.fact_complete_booking fb
;


SELECT *
FROM se.data.se_booking sb
;

SELECT *
FROM se.data.tb_booking tb
;


--- continue on the types of fields within booking tables.

-- financial fields we care about
-- gross revenue
-- margin gross of toms vat
-- margin constant currency
-- dates
-- --  booking created
-- --  booking completed
-- --  booking check in
-- --  booking check out
-- currency
-- device platform
-- travel type calc of posa territory in comparison to posu country

SELECT
	-- user data
	shiro_user_id                 AS shiro_user_id,
	SHA2(LOWER(TRIM(email)), 256) AS hashed_email_address
FROM data_vault_mvp.dwh.user_attributes
WHERE email_opt_in_status != 'opted out'
  AND member_original_affiliate_classification NOT IN ('PARTNER', 'PARTNER_WHITE_LABEL')
  AND email IS NOT NULL
  AND email NOT LIKE '%delete%'
  AND membership_account_status = 'FULL_ACCOUNT';
