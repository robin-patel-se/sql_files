//https://secretescapes.atlassian.net/browse/SPI-5029

/*
 Customer Email
Merchant Order ID
Total
Currency
Order Date String
 */


-- 	Going to follow MentionMe process for hashing emails
-- https://help.mention-me.com/hc/en-gb/articles/12351227476381-Hashing-historical-customer-emails

/*
Lower case the email address
Append the secret key provided by Mention Me
Hash the combined email address and key using the SHA-256 hash algorithm
Ensure the output hash is lower case
 */

SELECT
	LOWER(SHA2(LOWER(sua.email) || 'replace with mention me secret key')) AS customer_email,
	fcb.transaction_id                                                    AS merchant_order_id,
	fcb.gross_revenue_cc                                                  AS total,
	fcb.currency,
	fcb.booking_completed_timestamp::timestamp_tz                         AS order_date_string
FROM se.data.fact_complete_booking fcb
	INNER JOIN se.data_pii.se_user_attributes sua ON fcb.shiro_user_id = sua.shiro_user_id
WHERE fcb.booking_completed_date >= DATEADD(YEAR, -3, CURRENT_DATE)
;



