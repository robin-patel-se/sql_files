USE ROLE accountadmin
;

SELECT *

FROM snowflake.account_usage.login_history

WHERE client_ip IN (
					'104.223.91.28',
					'198.54.135.99',
					'184.147.100.29',
					'146.70.117.210',
					'198.54.130.153',
					'169.150.203.22',
					'185.156.46.163',
					'146.70.171.99',
					'206.217.206.108',
					'45.86.221.146',
					'193.32.126.233',
					'87.249.134.11',
					'66.115.189.247',
					'104.129.24.124',
					'146.70.171.112',
					'198.54.135.67',
					'146.70.124.216',
					'45.134.142.200',
					'206.217.205.49',
					'146.70.117.56',
					'169.150.201.25',
					'66.63.167.147',
					'194.230.144.126',
					'146.70.165.227',
					'154.47.30.137',
					'154.47.30.150',
					'96.44.191.140',
					'146.70.166.176',
					'198.44.136.56',
					'176.123.6.193',
					'192.252.212.60',
					'173.44.63.112',
					'37.19.210.34',
					'37.19.210.21',
					'185.213.155.241',
					'198.44.136.82',
					'93.115.0.49',
					'204.152.216.105',
					'198.44.129.82',
					'185.248.85.59',
					'198.54.131.152',
					'102.165.16.161',
					'185.156.46.144',
					'45.134.140.144',
					'198.54.135.35',
					'176.123.3.132',
					'185.248.85.14',
					'169.150.223.208',
					'162.33.177.32',
					'194.230.145.67',
					'5.47.87.202',
					'194.230.160.5',
					'194.230.147.127',
					'176.220.186.152',
					'194.230.160.237',
					'194.230.158.178',
					'194.230.145.76',
					'45.155.91.99',
					'194.230.158.107',
					'194.230.148.99',
					'194.230.144.50',
					'185.204.1.178',
					'79.127.217.44',
					'104.129.24.115',
					'146.70.119.24',
					'138.199.34.144'
	)

ORDER BY event_timestamp
;

SELECT *
FROM snowflake.account_usage.sessions
WHERE PARSE_JSON(client_environment):APPLICATION = 'rapeflake' OR
	  (
		  PARSE_JSON(client_environment):APPLICATION = 'DBeaver_DBeaverUltimate'
			  AND
		  PARSE_JSON(client_environment):OS = 'Windows Server 2022'
		  )
ORDER BY created_on
;


SELECT *
FROM snowflake.account_usage.users u
WHERE u.disabled = 'false'
  AND u.deleted_on IS NULL
;

SELECT *
FROM snowflake.account_usage.login_history lh;

USE WAREHOUSE pipe_2xlarge;

CREATE OR REPLACE TABLE scratch.robinpatel.snowflake_users AS (
	WITH
		login_info AS (
			SELECT
				lh.user_name,
				COUNT(*)                           AS logins,
				SUM(IFF(is_success = 'YES', 1, 0)) AS success_logins,
				SUM(IFF(is_success = 'NO', 1, 0))  AS no_success_logins
			-- 			MAX(event_timestamp)                                AS last_login,
-- 			MAX(IFF(is_success = 'YES', event_timestamp, NULL)) AS last_success_login,
			FROM snowflake.account_usage.login_history lh
			GROUP BY 1

		),
		user_queries AS (
			SELECT
				qh.user_name,
				COUNT(*)                                 AS queries,
				SUM(IFF(qh.query_type = 'SELECT', 1, 0)) AS select_queries
			FROM snowflake.account_usage.query_history qh
			GROUP BY 1
		)
	SELECT
		u.user_id,
		u.name,
		u.created_on,
		u.last_success_login,
		li.logins,
		li.success_logins,
		li.no_success_logins,
		uq.queries,
		uq.select_queries,
		u.has_password,
		u.login_name,
		u.display_name,
		u.email,
		u.snowflake_lock,
		u.default_role,
		u.expires_at,
		u.locked_until_time,
		u.has_rsa_public_key,
		u.password_last_set_time,
		u.owner,
		u.comment,
		u.deleted_on,
		u.disabled
	FROM snowflake.account_usage.users u
		LEFT JOIN login_info li ON u.name = li.user_name
		LEFT JOIN user_queries uq ON u.name = uq.user_name
	WHERE u.disabled = 'false'
	  AND u.deleted_on IS NULL
)
;


/*
Account changes:
deleted
- MARIOMARTINEZ
- MARTAGLOWINSKA - TL
- PAULINAKROL - TL
- EMPATHYMARKETING
- MAKSYMILIAN - left
- FRANCESCOSAMMARTANO - left
- ELENIPASSA
- MARENKNOLL
- SEBASTIANSTASZEWSKI - left
- DAVINFOWLER
- WERONIKAGRABOWICZ - left
- REBECCATOSSELL - left
- LAURAVALENTINE
- NIKOLAFASS
- ERICMOIOLI
- ANNARIEDL
- BARBARAJESSACHER
- MARENLOESCHE
- MONICAZAIA
- VERAKORNIOUKHIN
- MARTABIALKOWSKA
- AMYSEMIKIN
- LIVIABRASILI


sitel users that have a password who have not logged in for a long time and were created a long time ago
- KALOYANERMENKOV
- IVANIMENDES
- CARLOSSANTOS
- NEFTALISANCHEZ
- MARLENEJUSTO
- SAMEERSHARMA
- MARTAZAMPARUTTI
- RAYKORAYKOV
- BLAGOVESTAMARINOVA
- SIRANJEEVIVIVEKANANTHAN
- GONCALOALMENDRA
- GERGANAGUCHKOVA
- DEYANDEYKOV
- PATRICIASANTOS§
- FILIPEPINHO
- VENELINATERZIEVA
- MAUREENPEDRO
- LUISDIAZ
- CESARSILVA


*/


SELECT *
FROM scratch.robinpatel.snowflake_users
;
USE ROLE accountadmin;
SELECT *
FROM snowflake.account_usage.query_history qh
WHERE qh.user_name = 'annacollell'
;


SELECT
	qh.user_name,
	COUNT(*)                                 AS queries,
	SUM(IFF(qh.query_type = 'SELECT', 1, 0)) AS select_queries
FROM snowflake.account_usage.query_history qh
GROUP BY 1




ABANOUBHAKEEM
DONALDGATFIELD
EIRIKPETTERSEN
GIANNIRAFTIS
KIRSTENGRIEVE
PARASTOUABBASI
PAULHARRUP
PIPELINERUNNER
ROBINPATEL
SAURDASH
SE_SNOWF_ADMIN
TOLLYVONDERHEYDE




WITH
		login_info AS (
			SELECT
				lh.user_name,
				COUNT(*)                           AS logins,
				SUM(IFF(is_success = 'YES', 1, 0)) AS success_logins,
				SUM(IFF(is_success = 'NO', 1, 0))  AS no_success_logins
			-- 			MAX(event_timestamp)                                AS last_login,
-- 			MAX(IFF(is_success = 'YES', event_timestamp, NULL)) AS last_success_login,
			FROM snowflake.account_usage.login_history lh
			GROUP BY 1

		),
		user_queries AS (
			SELECT
				qh.user_name,
				COUNT(*)                                 AS queries,
				SUM(IFF(qh.query_type = 'SELECT', 1, 0)) AS select_queries
			FROM snowflake.account_usage.query_history qh
			GROUP BY 1
		)
	SELECT
		u.user_id,
		u.name,
		u.created_on,
		u.last_success_login,
		li.logins,
		li.success_logins,
		li.no_success_logins,
		uq.queries,
		uq.select_queries,
		u.has_password,
		u.login_name,
		u.display_name,
		u.email,
		u.snowflake_lock,
		u.default_role,
		u.expires_at,
		u.locked_until_time,
		u.has_rsa_public_key,
		u.password_last_set_time,
		u.owner,
		u.comment,
		u.deleted_on,
		u.disabled
	FROM snowflake.account_usage.users u
		LEFT JOIN login_info li ON u.name = li.user_name
		LEFT JOIN user_queries uq ON u.name = uq.user_name
-- WHERE li.user_name = 'SE_SNOWF_ADMIN'
	WHERE u.disabled = 'false'
	  AND u.deleted_on IS NULL