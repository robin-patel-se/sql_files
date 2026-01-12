https://eblik.pl/ 18767
https://e.blik.com/ 12612
https://secure.payu.com/ 9233
https://auth.pkobp.pl/ 3400
https://login.ingbank.pl/ 2415
https://www.centrum24.pl/ 2304
https://online.mbank.pl/ 2077
https://credit.payu.com/ 1911
https://api.pekao24.pl/ 1368
https://psd2.bankmillennium.pl/ 1280
https://system.aliorbank.pl/ 621
https://ca24.credit-agricole.pl/ 431
https://www.citibankonline.pl/ 342
https://auth.inteligo.pl/ 183
https://secure.velobank.pl/ 120
https://epayments.bnpparibas.pl/ 73
https://interpay.pkobp.pl/ 72
https://login.nestbank.pl/ 56
https://www.platnosci.pekao24.pl/ 53
https://pbn.paybynet.com.pl/ 44
http://m.facebook.com/ 39
https://www.ipko.pl/ 25
https://mtransfer.mbank.pl/ 24
https://ingbusiness.pl/ 22
https://bn.aliorbank.pl/ 21
https://bosbank24.pl/ 18
https://plusbank24.pl/ 17
https://www.pekaobiznes24.pl/ 13
https://e-bsjastrzebie.cui.pl/ 9
https://e-bank.lubuskibs.pl/ 9
https://bswschowa24.pl/ 8
https://www.bankmillennium.pl/ 6
https://ebsd.pl/ 6
https://ebp.bsolesnica.net/ 5
https://ebo.bsjarocin.pl/ 3
https://ebo.bslesnica.pl/ 3
https://login.gbsbank.pl/ 3
https://ebo.bspawlowice.pl/ 3
https://systemkantor.aliorbank.pl/ 3
https://inteligo.pl/ 2
https://ebp.bsplonsk.pl/ 2
http://baidu.com/ 2
https://bslubniany.cui.pl/ 2
https://ebo.bs-suchedniow.com.pl/ 1
https://ebobank.bsjl.pl/ 1
https://ebo.bsrymanow.pl/ 1
https://ib.bsmiedzna.pl/ 1
https://bank24.bsbrodnica.pl/ 1
https://e-bsjaroslaw.cui.pl/ 1
https://net-bank.bszgierz.pl/ 1
https://ilowabank.cui.pl/ 1
https://ebank.bsszczytno.pl/ 1
https://ebo.bskonskie.pl/ 1
https://bslubartow24.pl/ 1
https://online.bankbps.pl/ 1


SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_url_hostname muh



SELECT
	column1                         AS url,
	PARSE_URL(url)['host']::VARCHAR AS hostname,

-- Travelist payment providers
	hostname = ('e.blik.com') OR
	hostname = ('eblik.pl') OR
	hostname LIKE ('%payu.com') OR
	hostname = ('auth.pkobp.pl') OR
	hostname LIKE ('%pekao24.pl') OR
	hostname = ('www.centrum24.pl') OR
	hostname = ('psd2.bankmillenium.pl') OR
	hostname LIKE ('%inteligo.pl') OR
	hostname = ('ca24.credit-agricole.pl') OR
	hostname = ('pbn.paybynet.com.pl') OR
	hostname LIKE ('%bnpparibas.pl') OR
	hostname = ('login.nestbank.pl') OR
	hostname LIKE ('%mbank.pl') OR
	hostname = ('login.ingbank.pl') OR
	hostname LIKE ('%aliorbank.pl') OR
	hostname LIKE ('%citibankonline.pl')
		OR

-- new
	hostname LIKE ('%bankmillennium.pl') OR
	hostname LIKE ('%velobank.pl') OR
	hostname = ('interpay.pkobp.pl') OR
	hostname = ('www.ipko.pl') OR
	hostname = ('ingbusiness.pl') OR
	hostname = ('bosbank24.pl') OR
	hostname = ('plusbank24.pl') OR
	hostname = ('www.pekaobiznes24.pl') OR
	hostname LIKE ('%.cui.pl') OR
	hostname LIKE ('%.lubuskibs.pl') OR
	hostname = ('bswschowa24.pl') OR
	hostname = ('www.bankmillennium.pl') OR
	hostname = ('ebsd.pl') OR
	hostname = ('ebp.bsolesnica.net') OR
	hostname = ('ebo.bsjarocin.pl') OR
	hostname = ('ebo.bslesnica.pl') OR
	hostname = ('login.gbsbank.pl') OR
	hostname = ('ebo.bspawlowice.pl') OR
	hostname = ('ebp.bsplonsk.pl') OR
	hostname = ('bslubniany.cui.pl') OR
	hostname LIKE ('%.bs-suchedniow.com.pl') OR
	hostname = ('ebobank.bsjl.pl') OR
	hostname = ('ebo.bsrymanow.pl') OR
	hostname = ('ib.bsmiedzna.pl') OR
	hostname LIKE ('%.bsbrodnica.pl') OR
	hostname = ('e-bsjaroslaw.cui.pl') OR
	hostname = ('net-bank.bszgierz.pl') OR
	hostname = ('ilowabank.cui.pl') OR
	hostname = ('ebank.bsszczytno.pl') OR
	hostname = ('ebo.bskonskie.pl') OR
	hostname = ('bslubartow24.pl') OR
	hostname = ('online.bankbps.pl')
FROM
VALUES ('https://eblik.pl/'),
	   ('https://e.blik.com/'),
	   ('https://secure.payu.com/'),
	   ('https://auth.pkobp.pl/'),
	   ('https://login.ingbank.pl/'),
	   ('https://www.centrum24.pl/'),
	   ('https://online.mbank.pl/'),
	   ('https://credit.payu.com/'),
	   ('https://api.pekao24.pl/'),
	   ('https://psd2.bankmillennium.pl/'),
	   ('https://system.aliorbank.pl/'),
	   ('https://ca24.credit-agricole.pl/'),
	   ('https://www.citibankonline.pl/'),
	   ('https://auth.inteligo.pl/'),
	   ('https://secure.velobank.pl/'),
	   ('https://epayments.bnpparibas.pl/'),
	   ('https://interpay.pkobp.pl/'),
	   ('https://login.nestbank.pl/'),
	   ('https://www.platnosci.pekao24.pl/'),
	   ('https://pbn.paybynet.com.pl/'),
	   ('http://m.facebook.com/'),
	   ('https://www.ipko.pl/'),
	   ('https://mtransfer.mbank.pl/'),
	   ('https://ingbusiness.pl/'),
	   ('https://bn.aliorbank.pl/'),
	   ('https://bosbank24.pl/'),
	   ('https://plusbank24.pl/'),
	   ('https://www.pekaobiznes24.pl/'),
	   ('https://e-bsjastrzebie.cui.pl/'),
	   ('https://e-bank.lubuskibs.pl/'),
	   ('https://bswschowa24.pl/'),
	   ('https://www.bankmillennium.pl/'),
	   ('https://ebsd.pl/'),
	   ('https://ebp.bsolesnica.net/'),
	   ('https://ebo.bsjarocin.pl/'),
	   ('https://ebo.bslesnica.pl/'),
	   ('https://login.gbsbank.pl/'),
	   ('https://ebo.bspawlowice.pl/'),
	   ('https://systemkantor.aliorbank.pl/'),
	   ('https://inteligo.pl/'),
	   ('https://ebp.bsplonsk.pl/'),
	   ('http://baidu.com/'),
	   ('https://bslubniany.cui.pl/'),
	   ('https://ebo.bs-suchedniow.com.pl/'),
	   ('https://ebobank.bsjl.pl/'),
	   ('https://ebo.bsrymanow.pl/'),
	   ('https://ib.bsmiedzna.pl/'),
	   ('https://bank24.bsbrodnica.pl/'),
	   ('https://e-bsjaroslaw.cui.pl/'),
	   ('https://net-bank.bszgierz.pl/'),
	   ('https://ilowabank.cui.pl/'),
	   ('https://ebank.bsszczytno.pl/'),
	   ('https://ebo.bskonskie.pl/'),
	   ('https://bslubartow24.pl/'),
	   ('https://online.bankbps.pl/')
;


SELECT
	PARSE_URL(stba.touch_referrer_url, 1)['host']::VARCHAR AS host,
	COUNT(*)                                               AS sessions
FROM se.data_pii.scv_touch_basic_attributes stba
WHERE stba.touch_se_brand IS DISTINCT FROM 'SE Brand'
GROUP BY 1
ORDER BY 2 DESC
;


SELECT
	COUNT(*) AS sessions
FROM se.data_pii.scv_touch_basic_attributes stba
WHERE stba.touch_se_brand IS DISTINCT FROM 'SE Brand'


------------------------------------------------------------------------------------------------------------------------

USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_unique_urls
	CLONE data_vault_mvp.single_customer_view_stg.module_unique_urls
;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_url_hostname
	CLONE data_vault_mvp.single_customer_view_stg.module_url_hostname
;

self_describing_task
\
    --include 'biapp.task_catalogue.dv.dwh.scv.01_url_manipulation.02_01_module_url_hostname.py' \
    --method 'run' \
    --start '2025-01-06 00:00:00' \
    --end '2025-01-06 00:00:00'



SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_url_hostname
;

CREATE OR REPLACE SCHEMA data_vault_mvp.single_customer_view_stg_20250109 CLONE data_vault_mvp.single_customer_view_stg
;

------------------------------------------------------------------------------------------------------------------------

USE ROLE pipelinerunner
;

GRANT USAGE ON SCHEMA data_vault_mvp.single_customer_view_stg_20250113 TO ROLE data_team_basic
;

GRANT SELECT ON ALL TABLES IN SCHEMA data_vault_mvp.single_customer_view_stg_20250113 TO ROLE data_team_basic
;

CREATE OR REPLACE SCHEMA data_vault_mvp.single_customer_view_stg_20250113 CLONE data_vault_mvp.single_customer_view_stg
;

DROP TABLE data_vault_mvp.single_customer_view_stg.module_url_hostname
;

SELECT
	url_hostname,
	COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_url_hostname
WHERE url_medium = 'payment_gateway'
GROUP BY 1

------------------------------------------------------------------------------------------------------------------------

WITH
	ref_hostname AS (
		SELECT
			PARSE_URL(stba.touch_referrer_url, 1)['host']::VARCHAR AS referrer_hostname,
			*
		FROM se.data.scv_touch_basic_attributes stba
		WHERE stba.touch_se_brand = 'Travelist'
		  AND (
			referrer_hostname LIKE ('%bankmillennium.pl') OR
			referrer_hostname LIKE ('%velobank.pl') OR
			referrer_hostname = ('interpay.pkobp.pl') OR
			referrer_hostname = ('www.ipko.pl') OR
			referrer_hostname = ('ingbusiness.pl') OR
			referrer_hostname = ('bosbank24.pl') OR
			referrer_hostname = ('plusbank24.pl') OR
			referrer_hostname = ('www.pekaobiznes24.pl') OR
			referrer_hostname LIKE ('%.cui.pl') OR
			referrer_hostname LIKE ('%.lubuskibs.pl') OR
			referrer_hostname = ('bswschowa24.pl') OR
			referrer_hostname = ('www.bankmillennium.pl') OR
			referrer_hostname = ('ebsd.pl') OR
			referrer_hostname = ('ebp.bsolesnica.net') OR
			referrer_hostname = ('ebo.bsjarocin.pl') OR
			referrer_hostname = ('ebo.bslesnica.pl') OR
			referrer_hostname = ('login.gbsbank.pl') OR
			referrer_hostname = ('ebo.bspawlowice.pl') OR
			referrer_hostname = ('ebp.bsplonsk.pl') OR
			referrer_hostname = ('bslubniany.cui.pl') OR
			referrer_hostname LIKE ('%.bs-suchedniow.com.pl') OR
			referrer_hostname = ('ebobank.bsjl.pl') OR
			referrer_hostname = ('ebo.bsrymanow.pl') OR
			referrer_hostname = ('ib.bsmiedzna.pl') OR
			referrer_hostname LIKE ('%.bsbrodnica.pl') OR
			referrer_hostname = ('e-bsjaroslaw.cui.pl') OR
			referrer_hostname = ('net-bank.bszgierz.pl') OR
			referrer_hostname = ('ilowabank.cui.pl') OR
			referrer_hostname = ('ebank.bsszczytno.pl') OR
			referrer_hostname = ('ebo.bskonskie.pl') OR
			referrer_hostname = ('bslubartow24.pl') OR
			referrer_hostname = ('online.bankbps.pl')
			)
	)
SELECT
	rh.referrer_hostname,
COUNT(*)
FROM ref_hostname rh
GROUP BY 1

/*
REFERRER_HOSTNAME	COUNT(*)
psd2.bankmillennium.pl	1833
secure.velobank.pl	180
interpay.pkobp.pl	84
ingbusiness.pl	42
bosbank24.pl	38
www.ipko.pl	25
bswschowa24.pl	19
www.pekaobiznes24.pl	19
plusbank24.pl	18
ebp.bsolesnica.net	12
login.gbsbank.pl	10
e-bank.lubuskibs.pl	6
ebo.bsjarocin.pl	6
ebsd.pl	6
ebp.bsplonsk.pl	5
www.bankmillennium.pl	5
ebo.bslesnica.pl	3
ebo.bspawlowice.pl	2
ebobank.bsjl.pl	2
bslubniany.cui.pl	2
e-bsjastrzebie.cui.pl	2
bsnamyslow.cui.pl	2
ebo.bs-suchedniow.com.pl	1
ebo.bsrymanow.pl	1
e-bsjaroslaw.cui.pl	1
online.bankbps.pl	1
net-bank.bszgierz.pl	1
bslubartow24.pl	1
ib.bsmiedzna.pl	1
bank24.bsbrodnica.pl	1
bskrotoszyn24.cui.pl	1
ilowabank.cui.pl	1
ebo.bskonskie.pl	1
ebank.bsszczytno.pl	1


2333 sessions with a referrer in this list
*/

SELECT
	stba.touch_se_brand,
	COUNT(*)
FROM se.data_pii.scv_touch_basic_attributes stba
GROUP BY 1

/*
TOUCH_SE_BRAND	COUNT(*)
SE Brand	1,120,113,515
Travelist	68,138,395
*/

/*
TOUCH_SE_BRAND	COUNT(*)
SE Brand	1,120,152,684
Travelist	68,150,243
*/
