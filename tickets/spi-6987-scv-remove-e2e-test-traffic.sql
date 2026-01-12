SELECT
	stba.touch_hostname_territory,
	COUNT(*)
FROM se.data.scv_touch_basic_attributes stba
GROUP BY 1

/*
 TOUCH_HOSTNAME_TERRITORY	COUNT(*)
PP	522124
JP	310
ZM	28
PK	118
GM	5
TT	15
BZ	2
TL	1
SE TECH	1952879
LU	3375
AI	123
NZ	497
LT	144
NG	179
TH	254
MN	13
GP	7
DE	546616156
NL	28364941
MT	218
LV	63
SS	4
MC	42
CV	9
GY	6
TO	1
ID	123274
GI	350
FO	31
AW	76
IO	33
FR	13919981
AL	693
RO	796
PH	75
DZ	422
CY	138
PA	17
001	50
BF	13
AC	6
BM	14
DEFAULT	1
TD	1
SH	2
PR	92
OM	76
HU	6683
MK	54
ML	4
PN	4
TG	2
IC	120
PF	12
MV	51
GU	13
NE	2
CG	1
EG	309
SI	300
VA	10
CU	71
EH	4
BI	17
419	5
BA	166
FJ	30
LY	21
RE	3
website-new	1
DK	8385605
CA	1181
AZ	42
UY	41
CI	5
JM	82
HK	186608
HR	360
VN	157
PT	708
AQ	159
JO	51
IM	378
BD	55
EC	23
AG	37
NR	3
GQ	3
CF	10
SY	2
MY	140745
YE	7
ER	37
IR	138
BE	10605594
SG	422901
IQ	67
SV	9
KH	22
GT	11
PW	6
AT	473154
UM	240
MA	83
KG	7
CX	9
ET	59
pl	121
MO	9
SO	3
IE	1915180
AU	3060
CZ	991
BR	510
LC	97
BY	47
PS	5
MG	10
PL	69023858
SA	525
CL	119
JE	725
GD	21
NP	30925
VU	39
UG	22
Guardian - UK	9
RW	5
UZ	17
HM	2
EA	3
""	2722
CR	69
CM	20
MW	7
GW	8
TF	2
KR	102
ios_app	29
SZ	4
TC	5
IN	613
GH	46
SX	2
SN	11
TB-BE_FR	9
TA	4
NC	1
BG	369
AS	486
EE	213
AR	751
RU	1146
UK_BLOG	308
AD	363
BH	92
BS	53
MQ	35
SM	63
WF	26
PM	18
KM	8
GL	1
UA	654
XK	31
TP	979
150	86
ME	14
FK	25
AM	52
SJ	16
GF	2
BN	2
ES	4347381
NON_VERIFIED	97453
RS	161
GE	96
TR	778
BV	3
Other	307103
AE	1182
NA	206
BT	11
KZ	62
SL	12
US	6262835
MD	104
IL	184
SC	22
DG	48
DJ	13
15	18
LA	18
CW	2
IT	72461760
ZA	733
QA	111
CK	13
SR	1
travelistpl	4
SE	28761854
ANOMALOUS	1664743
SK	7612
MP	119042
KW	102
VI	229
TZ	25
MF	8
BW	7
ZW	38
BO	12
PG	3
MR	2
MX	248
	54844
LB	59
LI	270
DO	125
LK	42
VG	76
BJ	32
PE	35
NI	4
HT	3
GN	2
CD	2
UK	384541889
FI	195
TN	48
KE	154
SB	30
GA	52
BQ	4
CP	5
CH	7625796
AX	791
TB-NL	107
CN	1351
VE	12
AF	341
KY	44
BB	56
PY	23
MZ	18
WS	1
NO	4283416
GR	675
TW	156
GG	450
DM	79
MU	26
TB-BE_NL	219
AO	255
IS	221
CO	106
HN	11
SD	5
NU	3
TM	5

 */


USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS hygiene_vault_mvp_dev_robin.snowplow
;

CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream
	CLONE hygiene_vault_mvp.snowplow.event_stream
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
	CLONE data_vault_mvp.single_customer_view_stg.module_touchification
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_anomalous_user_dates
	CLONE data_vault_mvp.single_customer_view_stg.module_anomalous_user_dates
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchifiable_events
	CLONE data_vault_mvp.single_customer_view_stg.module_touchifiable_events
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_events_of_interest
	CLONE data_vault_mvp.single_customer_view_stg.module_events_of_interest
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.page_screen_enrichment
	CLONE data_vault_mvp.single_customer_view_stg.page_screen_enrichment
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_attributes
	CLONE data_vault_mvp.dwh.user_attributes
;

-- optional statement to create the module target table --
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
;

/*
self_describing_task \
    --include 'biapp.task_catalogue.dv.dwh.scv.05_touch_basic_attributes.01_module_touch_basic_attributes.py' \
    --method 'run' \
    --start '2024-12-01 00:00:00' \
    --end '2024-12-01 00:00:00'

 */


SELECT *
FROM se.data.scv_touch_basic_attributes stba
WHERE stba.touch_se_brand = 'Travelist' AND
	  stba.touch_start_tstamp >= CURRENT_DATE - 1
	https://travelist.pl/117828/polska-wybrzeze-ustka-grand-lubicz/?is_tvl_se_tech=1
;



SELECT
	CASE
		WHEN is_non_verified_user_session -- first all non verified users should be set to the territory non verified
			THEN 'NON_VERIFIED'
		WHEN
			is_anomalous_user_session -- all anomalous users should be set to anomalous territory
			THEN 'ANOMALOUS'
		WHEN
			se.data.wrd_spv_territory(touch_hostname, se_action) IS NOT NULL
			THEN se.data.wrd_spv_territory(touch_hostname, se_action) -- attribute territory based on wrd udf
		WHEN
			LOWER(md.touch_hostname) REGEXP
			'(.*(web|db-loadtesting|sandbox).*\\\\.secretescapes\\\\.com|.*\\\\.fs-staging\\\\.escapes\\\\.tech|[0-9]{{{{1,3}}}}\\\\.[0-9]{{{{1,3}}}}\\\\.[0-9]{{{{1,3}}}}\\\\.[0-9]{{{{1,3}}}}|.*\\\\.amazonaws\\\\.com)'
				OR
				-- logic for a pen test that took place to test the web application against potential attacks: https://secretescapes.atlassian.net/browse/SPI-3662
			user_ipaddress REGEXP
			'(52\\\\.205\\\\.190\\\\.([1-9]|[1-9][0-9]|[1-2][0-5][0-4]))|(35\\\\.245\\\\.67\\\\.(225|226))|(34\\\\.145\\\\.238\\\\.([1-9]|[1-9][0-9]|[1-2][0-5][0-4]))'
				OR
			LOWER(md.touch_hostname) IN (
										 'admin.travelist.pl',
										 'api.secretescapes.com',
										 'applitool-affiliate.secretescapes.com',
										 'applitools-whitelabel.secretescapes.com',
										 'cdn.secretescapes.com',
										 'click.ebm.secretescapes.com',
										 'click.email.secretescapes.com',
										 'dev.secretescapes.com',
										 'flights.secretescapes.com',
										 'livetest.oferty.travelist.pl',
										 'mobile-staging.secretescapes.com',
										 'staging.secretescapes.com',
										 'staging01.secretescapes.com',
										 'staging02.secretescapes.com',
										 'staging.travelist.pl',
										 'tracker.secretescapes.com'
				)
				OR md.page_load_testing
				OR is_app_background_session = TRUE
				OR is_solo_push_open_session = TRUE
				OR sua.is_test_user = TRUE
				OR
			PARSE_URL(md.touch_landing_page, 1)['parameters']['is_tvl_se_tech']::VARCHAR IS NOT NULL
			THEN 'SE TECH'
	END AS posa_territory,
	md.*

FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes_clone__model_data md
	INNER JOIN se.data.se_user_attributes sua
			   ON md.attributed_user_id = sua.shiro_user_id::VARCHAR AND sua.is_test_user
				   AND stitched_identity_type = 'se_user_id'
				   AND touch_start_tstamp >= CURRENT_DATE - 1
;


DROP TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
;


SELECT
	touch_landing_page,
	PARSE_URL(touch_landing_page, 1)['parameters']['affiliate']::VARCHAR
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
;



SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
	INNER JOIN se.data.se_user_attributes sua
			   ON mtba.attributed_user_id = sua.shiro_user_id::VARCHAR AND sua.is_test_user
WHERE mtba.stitched_identity_type = 'se_user_id'