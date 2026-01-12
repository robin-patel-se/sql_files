/*

 Secret Escapes - Data Warehouse Engineer Take Home Challenge
 Tomasz Wójcik

 */
/*
 -----------------------------
 1. MODELING
 -----------------------------
 */
/*
 1.1. Bookings
 -------------
 */

DROP TABLE IF EXISTS public.dim_bookings
;

CREATE TABLE IF NOT EXISTS collab.muse_tech_task.tomasz_dim_bookings
(
	booking_id     character varying(32) PRIMARY KEY,
	member_id      character varying(32),
	booking_date   timestamp without time zone,
	last_updated   timestamp without time zone,
	booking_status character varying(16)
)
;

ALTER TABLE IF EXISTS public.dim_bookings OWNER TO postgres
;


INSERT INTO collab.muse_tech_task.tomasz_dim_bookings (
	-- identify duplicates - same data but different "LAST_UPDATED"
	with row_num AS (
            SELECT row_number() OVER (
                    PARTITION BY BOOKING_DATE,
                    MEMBER_ID,
                    BOOKING_ID,
                    BOOKING_DATE,
                    BOOKING_STATUS
                    ORDER BY LAST_UPDATED
                ) AS row_num,
                *
            FROM collab.muse_tech_task.bookings
        ) -- remove duplicated rows
	,
	dedupe AS (
            SELECT *
            FROM row_num
            WHERE row_num = 1
        ) -- set attributes from recent update
        SELECT "BOOKING_ID",
	member_id,
	booking_date,
	last_updated,
	booking_status FROM (
                SELECT *,
                    row_number() OVER (
                        PARTITION BY "BOOKING_ID"
                        ORDER BY "LAST_UPDATED" DESC
                    ) AS status_num
                FROM dedupe
            )
        WHERE status_num = 1)
;

-- deduped bookings

SELECT *
FROM collab.muse_tech_task.bookings b
;

SELECT *
FROM collab.muse_tech_task.tomasz_dim_bookings
;

/*
 1.1. Members
 -------------
 */
DROP TABLE IF EXISTS public.dim_members
;

CREATE TABLE IF NOT EXISTS collab.muse_tech_task.tomasz_dim_members
(
	member_id       character varying(32) PRIMARY KEY,
	sign_up_date    timestamp without time zone,
	last_updated    timestamp without time zone,
	territories     character varying(50),
	member_age_days integer
)
;

INSERT INTO collab.muse_tech_task.tomasz_dim_members (
	-- identify duplicates - same data but different "LAST_UPDATED"
	with row_num AS (
            SELECT row_number() OVER (
                    PARTITION BY MEMBER_ID,
                    SIGN_UP_DATE,
                    ORIGINAL_TERRITORY,
                    CURRENT_TERRITORY
                    ORDER BY LAST_UPDATED
                ) AS row_num,
                *
            FROM collab.muse_tech_task.members
        ) -- remove duplicated rows
	,
	dedupe AS (
            SELECT *
            FROM row_num
            WHERE row_num = 1
        ) -- aggregate anomalies (territories) and remove unnecessary columns
	,
	agg AS (
            SELECT min("ID") AS "ID",
                MEMBER_ID,
                SIGN_UP_DATE,
                LAST_UPDATED,
                LISTAGG(
                    DISTINCT concat(
                        ORIGINAL_TERRITORY,
                        ' -> ',
                        CURRENT_TERRITORY
                    ),
                    ';'
                ) AS TERRITORIES -- remove anomalies
            FROM dedupe
            GROUP BY 2,
                3,
                4
        ) -- define end_date for member_age calculation
	,
	end_date AS (
            SELECT max(EVENT_TSTAMP)::DATE AS end_date
            FROM collab.muse_tech_task.events
        ) -- set first "SIGN_UP_DATE" from recent update and calculate mamber_age
        SELECT "MEMBER_ID",
	sign_up_date,
	last_updated,
	territories,
	(
	SELECT end_date
	FROM end_date) - "SIGN_UP_DATE"::DATE AS member_age
FROM (
	SELECT *, row_number() OVER (
	PARTITION BY MEMBER_ID
	ORDER BY LAST_UPDATED DESC
	) AS sign_up_num
	FROM agg
	)
WHERE sign_up_num = 1
	)
;

SELECT
	COUNT(DISTINCT member_id),
	COUNT(*)
FROM collab.muse_tech_task.members m
;

SELECT
	COUNT(DISTINCT member_id),
	COUNT(*)
FROM collab.muse_tech_task.tomasz_dim_members
;

-- de duplication is spot on
-- interesting approach to territory - probe for context
-- member age has been established as an attribute

/*
 1.1. Events
 -------------
 */

DROP TABLE IF EXISTS public.fct_events
;

CREATE TABLE IF NOT EXISTS collab.muse_tech_task.tomasz_fct_events
(
	event_id                character varying(32) PRIMARY KEY,
	territory               character varying(2),
	cookie_id               character varying(36),
	member_id               character varying(32),
	booking_id              character varying(32),
	event_name              character varying(11),
	event_timestamp         timestamp without time zone,
	page_urlpath            text,
	session_id              character varying(40),
	seconds_from_prev_event numeric,
	session_event_sequence  integer
)
;

ALTER TABLE IF EXISTS public.fct_events OWNER TO postgres
;

-- INSERT INTO collab.muse_tech_task.tomasz_fct_events as
	-- create event_id hashing columns
	with members_in AS (
            SELECT md5(
                    ROW(
                        TERRITORY,
                        COOKIE_ID,
                        MEMBER_ID,
                        BOOKING_ID,
                        EVENT_NAME,
                        EVENT_TSTAMP,
                        PAGE_URLPATH
                    )::VARCHAR
                ) AS event_id,
                *
            FROM collab.muse_tech_task.events
        ) -- identify duplicates - same data but different "EXTRACTED_AT"
	,
	row_num AS (
            SELECT *,
                row_number() OVER (
                    PARTITION BY event_id
                    ORDER BY EXTRACTED_AT ASC
                ) AS row_num
            FROM members_in
        ) -- remove duplicated rows and unnecessary columns
	,
	dedupe AS (
            SELECT event_id,
                TERRITORY,
                COOKIE_ID,
                MEMBER_ID,
                BOOKING_ID,
                EVENT_NAME,
                EVENT_TSTAMP,
                PAGE_URLPATH
            FROM row_num
            WHERE row_num = 1 -- take earliest occurance in warehouse only
        ) -- add previous event time, current vs previous event time difference in seconds, flag - value: 1, if event is first after 30 minutes gap
	,
	session_flg AS (
            SELECT *,
                LAG(EVENT_TSTAMP) OVER (                    PARTITION BY MEMBER_ID
                    ORDER BY EVENT_TSTAMP ASC) AS previous_event,
                extract(
                    epoch
                    FROM EVENT_TSTAMP - LAG(EVENT_TSTAMP) OVER (                    PARTITION BY MEMBER_ID
                    ORDER BY EVENT_TSTAMP ASC)
                ) AS time_diff,
                CASE
                    WHEN LAG(EVENT_TSTAMP) OVER (                    PARTITION BY MEMBER_ID
                    ORDER BY EVENT_TSTAMP ASC) IS NULL THEN 1 -- first event for given member
                    WHEN extract(
                        epoch
                        FROM EVENT_TSTAMP - LAG(EVENT_TSTAMP) OVER (                    PARTITION BY MEMBER_ID
                    ORDER BY EVENT_TSTAMP ASC)
                    ) > 1800 THEN 1 -- 1 if next event occured after 1800 seconds (30 minutes)
                    ELSE 0
                END AS time_diff_session_flg
            FROM dedupe
        ) -- group events into sessions
	,
	session_group AS (
            SELECT *,
                sum(time_diff_session_flg) OVER (
                    PARTITION BY "MEMBER_ID"
                    ORDER BY "EVENT_TSTAMP" ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS session_group
            FROM session_flg
        ) -- add session_id and event sequence number inside each session
	,
	session_id AS (
            SELECT *,
                concat("MEMBER_ID", '-', session_group::TEXT) AS session_id,
                row_number() OVER (
                    PARTITION BY concat("MEMBER_ID", '-', session_group::TEXT)
                    ORDER BY "EVENT_TSTAMP"
                ) AS session_event_sequence
            FROM session_group
        ) -- clear sessions attributes for events without member_id -> not sessions BY definition
        SELECT event_id,
	territory,
	cookie_id,
	member_id,
	booking_id,
	event_name,
	event_tstamp,
	page_urlpath,
	case WHEN "MEMBER_ID" IS NOT NULL THEN session_id
                ELSE NULL
            END AS session_id,
	case WHEN "MEMBER_ID" IS NOT NULL THEN time_diff
                ELSE NULL
            END AS time_diff,
	case WHEN "MEMBER_ID" IS NOT NULL THEN session_event_sequence
                ELSE NULL
            END AS session_event_sequence
        FROM session_id)
;

/*
 1.1. Sessions
 -------------
 */
DROP TABLE IF EXISTS public.fct_sessions
;

CREATE TABLE IF NOT EXISTS public.fct_sessions
(
	session_id   character varying(40),
	member_id    character varying(32),
	start_time   timestamp without time zone,
	end_time     timestamp without time zone,
	start_url    text,
	duration     integer,
	events_count integer,
	converted    integer,
	CONSTRAINT fct_sessions_pkey PRIMARY KEY (session_id)
) TABLESPACE pg_default
;

ALTER TABLE IF EXISTS public.fct_sessions OWNER TO postgres
;

INSERT INTO fct_sessions (
	-- calculte session start_time(event_time from first event in session), session end_time (event_time from last event in session), 'real' conversion from bookings 'COMPLETE' status only, session start_url
	-- only for events with session_id -> with identified member
	with EVENTS AS (
            SELECT fct_events.event_id,
                fct_events.session_id,
                fct_events.member_id,
                first_value(fct_events.event_timestamp) OVER sessions AS start_time,
                first_value(fct_events.event_timestamp) OVER (
                    PARTITION BY fct_events.session_id
                    ORDER BY fct_events.session_event_sequence DESC
                ) AS end_time,
                CASE
                    WHEN dim_bookings.booking_status = 'COMPLETE' THEN 1
                    ELSE 0
                END AS converted,
                first_value(fct_events.page_urlpath) OVER sessions AS start_url
            FROM fct_events
                LEFT JOIN dim_bookings ON fct_events.booking_id = dim_bookings.booking_id
            WHERE fct_events.session_id IS NOT NULL
	    WINDOW sessions AS (
                    PARTITION BY fct_events.session_id
                    ORDER BY fct_events.session_event_sequence ASC
                )
            ORDER BY fct_events.session_id,
                fct_events.session_event_sequence
        ) -- aggregating events into sessions, calculating session duration in secons, number of events in session
        SELECT session_id,
	member_id,
	start_time,
	end_time,
	start_url,
	extract(
                epoch
                FROM end_time - start_time
            ) AS duration,
	count(event_id) AS events_count,
	case WHEN sum(converted) > 0 THEN 1
                ELSE 0
            END AS converted
        FROM EVENTS
        GROUP BY 1,
	2,
	3,
	4,
	5)
;

/*
 -----------------------------
 2. CHECKS
 -----------------------------
 */
WITH
	counts AS (
		SELECT
			'bookings' AS dataset,
			(
				SELECT
					COUNT(DISTINCT booking_id)
				FROM bookings
			)          AS distinct_id_cnt_from_source,
			(
				SELECT
					COUNT(booking_id)
				FROM dim_bookings
			)          AS id_cnt_from_model
		UNION
		SELECT
			'members',
			(
				SELECT
					COUNT(DISTINCT member_id)
				FROM members
			),
			(
				SELECT
					COUNT(member_id)
				FROM dim_members
			)
		UNION
		SELECT
			'events',
			(
				SELECT
					COUNT(
							DISTINCT MD5(
							ROW (
								territory,
								cookie_id,
								member_id,
								booking_id,
								event_name,
								event_tstamp,
								page_urlpath
								)::text
									 )
					)
				FROM events
			),
			(
				SELECT
					COUNT(event_id)
				FROM fct_events
			)
		UNION
		SELECT
			'sessions - number of events',
			(
				SELECT
					COUNT(
							DISTINCT MD5(
							ROW (
								territory,
								cookie_id,
								member_id,
								booking_id,
								event_name,
								event_tstamp,
								page_urlpath
								)::text
									 )
					)
				FROM events
				WHERE member_id IS NOT NULL
			),
			(
				SELECT
					SUM(events_count)
				FROM fct_sessions
			)
	)
SELECT *,
	   CASE
		   WHEN distinct_id_cnt_from_source = id_cnt_from_model THEN 'Yes'
		   ELSE 'No'
	   END AS is_correct
FROM counts
;

/*
 -----------------------------
 3. ANSWERS
 -----------------------------
 */
/*
 3.1. Top 10 session landing pages
 ---------------------------------
 */
-- session landing page is first page in session
SELECT
	start_url,
	COUNT(session_id) AS landing_page_count,
	ROUND(
			COUNT(session_id) * 100 / SUM(COUNT(session_id)) OVER (),
			2
	)                 AS landing_page_percent
FROM fct_sessions
GROUP BY start_url
ORDER BY 2 DESC
LIMIT 10
;

/*
 3.2. Session conversion rate
 ---------------------------------
 */
-- defined as sessions with one or many conversion event(s) / total sessioins
-- conversion event -> event "EVENT_NAME" = 'transaction' ("BOOKING_ID" is not null) and there is corresponding record in booking table and latest booking status is 'COMPLETED'
SELECT
	TO_CHAR(DATE_TRUNC('year', start_time), 'yyyy')     AS year,
	TO_CHAR(DATE_TRUNC('month', start_time), 'yyyy-mm') AS month,
	SUM(converted)                                      AS converted_sessions,
	SUM(COUNT(session_id)) OVER                         AS year_month AS all_sessions, ROUND(
		SUM(converted) * 100 / SUM(COUNT(session_id)) OVER year_month,
		2
																					   ) AS session_conversion_rate_percent
FROM fct_sessions
GROUP BY ROLLUP (1, 2) WINDOW year_month AS (
        PARTITION BY to_char(date_trunc('year', start_time), 'yyyy'),
        to_char(date_trunc('month', start_time), 'yyyy-mm')
    )
;

/*
 3.3. Of our member sessions, count of sessions and avg number of events BY buckets of member age (age they have been a member)
 ---------------------------------
 */
-- buckets are 10 days and average number of events in session
WITH
	sessions AS (
		SELECT
			fct_sessions.member_id,
			fct_sessions.session_id,
			fct_sessions.events_count,
			dim_members.member_age_days,
			div(dim_members.member_age_days, 10) AS bucket
		FROM fct_sessions
			JOIN dim_members ON fct_sessions.member_id = dim_members.member_id
		ORDER BY dim_members.member_age_days
	)
SELECT
	bucket,
	(bucket * 10)::text || '-' || ((bucket + 1) * 10 - 1)::text || ' days' AS bucket_name,
	COUNT(session_id)                                                      AS sessions,
	ROUND(AVG(events_count), 2)                                            AS average_events_in_session,
	COUNT(DISTINCT member_id)                                              AS members
FROM sessions
GROUP BY ROLLUP (bucket)
ORDER BY bucket