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
DROP TABLE IF EXISTS public.dim_bookings;

CREATE TABLE IF NOT EXISTS public.dim_bookings (
    booking_id character varying(32),
    member_id character varying(32),
    booking_date timestamp without time zone,
    last_updated timestamp without time zone,
    booking_status character varying(16) COLLATE pg_catalog."default",
    CONSTRAINT dim_bookings_pkey PRIMARY KEY (booking_id)
) TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.dim_bookings OWNER TO postgres;

INSERT INTO dim_bookings (
        -- identify duplicates - same data but different "LAST_UPDATED"
        WITH row_num AS (
            SELECT row_number() OVER (
                    PARTITION BY "BOOKING_DATE",
                    "MEMBER_ID",
                    "BOOKING_ID",
                    "BOOKING_DATE",
                    "BOOKING_STATUS"
                    ORDER BY "LAST_UPDATED"
                ) AS row_num,
                *
            FROM bookings
        ) -- remove duplicated rows
,
        dedupe AS (
            SELECT *
            FROM row_num
            WHERE row_num = 1
        ) -- set attributes from recent update
        SELECT "BOOKING_ID",
            "MEMBER_ID",
            "BOOKING_DATE",
            "LAST_UPDATED",
            "BOOKING_STATUS"
        FROM (
                SELECT *,
                    row_number() OVER (
                        PARTITION BY "BOOKING_ID"
                        ORDER BY "LAST_UPDATED" DESC
                    ) AS status_num
                FROM dedupe
            )
        WHERE status_num = 1
    );

-- deduped bookings
-- highlighted performance issues



/*
 1.1. Members
 -------------
 */
DROP TABLE IF EXISTS public.dim_members;

CREATE TABLE IF NOT EXISTS public.dim_members (
    member_id character varying(32),
    sign_up_date timestamp without time zone,
    last_updated timestamp without time zone,
    territories character varying(50),
    member_age_days integer,
    CONSTRAINT dim_members_pkey PRIMARY KEY (member_id)
) TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.dim_members OWNER TO postgres;

INSERT INTO dim_members (
        -- identify duplicates - same data but different "LAST_UPDATED"
        WITH row_num AS (
            SELECT row_number() OVER (
                    PARTITION BY "MEMBER_ID",
                    "SIGN_UP_DATE",
                    "ORIGINAL_TERRITORY",
                    "CURRENT_TERRITORY"
                    ORDER BY "LAST_UPDATED"
                ) AS row_num,
                *
            FROM members
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
                "MEMBER_ID",
                "SIGN_UP_DATE",
                "LAST_UPDATED",
                string_agg(
                    DISTINCT concat(
                        "ORIGINAL_TERRITORY",
                        ' -> ',
                        "CURRENT_TERRITORY"
                    ),
                    ';'
                ) AS "TERRITORIES" -- remove anomalies
            FROM dedupe
            GROUP BY 2,
                3,
                4
        ) -- define end_date for member_age calculation
,
        end_date AS (
            SELECT max("EVENT_TSTAMP")::date AS end_date
            FROM EVENTS
        ) -- set first "SIGN_UP_DATE" from recent update and calculate mamber_age
        SELECT "MEMBER_ID",
            "SIGN_UP_DATE",
            "LAST_UPDATED",
            "TERRITORIES",
            (
                SELECT end_date
                FROM end_date
            ) - "SIGN_UP_DATE"::date AS member_age
        FROM (
                SELECT *,
                    row_number() OVER (
                        PARTITION BY "MEMBER_ID"
                        ORDER BY "LAST_UPDATED" DESC
                    ) AS sign_up_num
                FROM agg
            )
        WHERE sign_up_num = 1
    );

-- de duplication is spot on
-- interesting approach to territory - probe for context
-- member age has been established as an attribute
-- mixes ctes and subqueries

	-- what would you ask about territories


/*
 1.1. Events
 -------------
 */
DROP TABLE IF EXISTS public.fct_events;

CREATE TABLE IF NOT EXISTS public.fct_events (
    event_id character varying(32),
    territory character varying(2),
    cookie_id character varying(36),
    member_id character varying(32),
    booking_id character varying(32),
    event_name character varying(11),
    event_timestamp timestamp without time zone,
    page_urlpath text,
    session_id character varying(40),
    seconds_from_prev_event numeric,
    session_event_sequence integer,
    CONSTRAINT fct_events_pkey PRIMARY KEY (event_id)
) TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.fct_events OWNER TO postgres;

INSERT INTO fct_events (
        -- create event_id hashing columns
        WITH members_in AS (
            SELECT md5(
                    ROW(
                        "TERRITORY",
                        "COOKIE_ID",
                        "MEMBER_ID",
                        "BOOKING_ID",
                        "EVENT_NAME",
                        "EVENT_TSTAMP",
                        "PAGE_URLPATH"
                    )::text
                ) AS event_id,
                *
            FROM EVENTS
        ) -- identify duplicates - same data but different "EXTRACTED_AT"
,
        row_num AS (
            SELECT *,
                row_number() OVER (
                    PARTITION BY event_id
                    ORDER BY "EXTRACTED_AT" ASC
                ) AS row_num
            FROM members_in
        ) -- remove duplicated rows and unnecessary columns
,
        dedupe AS (
            SELECT event_id,
                "TERRITORY",
                "COOKIE_ID",
                "MEMBER_ID",
                "BOOKING_ID",
                "EVENT_NAME",
                "EVENT_TSTAMP",
                "PAGE_URLPATH"
            FROM row_num
            WHERE row_num = 1 -- take earliest occurance in warehouse only
        ) -- add previous event time, current vs previous event time difference in seconds, flag - value: 1, if event is first after 30 minutes gap
,
        session_flg AS(
            SELECT *,
                lag("EVENT_TSTAMP") OVER prev_event AS previous_event,
                extract(
                    epoch
                    FROM "EVENT_TSTAMP" - lag("EVENT_TSTAMP") OVER prev_event
                ) AS time_diff,
                CASE
                    WHEN lag("EVENT_TSTAMP") OVER prev_event IS NULL THEN 1 -- first event for given member
                    WHEN extract(
                        epoch
                        FROM "EVENT_TSTAMP" - lag("EVENT_TSTAMP") OVER prev_event
                    ) > 1800 THEN 1 -- 1 if next event occured after 1800 seconds (30 minutes)
                    ELSE 0
                END AS time_diff_session_flg
            FROM dedupe
	    WINDOW prev_event AS (
                    PARTITION BY "MEMBER_ID"
                    ORDER BY "EVENT_TSTAMP" ASC
                )
        ) -- group events into sessions
,
        session_group AS (
            SELECT *,
                sum(time_diff_session_flg) OVER (
                    PARTITION BY "MEMBER_ID"
                    ORDER BY "EVENT_TSTAMP" ASC ROWS BETWEEN unbounded preceding AND current ROW
                ) AS session_group
            FROM session_flg
        ) -- add session_id and event sequence number inside each session
,
        session_id AS (
            SELECT *,
                concat("MEMBER_ID", '-', session_group::text) AS session_id,
                row_number() OVER (
                    PARTITION BY concat("MEMBER_ID", '-', session_group::text)
                    ORDER BY "EVENT_TSTAMP"
                ) AS session_event_sequence
            FROM session_group
        ) -- clear sessions attributes for events without member_id -> not sessions BY definition
        SELECT event_id,
            "TERRITORY",
            "COOKIE_ID",
            "MEMBER_ID",
            "BOOKING_ID",
            "EVENT_NAME",
            "EVENT_TSTAMP",
            "PAGE_URLPATH",
            CASE
                WHEN "MEMBER_ID" IS NOT NULL THEN session_id
                ELSE NULL
            END AS session_id,
            CASE
                WHEN "MEMBER_ID" IS NOT NULL THEN time_diff
                ELSE NULL
            END AS time_diff,
            CASE
                WHEN "MEMBER_ID" IS NOT NULL THEN session_event_sequence
                ELSE NULL
            END AS session_event_sequence
        FROM session_id
    );

-- more complicated questions

/*
 1.1. Sessions
 -------------
 */
DROP TABLE IF EXISTS public.fct_sessions;

CREATE TABLE IF NOT EXISTS public.fct_sessions (
    session_id character varying(40),
    member_id character varying(32),
    start_time timestamp without time zone,
    end_time timestamp without time zone,
    start_url text,
    duration integer,
    events_count integer,
    converted integer,
    CONSTRAINT fct_sessions_pkey PRIMARY KEY (session_id)
) TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.fct_sessions OWNER TO postgres;

INSERT INTO fct_sessions (
        -- calculte session start_time(event_time from first event in session), session end_time (event_time from last event in session), 'real' conversion from bookings 'COMPLETE' status only, session start_url
        -- only for events with session_id -> with identified member
        WITH EVENTS AS (
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
            COUNT(event_id) AS events_count,
            CASE
                WHEN sum(converted) > 0 THEN 1
                ELSE 0
            END AS converted
        FROM EVENTS
        GROUP BY 1,
            2,
            3,
            4,
            5
    );

/*
 -----------------------------
 2. CHECKS
 -----------------------------
 */
WITH counts AS (
    SELECT 'bookings' AS dataset,
        (
            SELECT COUNT(DISTINCT "BOOKING_ID")
            FROM bookings
        ) AS distinct_id_cnt_from_source,
        (
            SELECT COUNT(booking_id)
            FROM dim_bookings
        ) AS id_cnt_from_model
    UNION
    SELECT 'members',
        (
            SELECT COUNT(DISTINCT "MEMBER_ID")
            FROM members
        ),
        (
            SELECT COUNT(member_id)
            FROM dim_members
        )
    UNION
    SELECT 'events',
        (
            SELECT COUNT(
                    DISTINCT md5(
                        ROW(
                            "TERRITORY",
                            "COOKIE_ID",
                            "MEMBER_ID",
                            "BOOKING_ID",
                            "EVENT_NAME",
                            "EVENT_TSTAMP",
                            "PAGE_URLPATH"
                        )::text
                    )
                )
            FROM EVENTS
        ),
        (
            SELECT COUNT(event_id)
            FROM fct_events
        )
    UNION
    SELECT 'sessions - number of events',
        (
            SELECT COUNT(
                    DISTINCT md5(
                        ROW(
                            "TERRITORY",
                            "COOKIE_ID",
                            "MEMBER_ID",
                            "BOOKING_ID",
                            "EVENT_NAME",
                            "EVENT_TSTAMP",
                            "PAGE_URLPATH"
                        )::text
                    )
                )
            FROM EVENTS
            WHERE "MEMBER_ID" IS NOT NULL
        ),
        (
            SELECT sum(events_count)
            FROM fct_sessions
        )
)
SELECT *,
    CASE
        WHEN distinct_id_cnt_from_source = id_cnt_from_model THEN 'Yes'
        ELSE 'No'
    END AS is_correct
FROM counts;

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
SELECT start_url,
    COUNT(session_id) AS landing_page_count,
    round(
        COUNT(session_id) * 100 / sum(COUNT(session_id)) OVER (),
        2
    ) AS landing_page_percent
FROM fct_sessions
GROUP BY start_url
ORDER BY 2 DESC
LIMIT 10;

/*
 3.2. Session conversion rate
 ---------------------------------
 */
-- defined as sessions with one or many conversion event(s) / total sessioins
-- conversion event -> event "EVENT_NAME" = 'transaction' ("BOOKING_ID" is not null) and there is corresponding record in booking table and latest booking status is 'COMPLETED'
SELECT to_char(date_trunc('year', start_time), 'yyyy') AS year,
    to_char(date_trunc('month', start_time), 'yyyy-mm') AS MONTH,
    sum(converted) AS converted_sessions,
    sum(COUNT(session_id)) OVER year_month AS all_sessions,
    round(
        sum(converted) * 100 / sum(COUNT(session_id)) OVER year_month,
        2
    ) AS session_conversion_rate_percent
FROM fct_sessions
GROUP BY rollup(1, 2)
WINDOW year_month AS (
        PARTITION BY to_char(date_trunc('year', start_time), 'yyyy'),
        to_char(date_trunc('month', start_time), 'yyyy-mm')
    );

-- has identified that sessions might have multiple conversions

/*
 3.3. Of our member sessions, count of sessions and avg number of events BY buckets of member age (age they have been a member)
 ---------------------------------
 */
-- buckets are 10 days and average number of events in session
WITH sessions AS (
    SELECT fct_sessions.member_id,
        fct_sessions.session_id,
        fct_sessions.events_count,
        dim_members.member_age_days,
        DIV(dim_members.member_age_days, 10) AS bucket
    FROM fct_sessions
        JOIN dim_members ON fct_sessions.member_id = dim_members.member_id
    ORDER BY dim_members.member_age_days
)
SELECT bucket,
    (bucket * 10)::text || '-' || ((bucket + 1) * 10 - 1)::text || ' days' AS bucket_name,
    COUNT(session_id) AS sessions,
    round(avg(events_count), 2) AS average_events_in_session,
    COUNT(DISTINCT member_id) AS members
FROM sessions
GROUP BY rollup(bucket)
ORDER BY bucket

-- member age is based on current age, what about their age at the time of session?