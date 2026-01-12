USE WAREHOUSE pipe_xlarge
-- calculating clicks
WITH
	exploding_clicks AS (
		SELECT
			ssel.touch_id,
			spse.event_tstamp,
			spse.event_hash,
			spse.page_url,
			spse.content_interaction_array,
			clicks.value,
			clicks.value['element_category']::VARCHAR     AS element_category,
			clicks.value['element_sub_category']::VARCHAR AS element_sub_category,
			clicks.value['interaction_type']::VARCHAR     AS interaction_type,
			clicks.value['sale_id']::VARCHAR              AS se_sale_id,
		FROM se.data_pii.scv_page_screen_enrichment spse
				 INNER JOIN se.data_pii.scv_session_events_link ssel
							ON spse.event_hash = ssel.event_hash
								AND ssel.event_tstamp >= '2024-01-01',
			 LATERAL FLATTEN(INPUT => spse.content_interaction_array, OUTER => TRUE) clicks
		WHERE spse.event_tstamp >= '2024-01-01'
		  AND spse.event_name = 'page_view'
		  AND spse.page_url LIKE '%search/search%'
		  AND spse.content_interaction_array IS NOT NULL
		  AND (
			clicks.value['element_category']::VARCHAR IS NULL
				OR clicks.value['element_category']::VARCHAR IN ('search results', 'kronos_recommended_for_you')
			)
	)
SELECT
	exploding_clicks.touch_id,
	COUNT(*)                                                                                AS search_clicks,
	SUM(IFF(exploding_clicks.element_category IS NOT DISTINCT FROM 'search results', 1, 0)) AS search_results_clicks,
	SUM(IFF(exploding_clicks.element_category IS NULL OR
			exploding_clicks.element_category = 'kronos_recommended_for_you', 1,
			0))                                                                             AS search_results_kronos_clicks,
	ARRAY_AGG(DISTINCT exploding_clicks.se_sale_id)                                         AS search_clicks_array,
	ARRAY_AGG(DISTINCT
			  IFF(exploding_clicks.element_category IS NOT DISTINCT FROM 'search results', exploding_clicks.se_sale_id,
				  NULL))                                                                    AS search_results_clicks_array,
	ARRAY_AGG(DISTINCT IFF(exploding_clicks.element_category IS NULL OR
						   exploding_clicks.element_category = 'kronos_recommended_for_you',
						   exploding_clicks.se_sale_id,
						   NULL))                                                           AS search_results_kronos_clicks_array,
FROM exploding_clicks
GROUP BY 1
;



SELECT
	event_tstamp::DATE                   AS click_date,
	-- kronos clicks currently don't have a category, we are assuming clicks without a category on
	-- the search page are kronos clicks.
	COALESCE(element_category, 'kronos') AS element_category,
	COUNT(*)                             AS clicks
FROM exploding_clicks
WHERE element_category IS NULL OR element_category = 'search results'
GROUP BY 1, 2
;


SELECT *
FROM se.data_pii.scv_event_stream ses
WHERE ses.contexts_com_secretescapes_search_context_1 IS NOT NULL
  AND ses.event_tstamp >= CURRENT_DATE - 1


SELECT *
FROM se.data_pii.scv_page_screen_enrichment spse



USE ROLE PERSONAL_ROLE__ROBINPATEL;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.page_screen_enrichment
CLONE data_vault_mvp.single_customer_view_stg.page_screen_enrichment;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs
CLONE data_vault_mvp.single_customer_view_stg.module_touched_spvs;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
CLONE data_vault_mvp.single_customer_view_stg.module_touchification;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_transactions
CLONE data_vault_mvp.single_customer_view_stg.module_touched_transactions;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_searches
CLONE data_vault_mvp.single_customer_view_stg.module_touched_searches;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_booking_form_views
CLONE data_vault_mvp.single_customer_view_stg.module_touched_booking_form_views;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel
CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution
CLONE data_vault_mvp.single_customer_view_stg.module_touch_attribution;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.dim_sale
CLONE data_vault_mvp.dwh.dim_sale;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.fact_booking
AS SELECT * FROM data_vault_mvp.dwh.fact_booking;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.se_calendar
AS SELECT * FROM data_vault_mvp.dwh.se_calendar;

-- -- optional statement to create the module target table --
-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.search_model
-- CLONE data_vault_mvp.bi.search_model;

self_describing_task \
    --include 'biapp.task_catalogue.dv.bi.search.search_model.py' \
    --method 'run' \
    --start '2025-07-24 00:00:00' \
    --end '2025-07-24 00:00:00'




                     SELECT
            session_metrics.session_date,
            session_metrics.touch_experience,
            session_metrics.touch_mkt_channel,
            session_metrics.posa_category,
            session_metrics.touch_affiliate_territory,

            COUNT(DISTINCT session_metrics.touch_id) AS sessions,
            COUNT(DISTINCT session_metrics.attributed_user_id) AS users,

            COUNT(DISTINCT IFF(session_metrics.session_has_booking, session_metrics.touch_id, NULL)) AS sessions_with_booking,
            COUNT(DISTINCT IFF(session_metrics.session_has_spv, session_metrics.touch_id, NULL)) AS sessions_with_spv,
            COUNT(DISTINCT IFF(session_metrics.session_has_bfv, session_metrics.touch_id, NULL)) AS sessions_with_bfv,
            COUNT(DISTINCT IFF(session_metrics.session_has_search, session_metrics.touch_id, NULL)) AS sessions_with_search,
            COUNT(DISTINCT IFF(session_metrics.session_has_user_search, session_metrics.touch_id, NULL)) AS sessions_with_user_search,
            COUNT(DISTINCT IFF(session_metrics.session_has_pageload_search, session_metrics.touch_id, NULL)) AS sessions_with_pageload_search,
            COUNT(DISTINCT IFF(session_metrics.session_has_kronos_search, session_metrics.touch_id, NULL)) AS sessions_with_kronos_search,

            SUM(session_metrics.bookings) AS bookings,
            SUM(session_metrics.bookings_1_adult) AS bookings_1_adult,
            SUM(session_metrics.bookings_2_adults) AS bookings_2_adults,
            SUM(session_metrics.bookings_more_than_2_people) AS bookings_more_than_2_people,
            SUM(session_metrics.margin_gbp) AS margin_gbp,
            SUM(session_metrics.margin_gbp_1_adult) AS margin_gbp_1_adult,
            SUM(session_metrics.margin_gbp_2_adults) AS margin_gbp_2_adults,
            SUM(session_metrics.margin_gbp_more_than_2_people) AS margin_gbp_more_than_2_people,
            SUM(session_metrics.spvs) AS spvs,
            SUM(session_metrics.bfvs) AS bfvs,
            SUM(session_metrics.searches) AS searches,
            SUM(session_metrics.user_searches) AS user_searches,
            SUM(session_metrics.user_searches_zero_results) AS user_searches_zero_results,
            SUM(session_metrics.user_searches_one_five_results) AS user_searches_one_five_results,
            SUM(session_metrics.user_searches_six_ten_results) AS user_searches_six_ten_results,
            SUM(session_metrics.user_searches_one_ten_results) AS user_searches_one_ten_results,
            SUM(session_metrics.user_searches_greater_than_ten_results) AS user_searches_greater_than_ten_results,
            SUM(session_metrics.pageload_searches) AS pageload_searches,
            SUM(session_metrics.pageload_searches_zero_results) AS pageload_searches_zero_results,
            SUM(session_metrics.pageload_searches_one_five_results) AS pageload_searches_one_five_results,
            SUM(session_metrics.pageload_searches_six_ten_results) AS pageload_searches_six_ten_results,
            SUM(session_metrics.pageload_searches_one_ten_results) AS pageload_searches_one_ten_results,
            SUM(session_metrics.pageload_searches_greater_than_ten_results) AS pageload_searches_greater_than_ten_results,
            SUM(session_metrics.kronos_searches) AS kronos_searches,
            SUM(session_metrics.kronos_searches_zero_results) AS kronos_searches_zero_results,
            SUM(session_metrics.kronos_searches_one_five_results) AS kronos_searches_one_five_results,
            SUM(session_metrics.kronos_searches_six_ten_results) AS kronos_searches_six_ten_results,
            SUM(session_metrics.kronos_searches_one_ten_results) AS kronos_searches_one_ten_results,
            SUM(session_metrics.kronos_searches_greater_than_ten_results) AS kronos_searches_greater_than_ten_results,
            SUM(session_metrics.sessions_with_user_search_zero_results) AS sessions_with_user_search_zero_results,
            SUM(session_metrics.sessions_with_user_search_one_five_results) AS sessions_with_user_search_one_five_results,
            SUM(session_metrics.sessions_with_user_search_six_ten_results) AS sessions_with_user_search_six_ten_results,
            SUM(session_metrics.sessions_with_user_search_one_ten_results) AS sessions_with_user_search_one_ten_results,
            SUM(session_metrics.sessions_with_user_search_greater_than_ten_results) AS sessions_with_user_search_greater_than_ten_results,
            SUM(session_metrics.sessions_with_pageload_search_zero_results) AS sessions_with_pageload_search_zero_results,
            SUM(session_metrics.sessions_with_pageload_search_one_five_results) AS sessions_with_pageload_search_one_five_results,
            SUM(session_metrics.sessions_with_pageload_search_six_ten_results) AS sessions_with_pageload_search_six_ten_results,
            SUM(session_metrics.sessions_with_pageload_search_one_ten_results) AS sessions_with_pageload_search_one_ten_results,
            SUM(session_metrics.sessions_with_pageload_search_greater_than_ten_results) AS sessions_with_pageload_search_greater_than_ten_results,

            COUNT(DISTINCT IFF(session_metrics.session_has_spv_from_user_search, session_metrics.touch_id, NULL)) AS sessions_with_spv_from_user_search,
            COUNT(DISTINCT IFF(session_metrics.session_has_spv_from_kronos_search, session_metrics.touch_id, NULL)) AS sessions_with_spv_from_kronos_search,

            COUNT(DISTINCT IFF(session_metrics.session_has_bfv_from_user_search, session_metrics.touch_id, NULL)) AS sessions_with_bfv_from_user_search,
            COUNT(DISTINCT IFF(session_metrics.session_has_bfv_from_kronos_search, session_metrics.touch_id, NULL)) AS sessions_with_bfv_from_kronos_search,

            COUNT(DISTINCT IFF(session_metrics.session_has_booking_from_user_search, session_metrics.touch_id, NULL)) AS sessions_with_booking_from_user_search,
            COUNT(DISTINCT IFF(session_metrics.session_has_booking_from_kronos_search, session_metrics.touch_id, NULL)) AS sessions_with_booking_from_kronos_search,

            -- Session metrics for sessions that have at least one user search
            COUNT(DISTINCT IFF(session_metrics.session_has_user_search
                AND session_metrics.session_has_spv,
                session_metrics.touch_id, NULL)) AS sessions_with_user_search_spv,
            COUNT(DISTINCT IFF(session_metrics.session_has_user_search
                AND session_metrics.session_has_spv
                AND session_metrics.session_has_bfv,
                session_metrics.touch_id, NULL)) AS sessions_with_user_search_spv_bfv,
            COUNT(DISTINCT IFF(session_metrics.session_has_user_search
                AND session_metrics.session_has_spv
                AND session_metrics.session_has_bfv
                AND session_metrics.session_has_booking,
                session_metrics.touch_id, NULL)) AS sessions_with_user_search_spv_bfv_booking,
            COUNT(DISTINCT IFF(session_metrics.session_has_user_search
                AND session_metrics.session_has_booking,
                session_metrics.touch_id, NULL)) AS sessions_with_user_search_booking,

            -- Session metrics for sessions that have at least one user search that returned zero results
            COUNT(DISTINCT IFF(session_metrics.session_has_atleast_one_user_search_zero_results
                AND session_metrics.session_has_spv,
                session_metrics.touch_id, NULL)) AS sessions_with_user_search_zero_results_spv,
            COUNT(DISTINCT IFF(session_metrics.session_has_atleast_one_user_search_zero_results
                AND session_metrics.session_has_spv
                AND session_metrics.session_has_bfv,
                session_metrics.touch_id, NULL)) AS sessions_with_user_search_zero_results_spv_bfv,
            COUNT(DISTINCT IFF(session_metrics.session_has_atleast_one_user_search_zero_results
                AND session_metrics.session_has_spv
                AND session_metrics.session_has_bfv
                AND session_metrics.session_has_booking,
                session_metrics.touch_id, NULL)) AS sessions_with_user_search_zero_results_spv_bfv_booking,

            -- Session metrics for sessions that have at least one user search that returned 1 to 5 results
            COUNT(DISTINCT IFF(session_metrics.session_has_atleast_one_user_search_one_five_results
                AND session_metrics.session_has_spv,
                session_metrics.touch_id, NULL)) AS sessions_with_user_search_one_five_results_spv,
            COUNT(DISTINCT IFF(session_metrics.session_has_atleast_one_user_search_one_five_results
                AND session_metrics.session_has_spv
                AND session_metrics.session_has_bfv,
                session_metrics.touch_id, NULL)) AS sessions_with_user_search_one_five_results_spv_bfv,
            COUNT(DISTINCT IFF(session_metrics.session_has_atleast_one_user_search_one_five_results
                AND session_metrics.session_has_spv
                AND session_metrics.session_has_bfv
                AND session_metrics.session_has_booking,
                session_metrics.touch_id, NULL)) AS sessions_with_user_search_one_five_results_spv_bfv_booking,

            -- Session metrics for sessions that have at least one user search that returned 6 to 10 results
            COUNT(DISTINCT IFF(session_metrics.session_has_atleast_one_user_search_six_ten_results
                AND session_metrics.session_has_spv,
                session_metrics.touch_id, NULL)) AS sessions_with_user_search_six_ten_results_spv,
            COUNT(DISTINCT IFF(session_metrics.session_has_atleast_one_user_search_six_ten_results
                AND session_metrics.session_has_spv
                AND session_metrics.session_has_bfv,
                session_metrics.touch_id, NULL)) AS sessions_with_user_search_six_ten_results_spv_bfv,
            COUNT(DISTINCT IFF(session_metrics.session_has_atleast_one_user_search_six_ten_results
                AND session_metrics.session_has_spv
                AND session_metrics.session_has_bfv
                AND session_metrics.session_has_booking,
                session_metrics.touch_id, NULL)) AS sessions_with_user_search_six_ten_results_spv_bfv_booking,

            -- Session metrics for sessions that have at least one user search that returned 1 to 10 results
            COUNT(DISTINCT IFF(session_metrics.session_has_atleast_one_user_search_one_ten_results
                AND session_metrics.session_has_spv,
                session_metrics.touch_id, NULL)) AS sessions_with_user_search_one_ten_results_spv,
            COUNT(DISTINCT IFF(session_metrics.session_has_atleast_one_user_search_one_ten_results
                AND session_metrics.session_has_spv
                AND session_metrics.session_has_bfv,
                session_metrics.touch_id, NULL)) AS sessions_with_user_search_one_ten_results_spv_bfv,
            COUNT(DISTINCT IFF(session_metrics.session_has_atleast_one_user_search_one_ten_results
                AND session_metrics.session_has_spv
                AND session_metrics.session_has_bfv
                AND session_metrics.session_has_booking,
                session_metrics.touch_id, NULL)) AS sessions_with_user_search_one_ten_results_spv_bfv_booking,

            -- Session metrics for sessions that have at least one user search that returned more than 10 results
            COUNT(DISTINCT IFF(session_metrics.session_has_atleast_one_user_search_greater_than_ten_results
                AND session_metrics.session_has_spv,
                session_metrics.touch_id, NULL)) AS sessions_with_user_search_greater_ten_results_spv,
            COUNT(DISTINCT IFF(session_metrics.session_has_atleast_one_user_search_greater_than_ten_results
                AND session_metrics.session_has_spv
                AND session_metrics.session_has_bfv,
                session_metrics.touch_id, NULL)) AS sessions_with_user_search_greater_ten_results_spv_bfv,
            COUNT(DISTINCT IFF(session_metrics.session_has_atleast_one_user_search_greater_than_ten_results
                AND session_metrics.session_has_spv
                AND session_metrics.session_has_bfv
                AND session_metrics.session_has_booking,
                session_metrics.touch_id, NULL)) AS sessions_with_user_search_greater_ten_results_spv_bfv_booking,

            SUM(IFF(session_metrics.session_has_user_search, session_metrics.margin_gbp, NULL)) AS margin_gbp_user_search,
            SUM(IFF(session_metrics.session_has_user_search, session_metrics.bookings, 0)) AS booking_user_search,

            SUM(IFF(session_metrics.first_triggered_by = 'user', session_metrics.bookings, 0)) AS first_triggered_user_search_bookings,
            SUM(IFF(session_metrics.first_triggered_by = 'user', session_metrics.margin_gbp, 0)) AS first_triggered_user_search_margin_gbp,
            COUNT(DISTINCT IFF(session_metrics.first_triggered_by = 'user', session_metrics.touch_id, NULL)) AS first_triggered_user_search_sessions,
            SUM(IFF(session_metrics.first_triggered_by = 'pageLoad', session_metrics.bookings, 0)) AS first_triggered_pageload_search_bookings,
            SUM(IFF(session_metrics.first_triggered_by = 'pageLoad', session_metrics.margin_gbp, 0)) AS first_triggered_pageload_search_pageload_margin_gbp,
            COUNT(DISTINCT IFF(session_metrics.first_triggered_by = 'pageLoad', session_metrics.touch_id, NULL)) AS first_triggered_pageload_search_sessions,

            COUNT(DISTINCT IFF(session_has_search_click, session_metrics.touch_id, NULL)) AS sessions_with_search_click,
            COUNT(DISTINCT IFF(session_has_search_results_click, session_metrics.touch_id, NULL)) AS sessions_with_search_results_click,
            COUNT(DISTINCT IFF(session_has_search_results_kronos_click, session_metrics.touch_id, NULL)) AS sessions_with_search_results_kronos_click,

            SUM(session_metrics.search_clicks) AS search_clicks,
            SUM(session_metrics.search_results_clicks) AS search_results_clicks,
            SUM(session_metrics.search_results_kronos_clicks) AS search_results_kronos_clicks,

            COUNT(DISTINCT IFF(session_metrics.session_has_search_click
                AND session_merics.session_has_booking,
                session_metrics.touch_id, NULL)) AS sessions_with_search_click_booking,
            COUNT(DISTINCT IFF(session_metrics.session_has_search_results_click
                AND session_merics.session_has_booking,
                session_metrics.touch_id, NULL)) AS sessions_with_search_results_click_booking,
            COUNT(DISTINCT IFF(session_metrics.session_has_search_results_kronos_click
                AND session_merics.session_has_booking,
                session_metrics.touch_id, NULL)) AS sessions_with_search_results_kronos_click_booking

        FROM data_vault_mvp_dev_robin.bi.search_model__step08__model_data_at_session_level session_metrics
        GROUP BY
            session_metrics.session_date,
            session_metrics.touch_experience,
            session_metrics.touch_mkt_channel,
            session_metrics.posa_category,
            session_metrics.touch_affiliate_territory;

SELECT get_ddl('table', 'SE.DATA.USER_SEGMENTATION');