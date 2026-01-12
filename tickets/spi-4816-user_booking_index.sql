USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh
;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.fact_booking
AS
SELECT *
FROM data_vault_mvp.dwh.fact_booking
;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_booking_index
	CLONE data_vault_mvp.dwh.user_booking_index
;

self_describing_task
\
    --include 'biapp.task_catalogue.dv.dwh.transactional.user_booking_index.py' \
    --method 'run' \
    --start '2024-12-11 00:00:00' \
    --end '2024-12-11 00:00:00'


SELECT
	schedule_tstamp,
	run_tstamp,
	operation_id,
	created_at,
	updated_at,
	booking_id,
	shiro_user_id,
	booking_completed_timestamp,
	booking_status_type,
	gross_booking_index,
	last_gross_booking_completed_timestamp,
	last_gross_booking_id,
	days_since_previous_gross_booking,
	live_booking_index,
	last_live_booking_id,
	last_live_booking_completed_timestamp,
	days_since_previous_live_booking
FROM data_vault_mvp_dev_robin.dwh.user_booking_index
;



------------------------------------------------------------------------------------------------------------------------
--transaction model


USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh
;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.fact_booking
AS
SELECT *
FROM data_vault_mvp.dwh.fact_booking
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_transactions
	CLONE data_vault_mvp.single_customer_view_stg.module_touched_transactions
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_booking
	CLONE data_vault_mvp.dwh.se_booking
;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.se_calendar
AS
SELECT *
FROM data_vault_mvp.dwh.se_calendar
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.tb_booking
	CLONE data_vault_mvp.dwh.tb_booking
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.tvl_booking
	CLONE data_vault_mvp.dwh.tvl_booking
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_booking_review
	CLONE data_vault_mvp.dwh.user_booking_review
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_booking_index
	CLONE data_vault_mvp.dwh.user_booking_index
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.wrd_booking
	CLONE data_vault_mvp.dwh.wrd_booking
;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.trx_union_bookings
	CLONE data_vault_mvp.bi.trx_union_bookings
;

self_describing_task
\
    --include 'biapp.task_catalogue.dv.bi.tableau.transaction_model.trx_union_bookings.py' \
    --method 'run' \
    --start '2024-12-11 00:00:00' \
    --end '2024-12-11 00:00:00'



self_describing_task --include 'biapp/task_catalogue/se/data/dwh/fact_booking.py'  --method 'run' --start '2024-12-10 00:00:00' --end '2024-12-10 00:00:00'
self_describing_task --include 'biapp/task_catalogue/se/data/dwh/fact_complete_booking.py'  --method 'run' --start '2024-12-10 00:00:00' --end '2024-12-10 00:00:00'



SELECT *
FROM data_vault_mvp_dev_robin.bi.trx_union_bookings
;


SELECT
	fcb.booking_id,
	fcb.gross_booking_index,
	fcb.last_gross_booking_completed_timestamp,
	fcb.last_gross_booking_id,
	fcb.days_since_previous_gross_booking,
	fcb.live_booking_index,
	fcb.last_live_booking_id,
	fcb.last_live_booking_completed_timestamp,
	fcb.days_since_previous_live_booking
FROM se.data.fact_complete_booking fcb
;



SELECT
	ub.date,
	ub.reporting_status,
	ub.booking_id,
	ub.transaction_id,
	ub.booking_status,
	ub.booking_status_type,
	ub.se_sale_id,
	ub.shiro_user_id,
	ub.check_in_date,
	ub.check_out_date,
	ub.booking_lead_time_days,
	ub.booking_created_date,
	ub.booking_completed_date,
	ub.booking_transaction_completed_date,
	ub.booking_completed_timestamp,
	ub.gross_revenue_gbp,
	ub.gross_revenue_gbp_constant_currency,
	ub.gross_revenue_eur_constant_currency,
	ub.customer_total_price_gbp,
	ub.customer_total_price_gbp_constant_currency,
	ub.gross_booking_value_gbp,
	ub.commission_ex_vat_gbp,
	ub.booking_fee_net_rate_gbp,
	ub.payment_surcharge_net_rate_gbp,
	ub.insurance_commission_gbp,
	ub.margin_gross_of_toms_gbp,
	ub.margin_gross_of_toms_gbp_constant_currency,
	ub.margin_gross_of_toms_eur_constant_currency,
	ub.no_nights,
	ub.room_nights,
	ub.adult_guests,
	ub.child_guests,
	ub.infant_guests,
	ub.price_per_night,
	ub.price_per_person_per_night,
	ub.rooms,
	ub.device_platform,
	ub.provider_name,
	ub.booking_full_payment_complete,
	ub.cancellation_date,
	ub.cancellation_reason,
	ub.cancellation_fee_refunded_on,
	ub.cancellation_fee_amount_cc,
	ub.cancellation_fee_amount_gbp_constant_currency,
	ub.cancellation_fee_amount_eur_constant_currency,
	ub.cancellation_fee_vat_cc,
	ub.cancellation_fee_vat_gbp_constant_currency,
	ub.cancellation_fee_vat_eur_constant_currency,
	ub.territory,
	ub.travel_type,
	ub.booking_includes_flight,
	ub.tech_platform,
	ub.booking_status_type_net_of_covid,
	ub.offer_name,
	ub.offer_id,
	ub.posa_category,
	ub.week,
	ub.check_in_week,
	ub.year,
	ub.se_year,
	ub.se_week,
	ub.month,
	ub.month_name,
	ub.day_of_month,
	ub.day_of_week,
	ub.week_start,
	ub.this_year,
	ub.last_year,
	ub.last_year_wda,
	ub.last_last_year,
	ub.last_last_year_wda,
	ub.this_year_ytd,
	ub.last_year_ytd,
	ub.last_last_year_ytd,
	ub.last_year_ytd_wda,
	ub.last_last_year_ytd_wda,
	ub.this_quarter,
	ub.last_quarter,
	ub.this_quarter_ly,
	ub.last_quarter_ly,
	ub.this_quarter_lly,
	ub.last_quarter_lly,
	ub.this_quarter_qtd,
	ub.this_quarter_qtd_ly,
	ub.this_quarter_qtd_lly,
	ub.this_month,
	ub.last_month,
	ub.last_month_wda,
	ub.this_month_ly,
	ub.this_month_ly_wda,
	ub.last_month_ly,
	ub.last_month_ly_wda,
	ub.this_month_lly,
	ub.this_month_lly_wda,
	ub.last_month_lly,
	ub.last_month_lly_wda,
	ub.this_month_mtd,
	ub.last_month_mtd,
	ub.this_month_mtd_ly,
	ub.last_month_mtd_ly,
	ub.this_month_mtd_lly,
	ub.last_month_mtd_lly,
	ub.last_month_mtd_wda,
	ub.this_month_mtd_ly_wda,
	ub.last_month_mtd_ly_wda,
	ub.this_month_mtd_lly_wda,
	ub.last_month_mtd_lly_wda,
	ub.this_week,
	ub.last_week,
	ub.last_last_week,
	ub.this_week_ly,
	ub.last_week_ly,
	ub.last_last_week_ly,
	ub.this_week_lly,
	ub.last_week_lly,
	ub.last_last_week_lly,
	ub.this_week_2019,
	ub.last_week_2019,
	ub.last_last_week_2019,
	ub.this_week_intra_day_reporting,
	ub.last_week_intra_day_reporting,
	ub.last_last_week_intra_day_reporting,
	ub.this_week_ly_intra_day_reporting,
	ub.last_week_ly_intra_day_reporting,
	ub.last_last_week_ly_intra_day_reporting,
	ub.this_week_lly_intra_day_reporting,
	ub.this_week_2019_intra_day_reporting,
	ub.last_week_lly_intra_day_reporting,
	ub.last_week_2019_intra_day_reporting,
	ub.last_last_week_lly_intra_day_reporting,
	ub.last_last_week_2019_intra_day_reporting,
	ub.this_week_wtd,
	ub.last_week_wtd,
	ub.last_last_week_wtd,
	ub.this_week_wtd_ly,
	ub.last_week_wtd_ly,
	ub.last_last_week_wtd_ly,
	ub.this_week_wtd_lly,
	ub.last_week_wtd_lly,
	ub.last_last_week_wtd_lly,
	ub.this_week_wtd_2019,
	ub.last_week_wtd_2019,
	ub.last_last_week_wtd_2019,
	ub.yesterday,
	ub.yesterday_last_week,
	ub.yesterday_last_last_week,
	ub.yesterday_ly,
	ub.yesterday_last_week_ly,
	ub.yesterday_last_last_week_ly,
	ub.yesterday_lly,
	ub.yesterday_last_week_lly,
	ub.yesterday_last_last_week_lly,
	ub.yesterday_2019,
	ub.yesterday_last_week_2019,
	ub.yesterday_last_last_week_2019,
	ub.today,
	ub.today_last_week,
	ub.today_last_last_week,
	ub.today_ly,
	ub.today_last_week_ly,
	ub.today_last_last_week_ly,
	ub.today_lly,
	ub.today_last_week_lly,
	ub.today_last_last_week_lly,
	ub.today_2019,
	ub.today_last_week_2019,
	ub.today_last_last_week_2019,
	ub.gross_revenue_supplier_currency,
	ub.margin_supplier_currency,
	ub.supplier_currency,
	ub.review_tstamp,
	ub.review_date,
	ub.customer_score,
	ub.review_type,
	ub.follow_up_question_id,
	NULL AS follow_up_question_context, -- 2024-10-02 issue ub.follow_up_question_context,
	ub.follow_up_question,
	NULL AS follow_up_answer_context,   -- 2024-10-02 issue  ub.follow_up_answer_context,
	ub.follow_up_answer,
	ub.survey_source,
	ub.platform,
	ub.channel,
	ub.margin_gross_of_toms_cc,
	ub.se_margin_gross_of_toms_sc,
	ub.se_flight_commission_cc,
	ub.se_flight_commission_gbp,
	ub.se_flight_commission_sc,
	ub.flight_buy_rate_cc,
	ub.flight_buy_rate_gbp,
	ub.se_flight_buy_rate_sc,
	ub.flight_only_price_cc,
	ub.flight_only_price_gbp,
	ub.se_flight_only_price_sc,
	ub.flight_supplier_name,
	ub.flight_supplier_reference,
	ub.se_flight_change_type_list,
	ub.se_flight_change_reason_list,
	ub.se_flight_adjustment_last_updated,
	ub.se_inbound_flight_arrival_timestamp,
	ub.inbound_flight_arrival_date,
	ub.se_outbound_flight_departure_timestamp,
	ub.outbound_flight_departure_date,
	ub.gross_booking_index,
	ub.last_gross_booking_completed_timestamp,
	ub.last_gross_booking_id,
	ub.days_since_previous_gross_booking,
	ub.live_booking_index,
	ub.last_live_booking_id,
	ub.last_live_booking_completed_timestamp,
	ub.days_since_previous_live_booking
FROM se.bi.union_bookings ub