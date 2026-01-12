self_describing_task --include 'se/data/udfs/udf_functions.py'  --method 'run' --start '2021-11-18 00:00:00' --end '2021-11-18 00:00:00'

CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.offers_offer CLONE hygiene_snapshot_vault_mvp.travelbird_mysql.offers_offer;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.offers_offerconcept CLONE hygiene_snapshot_vault_mvp.travelbird_mysql.offers_offerconcept;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.offers_category CLONE hygiene_snapshot_vault_mvp.travelbird_mysql.offers_category;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.offers_offerstaff CLONE hygiene_snapshot_vault_mvp.travelbird_mysql.offers_offerstaff;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.offers_accountmanager CLONE hygiene_snapshot_vault_mvp.travelbird_mysql.offers_accountmanager;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.offers_categorymanager CLONE hygiene_snapshot_vault_mvp.travelbird_mysql.offers_categorymanager;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.orders_order CLONE hygiene_snapshot_vault_mvp.travelbird_mysql.orders_order;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.partners_partner CLONE hygiene_snapshot_vault_mvp.travelbird_mysql.partners_partner;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_booking_review CLONE data_vault_mvp.dwh.user_booking_review;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_sale CLONE data_vault_mvp.dwh.se_sale;

self_describing_task --include 'dv/dwh/transactional/tb_offer.py'  --method 'run' --start '2021-11-18 00:00:00' --end '2021-11-18 00:00:00'

SELECT es.event_hash,
       es.touch_id,
       es.event_tstamp,
       es.event_category,
       es.event_subcategory,
       es.page_url,
       es.search_context,
       es.check_in_date,
       es.check_out_date,
       es.flexible_search,
       es.had_results,
       es.location,
       es.location_search,
       es.months,
       es.months_search,
       es.num_results,
       es.refine_by_travel_type_search,
       es.refine_by_trip_type_search,
       es.specific_dates_search,
       es.travel_types,
       es.trip_types,
       es.search_context,
       es.weekend_only_search
FROM se.data.scv_touched_searches es;

SELECT
       cc.base_currency,
       cc.currency,
       cc.category,
       cc.start_date,
       cc.end_date,
       cc.fx,
       cc.multiplier,
       cc.notes
FROm hygiene_snapshot_vault_mvp.fpa_gsheets.constant_currency cc

CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.fpa_gsheets.constant_currency CLONE hygiene_snapshot_vault_mvp.fpa_gsheets.constant_currency;

self_describing_task --include 'se/data/dwh/constant_currency.py'  --method 'run' --start '2021-11-18 00:00:00' --end '2021-11-18 00:00:00'
SELECT distinct se_brand FROM data_vault_mvp.dwh.tb_offer t