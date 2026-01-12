SELECT *,
       sc.code AS source_code,
       tc.code AS target_code
FROM data_vault_mvp.travelbird_cms.currency_exchangerate_snapshot ces
         LEFT JOIN data_vault_mvp.travelbird_cms.currency_currency_snapshot sc ON ces.source_id = sc.id
         LEFT JOIN data_vault_mvp.travelbird_cms.currency_currency_snapshot tc ON ces.target_id = tc.id


CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.orders_orderitembase CLONE hygiene_snapshot_vault_mvp.travelbird_mysql.orders_orderitembase;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.django_content_type CLONE hygiene_snapshot_vault_mvp.travelbird_mysql.django_content_type;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.currency_exchangerateupdate CLONE hygiene_snapshot_vault_mvp.travelbird_mysql.currency_exchangerateupdate;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.orders_order CLONE hygiene_snapshot_vault_mvp.travelbird_mysql.orders_order;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.fpa_gsheets.constant_currency CLONE hygiene_snapshot_vault_mvp.fpa_gsheets.constant_currency;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.travelbird_cms.currency_currency_snapshot CLONE data_vault_mvp.travelbird_cms.currency_currency_snapshot;
CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.travelbird_cms;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.travelbird_cms.orders_person_snapshot CLONE data_vault_mvp.travelbird_cms.orders_person_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.travelbird_cms.orders_orderevent_snapshot CLONE data_vault_mvp.travelbird_cms.orders_orderevent_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.travelbird_cms.orders_orderproperty_snapshot CLONE data_vault_mvp.travelbird_cms.orders_orderproperty_snapshot;

self_describing_task --include 'dv/dwh/transactional/tb_booking.py'  --method 'run' --start '2021-01-14 00:00:00' --end '2021-01-14 00:00:00'

SELECT order_id,
       currency,
       sold_price_total_cc,
       cost_price_total_cc,
       sold_price_total_eur,
       cost_price_total_eur

FROM data_vault_mvp_dev_robin.dwh.tb_booking__step02__calculate_financials
WHERE order_id = 21872095
;


--
-- SELECT oo.created_at_dts,
--        oo.cost_price_excl_vat,
--        sc.code as source_currency,
--        tc.code as target_currency,
--        fx_cc.*
--
-- FROM hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.orders_orderitembase oo
--          LEFT JOIN data_vault_mvp.travelbird_cms.currency_currency_snapshot sc ON oo.sold_price_currency_id = sc.id
--          LEFT JOIN data_vault_mvp.travelbird_cms.currency_currency_snapshot tc ON oo.cost_price_currency_id = tc.id
--          LEFT JOIN (
--     --work out the currency conversion from supplier currency back to customer currency
--     --this should already be deduped. Using group / aggregate here to add extra layer of error catching
--     SELECT usage_date,
--            source_id,
--            target_id,
--            AVG(rate) AS rate
--     FROM data_vault_mvp.travelbird_cms.currency_exchangerateupdate_snapshot
--     GROUP BY 1, 2, 3
-- ) fx_cc ON oo.created_at_dts::DATE = fx_cc.usage_date
--     AND oo.sold_price_currency_id = fx_cc.target_id
--     AND oo.cost_price_currency_id = fx_cc.source_id
-- WHERE oo.order_id = 21872095;


WITH rates AS (
    SELECT usage_date,
           source_id,
           target_id,
           AVG(rate) AS rate
    FROM data_vault_mvp.travelbird_cms.currency_exchangerateupdate_snapshot
    GROUP BY 1, 2, 3
),
     eur_rates AS (
         --conversion TO EUR is not included so reverse calculating
         SELECT usage_date,
                target_id AS source_id,
                source_id AS target_id,
                1 / rate  AS rate
         FROM rates
     ),
     union_rates AS (
         SELECT *
         FROM rates r
         UNION
         SELECT *
         FROM eur_rates
     )
SELECT *
FROM union_rates u
WHERE u.target_id = 1


SELECT *
FROM data_vault_mvp_dev_robin.dwh.tb_booking__step01_populate_fx_rates
WHERE source_currency = 'USD'
  AND target_currency = 'EUR'
  AND usage_date = '2019-08-30';


SELECT oo.id,
       oo.sold_price_currency_id,
       oo.cost_price_currency_id,
       oo.created_at_dts,
       oo.cost_price_excl_vat,
       fx_cc.*,
       IFF(oo.sold_price_currency_id = oo.cost_price_currency_id, oo.cost_price_excl_vat, oo.cost_price_excl_vat * fx_cc.rate)

FROM hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.orders_orderitembase oo
         LEFT JOIN data_vault_mvp_dev_robin.dwh.tb_booking__step01_populate_fx_rates fx_cc
                   ON oo.created_at_dts::DATE = fx_cc.usage_date
                       AND oo.sold_price_currency_id = fx_cc.target_id
                       AND oo.cost_price_currency_id = fx_cc.source_id
WHERE oo.order_id = 21872095;

self_describing_task --include 'dv/dwh/transactional/tb_booking.py'  --method 'run' --start '2021-01-14 00:00:00' --end '2021-01-14 00:00:00'


SELECT currency FROM data_vault_mvp_dev_robin.dwh.tb_booking tb;

seLECT * FROm se.data.

self_describing_task --include 'se/data/dwh/fact_booking.py'  --method 'run' --start '2021-01-14 00:00:00' --end '2021-01-14 00:00:00'
self_describing_task --include 'se/data/dwh/fact_complete_booking.py'  --method 'run' --start '2021-01-14 00:00:00' --end '2021-01-14 00:00:00'


SELECT * FROM se.data.se_booking sb WHERE sb.cancellation_reason = 'MEMBER_CANCELLATION_REQUEST'
AND CHECK_IN_DATE >= '2020-03-01'
AND CHECK_OUT_DATE <= '2020-12-31';