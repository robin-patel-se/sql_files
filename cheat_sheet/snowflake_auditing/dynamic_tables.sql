-- inventory_updates: append-only updates of current inventory
CREATE OR REPLACE TABLE scratch.robinpatel.inventory_updates
(
	product_id number,
	stock      number,
	event_time timestamp
)
;

-- Populate first round of test data in table 1
INSERT INTO scratch.robinpatel.inventory_updates
VALUES (1, 10, CURRENT_TIMESTAMP() - INTERVAL '1 day'),
	   (1, 5, CURRENT_TIMESTAMP()),
	   (2, 20, CURRENT_TIMESTAMP())
;

SELECT *
FROM scratch.robinpatel.inventory_updates iu
;


--Query table 1

-- inventory_targets: thresholds for treating inventory of a product as low
CREATE OR REPLACE TABLE scratch.robinpatel.inventory_targets
(
	product_id   number,
	stock_target number
)
;

-- Populate first round of test data in table 2
INSERT INTO scratch.robinpatel.inventory_targets
VALUES (1, 10),
	   (2, 10)
;

SELECT *
FROM scratch.robinpatel.inventory_targets
;

-- Dynamic Table showing current inventory levels
CREATE

OR

REPLACE
dynamic TABLE scratch.robinpatel.inventory_current_3 lag='DOWNSTREAM' WAREHOUSE=PIPE_MEDIUM AS

SELECT
	product_id,
	stock
FROM scratch.robinpatel.inventory_updates
QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY event_time DESC) = 1
;

SELECT *
FROM scratch.robinpatel.inventory_current_3
;

-- Dynamic Table showing products currently below stock targets
CREATE

OR

REPLACE
dynamic TABLE scratch.robinpatel.low_inventory_alerts_3 lag='60 seconds' WAREHOUSE=PIPE_MEDIUM AS

SELECT
	product_id,
	ROUND(stock / stock_target * 100) AS stock_percent
FROM scratch.robinpatel.inventory_current_3
	JOIN scratch.robinpatel.inventory_targets USING (product_id)
WHERE stock < stock_target
;


SELECT *
FROM scratch.robinpatel.low_inventory_alerts_3
;

-- adding data to inventory updates

INSERT INTO scratch.robinpatel.inventory_updates
VALUES (1, 15, CURRENT_TIMESTAMP()),
	   (2, 0, CURRENT_TIMESTAMP())
;

-- we should see change within a few minutes
SELECT *
FROM scratch.robinpatel.inventory_current_3
; -- 5 & 20 changing to 15 & 0
SELECT *
FROM scratch.robinpatel.low_inventory_alerts_3
; -- 1/50 changing to 2/0


SELECT *
FROM scratch.robinpatel.low_inventory_alerts_3
;


SELECT *
FROM TABLE (information_schema.dynamic_table_refresh_history())
WHERE name = 'INVENTORY_CURRENT_3'
;

SELECT *
FROM TABLE (information_schema.dynamic_table_refresh_history())
WHERE name = 'LOW_INVENTORY_ALERTS_3'
;



DROP
DYNAMIC TABLE scratch.robinpatel.inventory_current_3;
DROP
DYNAMIC TABLE scratch.robinpatel.low_inventory_alerts_3;


SELECT
	e.contexts_com_secretescapes_product_display_context_1[0]['posa_territory']::VARCHAR

FROM hygiene_vault_mvp.snowplow.event_stream e
WHERE e.collector_tstamp >= CURRENT_DATE



WITH
	sale_logic AS (
		SELECT
			ds.se_sale_id,
			ds.posu_country,
			gsa.deal_category,
			ds.posa_territory
		FROM se.data.dim_sale ds
			INNER JOIN se.data.global_sale_attributes gsa
					   ON ds.salesforce_opportunity_id = gsa.global_sale_id
	)
SELECT
	dds.planning_date,
	sl.posu_country,
	sl.deal_category,
	sl.posa_territory,
	dds.deal_id,
	COUNT(DISTINCT user_id) AS athena_users
FROM data_science.operational_output.daily_deals_selections dds
	INNER JOIN sale_logic sl ON sl.se_sale_id = dds.deal_id
WHERE dds.planning_date BETWEEN '2023-01-01' AND '2023-10-31'
  AND dds.planning_position <= 9
GROUP BY 1, 2, 3, 4, 5