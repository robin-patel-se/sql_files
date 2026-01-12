IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[process_sales_sales]') AND TYPE IN (N'P', N'PC'))
DROP PROCEDURE [dbo].process_sales_sales
    GO

--exec process_sales_sales 3, 1, 42
-- select * from stage_sales
-- select * from task_statistics

CREATE
proc process_sales_sales (	@task_id INTEGER,
									@step_sequence INTEGER,
									@task_process_step_id INTEGER )


AS
BEGIN declare @rcount INTEGER
declare @errno INTEGER
declare @msg VARCHAR(255)
declare @created INTEGER
declare @updated INTEGER
declare @rows_processed INTEGER


declare @created_tag_keywords INTEGER = 0
declare @deleted_keyword_xref INTEGER = 0
declare @created_keyword_xref INTEGER = 0
declare @deleted_territories_xref INTEGER = 0
declare @created_territories_xref INTEGER = 0
declare @deleted_dp_territories_xref INTEGER = 0
declare @created_dp_territories_xref INTEGER = 0

CREATE TABLE #changes
(
    merge_action nvarchar
(
    10
),
    key_sale INTEGER,
    tags VARCHAR
(
    2500
),
    old_dim_territory_names VARCHAR
(
    500
) NOT NULL,
    stage_territory_names VARCHAR
(
    500
) NOT NULL,
    old_dim_dp_territory_names VARCHAR
(
    500
) NOT NULL,
    stage_dp_territory_names VARCHAR
(
    500
) NOT NULL,
    sf_id VARCHAR
(
    50
) COLLATE Latin1_General_CS_AS NOT NULL )

CREATE TABLE #wordmap
(
    key_sale
    INTEGER
    NOT
    NULL,
    tag_keyword
    VARCHAR
(
    100
) NOT NULL)

-- This table will be a list of
--
--	all sales directly affected by an update in the stage table
--		PLUS
--	where the sales_model_version = 1
--		all other sales that have the same sf_id value
--
--	This is needed because for sales model version 1, an update to 1 sale can affect data
--	in other related sales.
CREATE TABLE #all_affected_sales
(
    key_sale INTEGER PRIMARY KEY
)

SET nocount ON

BEGIN TRY

-- We are getting "flash" from PL where we should be getting a provider of "TVLflash" so correct any that are wrong

UPDATE stage_sales
SET provider_name = partner + provider_name
FROM stage_sales AS s
         INNER JOIN original_sources AS so
                    ON so.source_id = s.source_id
WHERE provider_name = 'flash'
  AND partner <> ''


-- Map any sales that should be in the voucher sale provider but ar enot. Temp bodge due to errors in the sale list report.


UPDATE stage_sales
SET provider_name = r.map_to_provider_name
FROM stage_sales AS s
         INNER JOIN dbo.provider_name_remap AS r
                    ON s.sale_id = r.sale_id
                        AND s.provider_name = r.existing_provider_name
WHERE s.provider_name <> r.map_to_provider_name


-- If we have any new provider names put them in an unknown brand

INSERT INTO dbo.provider_brand_xref (provider_name, key_brand)
SELECT DISTINCT provider_name, 0
FROM dbo.stage_sales AS s
WHERE s.provider_name NOT IN (
    SELECT provider_name
    FROM dbo.provider_brand_xref
)
  AND s.provider_name IS NOT NULL


-- and then update the stage table with the correct key_brand

UPDATE stage_sales
SET key_brand = d.key_brand
FROM stage_sales AS s
         INNER JOIN dbo.provider_brand_xref AS d
                    ON d.provider_name = coalesce(s.provider_name, '')


UPDATE stage_sales
SET posu_region_id = pr.posu_region_id
FROM stage_sales AS s
         INNER JOIN posu_regions AS pr
                    ON pr.country = isnull(s.country, '')
                        AND pr.division = isnull(s.division, '')
                        AND pr.city = isnull(s.city, '')
                        AND pr.key_product = 3
WHERE s.key_product = 3

UPDATE stage_sales
SET posu_region_id = pr.posu_region_id
FROM stage_sales AS s
         INNER JOIN posu_regions AS pr
                    ON pr.country = isnull(s.country, '')
                        AND pr.division = isnull(s.division, '')
                        AND pr.city = isnull(s.city, '')
                        AND pr.key_product IS NULL
WHERE s.key_product <> 3



-- For sales that are still in "Unknown" PosuRegion create new regions for them.

-- drop table #work

CREATE TABLE #WORK
(
    country VARCHAR
(
    100
) NOT NULL,
    division VARCHAR
(
    100
) NOT NULL,
    city VARCHAR
(
    100
) NOT NULL,
    source_id SMALLINT NOT NULL,
    posu_region_id INTEGER NULL ,
    key_product TINYINT NOT NULL)

INSERT INTO #WORK (country,
                   division,
                   city,
                   source_id,
                   key_product)

SELECT DISTINCT isnull(s.country, ''), isnull(s.division, ''), isnull(s.city, ''), source_id, key_product
FROM stage_sales AS s
WHERE s.posu_region_id = 0


-- Find the best match region on country for this product type
UPDATE #WORK
SET posu_region_id = (SELECT MIN (posu_region_id) FROM posu_regions AS p
    WHERE p.country = #WORK.country
    AND p.key_product = #WORK.key_product)
WHERE posu_region_id IS NOT NULL
-- And if we do not have one get the one for NULL product
UPDATE #WORK
SET posu_region_id = (SELECT MIN (posu_region_id) FROM posu_regions AS p
    WHERE p.country = #WORK.country
    AND p.key_product IS NULL)
WHERE posu_region_id IS NULL


INSERT INTO posu_regions (country, division, city, posu_sub_region, posu_region, source_id, created_by_task_id, key_product,
                          last_updated_by_task_id, last_updated_date)

SELECT w.country, w.division, w.city, 'Other', posu_region, w.source_id, @task_id, W.key_product, @task_id, getutcdate()
FROM #WORK AS W
    INNER JOIN posu_regions AS pr
ON pr.posu_region_id = W.posu_region_id
WHERE W.posu_region_id IS NOT NULL
  AND W.key_product = 3
  AND NOT EXISTS (
    SELECT 1 FROM posu_regions AS pr2
    WHERE pr2.posu_region_id = W.posu_region_id
  AND pr2.posu_sub_region = 'Other'
  AND pr2.key_product= 3 )
UNION
SELECT w.country, w.division, w.city, 'Other', posu_region, w.source_id, @task_id, NULL, @task_id, getutcdate()
FROM #WORK AS W
    INNER JOIN posu_regions AS pr
ON pr.posu_region_id = W.posu_region_id
WHERE W.posu_region_id IS NOT NULL
  AND W.key_product <> 3
  AND NOT EXISTS (
    SELECT 1 FROM posu_regions AS pr2
    WHERE pr2.posu_region_id = W.posu_region_id
  AND pr2.posu_sub_region = 'Other'
  AND pr2.key_product IS NULL )


-- Now tidy up the sub regions to remove "Other"

UPDATE posu_regions
SET posu_sub_region         = isnull((
                                         SELECT top 1 posu_sub_region
                                         FROM posu_regions AS pr2
                                         WHERE pr2.country = posu_regions.country
                                           AND pr2.division = posu_regions.division
                                           AND pr2.posu_region_id <> posu_regions.posu_region_id
                                           AND pr2.posu_sub_region <> 'Other'
                                           AND key_product = 3
                                     ), 'Other'),
    last_updated_by_task_id = @task_id,
    last_updated_date = getutcdate()
WHERE posu_sub_region = 'Other' AND key_product = 3

UPDATE posu_regions
SET posu_sub_region         = isnull((
                                         SELECT top 1 posu_sub_region
                                         FROM posu_regions AS pr2
                                         WHERE pr2.country = posu_regions.country
                                           AND pr2.division = posu_regions.division
                                           AND pr2.posu_region_id <> posu_regions.posu_region_id
                                           AND pr2.posu_sub_region <> 'Other'
                                           AND key_product IS NULL
                                     ), 'Other'),
    last_updated_by_task_id = @task_id,
    last_updated_date = getutcdate()
WHERE posu_sub_region = 'Other' AND key_product IS NULL



UPDATE stage_sales
SET posu_region_id = pr.posu_region_id

FROM stage_sales AS s
         INNER JOIN posu_regions AS pr
                    ON pr.country = isnull(s.country, '')
                        AND pr.division = isnull(s.division, '')
                        AND pr.city = isnull(s.city, '')
                        AND pr.key_product = 3

WHERE s.posu_region_id = 0
  AND s.key_product = 3

UPDATE stage_sales
SET posu_region_id = pr.posu_region_id

FROM stage_sales AS s
         INNER JOIN posu_regions AS pr
                    ON pr.country = isnull(s.country, '')
                        AND pr.division = isnull(s.division, '')
                        AND pr.city = isnull(s.city, '')
                        AND pr.key_product IS NULL

WHERE s.posu_region_id = 0
  AND s.key_product <> 3

-- For existing sales get the key_sale into the stage table

UPDATE dbo.stage_sales
SET key_sale = d.key_sale
FROM dbo.stage_sales AS s
         INNER JOIN dbo.dim_sales AS d
                    ON d.sale_id = s.sale_id
                        AND d.key_brand = s.key_brand
                        AND d.source_id = s.source_id
WHERE s.sale_id IS NOT NULL

--- Now  work out if we need to adjust the sale start date because of bookings

UPDATE stage_sales
SET sale_start_date = foo.revised_sale_start_date
FROM stage_sales AS ss
         INNER JOIN
     (
         SELECT s.key_sale,
                CASE
                    WHEN nullif(s.sale_start_date, '19000101') IS NULL
                        THEN coalesce(ds.sale_start_date, s.sale_start_date, '19000101')
                    WHEN s.sale_start_date > ds.earliest_booking_date THEN ds.earliest_booking_date
                    ELSE s.sale_start_date END AS revised_sale_start_date
         FROM stage_sales AS s
                  INNER JOIN dim_sales AS ds
                             ON ds.key_sale = s.key_sale

     ) AS foo
     ON foo.key_sale = ss.key_sale
WHERE ss.sale_start_date <> foo.revised_sale_start_date
  AND ss.key_sale <> 0

-- The sale_trading_start_date calculated here is for the territory sale - the global_sale_trading_start_date is calculated later
-- in this stored procedure.

UPDATE stage_sales
SET sale_trading_start_date =
        CASE
            WHEN left(datename(dw, sale_start_date), 3) IN ('Fri', 'Sat', 'Sun') THEN
                CASE
                    WHEN datediff(DD, sale_start_date, sale_end_date) > 2
                        THEN dateadd(DD, 7, week_starting)
                    ELSE week_starting
                    END
            ELSE
                week_starting
            END
FROM stage_sales AS s
         INNER JOIN dim_date AS dd
                    ON dd.key_date = cast(s.sale_start_date AS DATE)
    merge INTO dbo.dim_sales AS dim
		USING
			(
				SELECT	sale_id,
						key_product,
						coalesce(key_contractor,0) AS key_contractor,
						coalesce(key_joint_contractor,0) AS key_joint_contractor,
						coalesce(CASE WHEN  charindex('|',sale_name) = 0
							THEN sale_name
								ELSE ltrim(rtrim(LEFT(sale_name,charindex('|',sale_name + '|',5) - 1 )))
						END,'')
							AS sale_name,
						coalesce(LEFT(upper(sale_name),1),'?') AS 	sale_name_starts_with,
						CAST(coalesce(sale_start_date,'19000101') AS DATE) AS sale_start_date,
						CAST(coalesce(sale_start_date,'19000101') AS TIME(0)) AS sale_start_time,
						CAST(coalesce(sale_end_date,'19000101')   AS DATE) AS sale_end_date,
						CAST(coalesce(sale_end_date,'19000101')   AS TIME(0)) AS sale_end_time,
						coalesce(destination_type, '') AS destination_type,
						coalesce(destination_name , '') AS destination_name,
						coalesce(country , '') AS country,
						coalesce(division , '') AS division,
						coalesce(city , '') AS city,
						coalesce(repeat , '') AS repeat,
						coalesce(company , '') AS company,
						coalesce(EXCLUSIVE , '') AS EXCLUSIVE,
						coalesce(territory_names , '') AS territory_names,
						coalesce(supplier_id, 0) AS supplier_id,
						coalesce(company_id , 0) AS company_id,
						coalesce(provider_name, '') AS provider_name,
						key_brand,
						source_id,
						isnull(tags,'') AS tags,
						posu_region_id,
						sale_trading_start_date,
						coalesce(contractor_region,'') AS contractor_region,
						sf_id,
						key_sale_type,
						CASE zero_deposit WHEN 'TRUE' THEN 1 ELSE 0 END AS zero_deposit_enabled,
						isnull(closest_airport_code,'') AS closest_airport_code,
						isnull(dp_territories,'') AS dp_territory_names,
						sale_model_version,
						key_sale_status,
						CASE S.overnight_flight
										WHEN 'FALSE' THEN 0
										WHEN 'TRUE' THEN 1
										ELSE 255 END  AS overnight_flight,
						CASE S.is_multi_destination
										WHEN 'FALSE' THEN 0
										WHEN 'TRUE' THEN 1
										ELSE 255 END  AS is_multi_destination



				FROM dbo.stage_sales AS S
					INNER JOIN
					(
						SELECT record_no, row_number() OVER (PARTITION BY key_brand, source_id, sale_id  ORDER BY sale_start_date DESC) AS rn
							FROM dbo.stage_sales
							WHERE errored < 2
					) 	AS most_recent
					ON most_recent.record_no = S.record_no
					WHERE rn = 1
			    EXCEPT    -- Using the except is a simple way to exclude any in the stage table where ALL columns exactly match the dimension table
				SELECT
						sale_id,
						key_product,
						key_current_contractor,
						key_joint_contractor,
						sale_name,
						sale_name_starts_with,
						sale_start_date,
						sale_start_time,
						sale_end_date,
						sale_end_time,
						destination_type,
						destination_name,
						country,
						division,
						city,
						repeat,
						company,
						EXCLUSIVE,
						territory_names,
						supplier_id,
						company_id,
						provider_name,
						key_brand,
						source_id,
						tags,
						posu_region_id,
						sale_trading_start_date,
						contractor_region,
						sf_id,
						key_sale_type,
						zero_deposit_enabled,
						closest_airport_code,
						dp_territory_names,
						sale_model_version,
						key_sale_status,
						overnight_flight ,
						is_multi_destination

					 FROM dbo.dim_sales
			) AS source

ON source.sale_id = dim.sale_id
    AND source.key_brand = dim.key_brand
    AND source.source_id = dim.source_id
    WHEN MATCHED THEN
UPDATE set key_product = source.key_product,
    key_current_contractor = source.key_contractor ,
    key_joint_contractor = source.key_joint_contractor,
    contractor_region = source.contractor_region,
    sale_name = source.sale_name,
    sale_name_starts_with = source.sale_name_starts_with ,
    sale_start_date = source.sale_start_date,
    sale_start_time = source.sale_start_time ,
    sale_end_date = source.sale_end_date ,
    sale_end_time = source.sale_end_time,
    destination_type = source.destination_type,
    destination_name = source.destination_name,
    country = source.country ,
    division = source.division,
    city = source.city ,
    repeat = source.repeat ,
    company = source.company,
    EXCLUSIVE = source.exclusive ,
    territory_names = source.territory_names ,
    supplier_id = source.supplier_id ,
    company_id = source.company_id,
    last_updated_by_task_id = @task_id,
    provider_name = CASE WHEN dim.provider_name = 'WRD' THEN dim.provider_name ELSE source.provider_name END, -- If we have set the sale inthe dimension to WRD leave it alone. CMs is wrong foer these!
    tags=source.tags,
    posu_region_id = source.posu_region_id,
    sale_trading_start_date = source.sale_trading_start_date,
    sf_id = isnull(source.sf_id,''),
    key_sale_type = source.key_sale_type,
    zero_deposit_enabled = source.zero_deposit_enabled,
    closest_airport_code = source.closest_airport_code,
    dp_territory_names = source.dp_territory_names ,
    key_sale_status = source.key_sale_status,
    overnight_flight = source.overnight_flight ,
    is_multi_destination =source.is_multi_destination
    WHEN NOT MATCHED THEN
INSERT (sale_id,
        key_product,
        key_current_contractor,
        key_joint_contractor,
        contractor_region,
        sale_name,
        sale_name_starts_with,
        sale_start_date,
        sale_start_time,
        sale_end_date,
        sale_end_time,
        destination_type,
        destination_name,
        country,
        division,
        city,
        repeat,
        company,
        EXCLUSIVE,
        territory_names,
        supplier_id,
        company_id,
        created_by_task_id,
        last_updated_by_task_id,
        provider_name,
        key_brand,
        source_id,
        tags,
        posu_region_id,
        sale_trading_start_date,
        sf_id,
        key_sale_type,
        zero_deposit_enabled,
        closest_airport_code,
        dp_territory_names,
        sale_model_version,
        key_sale_status,
        overnight_flight,
        is_multi_destination)


VALUES (source.sale_id,
    source.key_product,
    source.key_contractor,
    source.key_joint_contractor,
    source.contractor_region,
    source.sale_name,
    source.sale_name_starts_with,
    source.sale_start_date,
    source.sale_start_time,
    source.sale_end_date,
    source.sale_end_time,
    source.destination_type,
    source.destination_name,
    source.country,
    source.division,
    source.city,
    source.repeat,
    source.company,
    source.exclusive,
    source.territory_names,
    source.supplier_id,
    source.company_id,
    @task_id,
    @task_id,
    source.provider_name,
    source.key_brand,
    source.source_id,
    source.tags,
    source.posu_region_id,
    source.sale_trading_start_date,
    isnull(source.sf_id, ''),
    source.key_sale_type,
    source.zero_deposit_enabled,
    source.closest_airport_code,
    source.dp_territory_names,
    source.sale_model_version,
    source.key_sale_status,
    source.overnight_flight,
    source.is_multi_destination
    )
    output $ ACTION,
    inserted.key_sale,
    deleted.tags,
    isnull(deleted.territory_names, ''),
    isnull(inserted.territory_names, ''),
    isnull(deleted.dp_territory_names, ''),
    isnull(inserted.dp_territory_names, ''),
    isnull(source.sf_id, '')

INTO    #changes (merge_action,
                  key_sale,
                  tags,
                  old_dim_territory_names,
                  stage_territory_names,
                  old_dim_dp_territory_names,
                  stage_dp_territory_names,
                  sf_id)
OPTION (recompile);

SELECT @created = (SELECT count(*) FROM #changes WHERE merge_action = 'INSERT')
SELECT @updated = (SELECT count(*) FROM #changes WHERE merge_action = 'UPDATE')

UPDATE dbo.stage_sales
SET key_sale = d.key_sale
FROM dbo.stage_sales AS s
         INNER JOIN dbo.dim_sales AS d
                    ON d.sale_id = s.sale_id
                        AND d.key_brand = s.key_brand
                        AND d.source_id = s.source_id
WHERE s.sale_id IS NOT NULL



-- We populate the extra columns for stockalerts separately as we only want to do anything with thses when we create a new sale.
-- Removed as Alex says we should only take these from the salesforce feed.

--update dim_sales
--set stock_alert_country_code = isnull(c.alpha_2_code,'XX'),
--	stock_alert_language_code = isnull(nullif(c.default_language_code,''), 'en')
--from dim_sales as s
--		left outer join countries as c
--			on c.country_name = case when s.country in ('England', 'Scotland', 'Wales', 'Northern Ireland', 'Wales/Cymru') then 'United Kingdom'
--									when s.country in ('Polska') then 'Poland'
--									when s.country in ('USA') then 'United States'
--									else  s.country end
--where s.key_sale in (select key_sale from #changes where merge_action = 'INSERT')


INSERT INTO #all_affected_sales (key_sale)
SELECT key_sale
FROM #changes AS c

INSERT INTO #all_affected_sales (key_sale)
SELECT DISTINCT ds.key_sale
FROM
    #changes AS c
    INNER JOIN dbo.dim_sales AS ds
ON ds.sf_id = c.sf_id
WHERE ds.sale_model_version = 1
  AND ds.key_sale NOT IN ( SELECT aas.key_sale FROM #all_affected_sales AS aas )

-- Work out global_sale_start_date, global_sale_end_date and global_key_sale values for all affected sales

-- The #changed_sale_dates table was put there to allow the update of existing fact_booking data for bookings that were affected bu updates to sale date
-- info,. As mentioned in comments later in this procedure, it was decided that this should not be done, so references to this #changed_sale_dates table have been commented out.
--create table #changed_sale_dates
--(
--	key_sale int not null primary key,
--	old_global_sale_start_date date null,
--	new_global_sale_start_date date null
--)
-- For original sales model, just update the sales that have changed
UPDATE dbo.dim_sales
SET global_sale_start_date = ds.sale_start_date,
    global_sale_end_date   = ds.sale_end_date,
    global_key_sale        = ds.key_sale
    --output
    --	inserted.key_sale, deleted.global_sale_start_date, inserted.global_sale_start_date
    --into
    --	#changed_sale_dates (key_sale, old_global_sale_start_date, new_global_sale_start_date)
FROM dbo.dim_sales AS ds
         INNER JOIN #changes AS c
ON c.key_sale = ds.key_sale
WHERE
    ds.sale_model_version = 0 -- original type of sale
  AND
    (
    (ds.global_sale_start_date IS NULL
   OR ds.global_sale_start_date != ds.sale_start_date)
   OR
    (ds.global_sale_end_date IS NULL
   OR ds.global_sale_end_date != ds.sale_end_date)
   OR
    (ds.global_key_sale IS NULL
   OR ds.global_key_sale != ds.key_sale)
    );

-- For newer sales model, update all sales with a sf_id that exists in the #changes table
-- (i.e. where any sale with the same sf_id has been updated)
UPDATE dbo.dim_sales
SET global_sale_start_date = gs.global_sale_start_date,
    global_sale_end_date   = gs.global_sale_end_date,
    global_key_sale        = gs.global_key_sale
    --output
    --	inserted.key_sale, deleted.global_sale_start_date, inserted.global_sale_start_date
    --into
    --	#changed_sale_dates (key_sale, old_global_sale_start_date, new_global_sale_start_date)
FROM dbo.dim_sales AS ds
         INNER JOIN
     (
         SELECT sf_id,
                cast(min(ds.sale_start_date) AS DATE) AS global_sale_start_date,
                cast(max(ds.sale_end_date) AS DATE)   AS global_sale_end_date,
                MIN(ds.key_sale)                      AS global_key_sale
         FROM dbo.dim_sales AS ds
         WHERE ds.sale_model_version = 1
           AND ds.sf_id IN (
             SELECT c.sf_id
             FROM #changes AS c
         )
         GROUP BY sf_id
     ) AS gs ON gs.sf_id = ds.sf_id
WHERE ds.sale_model_version = 1 -- new type of sale
  AND (
        (ds.global_sale_start_date IS NULL OR ds.global_sale_start_date != gs.global_sale_start_date)
        OR
        (ds.global_sale_end_date IS NULL OR ds.global_sale_end_date != gs.global_sale_end_date)
        OR
        (ds.global_key_sale IS NULL OR ds.global_key_sale != gs.global_key_sale)
    )
  AND ds.sf_id IN (
    SELECT c.sf_id
    FROM #changes AS c
);

-- Delete live dates xref data for original sale model
DELETE
FROM fact_sale_live_dates_xref
WHERE key_sale IN (
    SELECT c.key_sale
    FROM  #changes AS c
)


-- Insert data
INSERT INTO fact_sale_live_dates_xref (key_sale, key_date, last_updated_by_task_id)
SELECT key_sale, key_date, @task_id
FROM dim_sales AS ds
    INNER JOIN dim_date AS DD
ON DD.key_date >= sale_start_date AND DD.key_date <= sale_end_date
WHERE ds.key_sale IN (SELECT c.key_Sale FROM  #changes AS c)


-- calculate global_sale_trading_start date based on global_start_date

UPDATE dim_sales
SET global_sale_trading_start_date =
        CASE
            WHEN left(datename(dw, global_sale_start_date), 3) IN ('Fri', 'Sat', 'Sun') THEN
                CASE
                    WHEN datediff(DD, global_sale_start_date, global_sale_end_date) > 2
                        THEN dateadd(DD, 7, week_starting)
                    ELSE week_starting
                    END
            ELSE
                week_starting
            END
FROM dim_sales AS s
         INNER JOIN #all_affected_sales AS aas
ON aas.key_sale = S.key_sale
    INNER JOIN dim_date AS DD ON DD.key_date = CAST (S.global_sale_start_date AS DATE)
WHERE
    (
    S.global_sale_trading_start_date !=
    CASE
    WHEN LEFT (datename(DW
    , global_sale_start_date)
    , 3) IN ('Fri'
    , 'Sat'
    , 'Sun') THEN
    CASE WHEN datediff(DD
    , global_sale_start_date
    , global_sale_end_date)
    > 2
    THEN dateadd(DD
    , 7
    , week_starting)
    ELSE week_starting
    END
    ELSE
    week_starting
    END
    )
   OR
    (
    S.global_sale_trading_start_date IS NULL
    )

-- The following code would update fact_bookings for sales where the global_sale_start_date has changed
-- At a meeting at SecretEscapes on 17 June 2018 Alex Singleton said that this should not be done for existing data
-- if only sale data has changed. If the booking is changed then the SQL that manages bookings will deal with the update appropriately

--update dbo.fact_bookings
--	set key_sale_age = case
--							when ((datediff(dd,ds.sale_trading_start_date, cast(f.date_time_booked as date)) / 7) +1 ) > 0 then ((datediff(dd,ds.sale_trading_start_date, cast(f.date_time_booked as date)) / 7) +1 )
--							else 0
--						end -- A few with dodgy start dates calculate incorrectly
--from dbo.fact_bookings as f
--inner join dbo.dim_sales as ds on ds.key_sale = f.key_sale
--inner join #changed_sale_dates as csd on csd.key_sale = f.key_sale
--where f.key_sale_age != case
--							when ((datediff(dd,ds.sale_trading_start_date, cast(f.date_time_booked as date)) / 7) +1 ) > 0 then ((datediff(dd,ds.sale_trading_start_date, cast(f.date_time_booked as date)) / 7) +1 )
--							else 0
--						end
--	and csd.old_global_sale_start_date is not null -- If this is null then it must have been a new sale, so fact_bookings will get processed by its own procedure


-- The following code would update fact_booking_form_page_views for sales where the global_sale_start_date has changed
-- At a meeting at SecretEscapes on 17 June 2018 Alex Singleton said that this should not be done for existing data
-- if only sale data has changed. If the fact_booking_form_page_views is changed then the SQL that manages fact_booking_form_page_views will deal with the update appropriately

--update dbo.fact_booking_form_page_views
--	set key_sale_age = case when ((datediff(dd,ds.sale_trading_start_date, f.key_date_viewed) / 7) + 1) > 0 then ((datediff(dd,ds.sale_trading_start_date, f.key_date_viewed) / 7) + 1) else 0 end -- A few with dodgy start dates calculate incorrectly
--from dbo.fact_booking_form_page_views as f
--inner join dbo.dim_sales as ds on ds.key_sale = f.key_sale
--inner join #changed_sale_dates as csd on csd.key_sale = f.key_sale
--where f.key_sale_age != case when ((datediff(dd,ds.sale_trading_start_date, f.key_date_viewed) / 7) + 1) > 0 then ((datediff(dd,ds.sale_trading_start_date, f.key_date_viewed) / 7) + 1) else 0 end
--	and csd.old_global_sale_start_date is not null -- If this is null then it must have been a new sale, so fact_booking_form_page_views will get processed by its own procedure


-- The following code would update fact_sales_page_views for sales where the global_sale_start_date has changed
-- At a meeting at SecretEscapes on 17 June 2018 Alex Singleton said that this should not be done for existing data
-- if only sale data has changed. If the fact_sales_page_views is changed then the SQL that manages fact_sales_page_views will deal with the update appropriately

--update dbo.fact_sales_page_views
--	set key_sale_age = case when ((datediff(dd,ds.sale_trading_start_date, f.key_date_viewed) / 7) + 1) > 0 then ((datediff(dd,ds.sale_trading_start_date, f.key_date_viewed) / 7) + 1) else 0 end -- A few with dodgy start dates calculate incorrectly
--from dbo.fact_sales_page_views as f
--inner join dbo.dim_sales as ds on ds.key_sale = f.key_sale
--inner join #changed_sale_dates as csd on csd.key_sale = f.key_sale
--where f.key_sale_age != case when ((datediff(dd,ds.sale_trading_start_date, f.key_date_viewed) / 7) + 1) > 0 then ((datediff(dd,ds.sale_trading_start_date, f.key_date_viewed) / 7) + 1) else 0 end
--and ds.global_sale_start_date <> '19000101'
--and csd.old_global_sale_start_date is not null -- If this is null then it must have been a new sale, so fact_sales_page_views will get processed by its own procedure


    declare @tag VARCHAR (2500)
    , @key_sale INTEGER
    declare c1 cursor FOR
SELECT *
FROM (
         SELECT DISTINCT
                isnull(s.tags, '') AS tags,
                s.key_sale
         FROM stage_sales AS s
                  INNER JOIN #changes AS c
         ON c.key_sale = S.key_sale
         WHERE merge_action = 'insert' -- Pick up the inserted ones
           AND S.tags IS NOT NULL

         UNION ALL

         SELECT DISTINCT S.tags, S.key_sale
         FROM stage_sales AS S
             INNER JOIN #changes AS c
         ON c.key_sale = S.key_sale
         WHERE c.merge_action = 'update'
           AND c.tags <> isnull(S.tags
             , '')

     ) AS foo
         open c1
    FETCH c1
INTO @tag, @key_sale
    while @@FETCH_STATUS = 0
BEGIN
INSERT INTO #wordmap
SELECT DISTINCT @key_sale, ltrim(rtrim(id))
FROM dbo.fn_split(@tag, ',')
UNION
SELECT @key_sale, '' -- Add blank one to ensure we have an entry for tags that are being deleted

    FETCH c1
INTO @tag, @key_sale
    END
    close c1
    deallocate c1


INSERT INTO dim_tag_keywords (tag_keyword)
SELECT DISTINCT ltrim(rtrim(tag_keyword))
FROM #wordmap
WHERE tag_keyword NOT IN (SELECT tag_keyword FROM dim_tag_keywords)
  AND tag_keyword <> ''

SET @created_tag_keywords = @@rowcount


DELETE
FROM fact_tag_keywords_xref
WHERE key_sale IN (
    SELECT key_sale
    FROM #wordmap
)

SET @deleted_keyword_xref = @@rowcount

INSERT INTO fact_tag_keywords_xref(key_sale, key_tag_keyword, last_updated_by_task_id)
SELECT DISTINCT key_sale, key_tag_keyword, @task_id
FROM #wordmap AS wm
    INNER JOIN dim_tag_keywords AS dt
ON dt.tag_keyword = wm.tag_keyword
WHERE wm.tag_keyword <> ''


SET @created_keyword_xref = @@rowcount

-- Now set repeat_latency on any sales we have created or updated

-- BUT:
-- How to deal with sales that are associated with more than 1 company?

-- repeat_latency figures

--	sale_start_date difference for this sale and the sale with:
--		same comany_id
--		different key_sale
--		earlier global_start_date
--		global_start_date != '19000101'
--		same territory (i.e. a sale that is associated with any of the territories that are associated with this sale)
--		same source_id

UPDATE dim_sales
SET repeat_latency_months = foo.repeat_latency_months,
    repeat_latency_days   = foo.repeat_latency_days
FROM dim_sales AS sale_to_update_outer
         INNER JOIN
     (
         SELECT sale_to_update.key_sale,
                isnull
                    (
                        (
                            SELECT top 1 datediff(MONTH, other_sales.sale_start_date, sale_to_update.sale_start_date)
                            FROM dim_sales AS other_sales
                                     INNER JOIN dbo.fact_sale_territories_xref AS other_sales_territories
                                                ON other_sales_territories.key_sale = other_sales.key_sale AND
                                                   other_sales_territories.is_in_sale_list_territory_names = 1
                                     INNER JOIN dbo.fact_sale_territories_xref AS sale_to_update_territories
                                                ON sale_to_update_territories.key_sale = sale_to_update.key_sale AND
                                                   sale_to_update_territories.is_in_sale_list_territory_names = 1
                            WHERE other_sales.key_sale <> sale_to_update.key_sale
                              AND other_sales.company_id = sale_to_update.company_id
                              AND other_sales.sale_start_date < sale_to_update.sale_start_date
                              AND other_sales.sale_start_date <> '19000101'
                              AND other_sales.source_id = sale_to_update.source_id
                              AND other_sales_territories.business_unit_id = sale_to_update_territories.business_unit_id
                            ORDER BY sale_start_date DESC
                        )
                    , 0
                    ) AS repeat_latency_months,

                isnull
                    (
                        (
                            SELECT top 1 datediff(DAY, other_sales.sale_start_date, sale_to_update.sale_start_date)
                            FROM dim_sales AS other_sales
                                     INNER JOIN dbo.fact_sale_territories_xref AS other_sales_territories
                                                ON other_sales_territories.key_sale = other_sales.key_sale AND
                                                   other_sales_territories.is_in_sale_list_territory_names = 1
                                     INNER JOIN dbo.fact_sale_territories_xref AS sale_to_update_territories
                                                ON sale_to_update_territories.key_sale = sale_to_update.key_sale AND
                                                   sale_to_update_territories.is_in_sale_list_territory_names = 1
                            WHERE other_sales.key_sale <> sale_to_update.key_sale
                              AND other_sales.company_id = sale_to_update.company_id
                              AND other_sales.sale_start_date < sale_to_update.sale_start_date
                              AND other_sales.sale_start_date <> '19000101'
                              AND other_sales.source_id = sale_to_update.source_id
                              AND other_sales_territories.business_unit_id = sale_to_update_territories.business_unit_id
                            ORDER BY sale_start_date DESC
                        )
                    ,
                        0
                    ) AS repeat_latency_days
         FROM dim_sales AS sale_to_update
     ) AS foo
     ON foo.key_sale = sale_to_update_outer.key_sale
WHERE sale_to_update_outer.repeat_latency_days <> foo.repeat_latency_days
  AND sale_to_update_outer.key_sale IN (
    SELECT key_sale
    FROM #changes
)


-- Do we need to modify the code below so that if a sale global_start_date is modified
-- then all other sales associated with the same company are also modified
-- because the latency of those sales could also be affected

-- global_repeat_latency figures

--	global_sale_start_date difference for this sale and the sale with:
--		same comany_id
--		different key_sale
--		earlier global_start_date
--		global_start_date != '19000101'
--		(any territory)
--		same source_id

UPDATE dim_sales
SET global_repeat_latency_months = foo.global_repeat_latency_months,
    global_repeat_latency_days   = foo.global_repeat_latency_days
FROM dim_sales AS ds
         INNER JOIN
     (

         SELECT key_sale,
                isnull((
                           SELECT top 1 datediff(MONTH, ds2.global_sale_start_date, ds.global_sale_start_date)
                           FROM dim_sales AS ds2
                           WHERE ds2.key_sale <> ds.key_sale
                             AND ds2.company_id = ds.company_id
                             AND ds2.global_sale_start_date < ds.global_sale_start_date
                             AND ds2.global_sale_start_date <> '19000101'
                             AND ds2.source_id = ds.source_id
                           ORDER BY global_sale_start_date DESC
                       ), 0) AS global_repeat_latency_months,
                isnull((
                           SELECT top 1 datediff(DAY, ds2.global_sale_start_date, ds.global_sale_start_date)
                           FROM dim_sales AS ds2
                           WHERE ds2.key_sale <> ds.key_sale
                             AND ds2.company_id = ds.company_id
                             AND ds2.global_sale_start_date < ds.global_sale_start_date
                             AND ds2.global_sale_start_date <> '19000101'
                             AND ds2.source_id = ds.source_id
                           ORDER BY global_sale_start_date DESC
                       ), 0) AS global_repeat_latency_days
         FROM dim_sales AS ds
     ) AS foo
     ON foo.key_sale = ds.key_sale
WHERE ds.repeat_latency_days <> foo.global_repeat_latency_days
  AND ds.key_sale IN (
    SELECT key_sale
    FROM #changes
)

-- Here we need to only add new ones rather than delete or update any existing ones
-- Add new rows to fact_sale_territories_xref if they aren't already there
-- This is to deal with the problems identified in project issue:
-- 1191 Hotel Plus Calculations Mismatch
-- where it was found that some sales page views and bookings are associated with
-- territories that the corresponding sale are not associated with.
-- This meant that the fact_sale_territories_xref table didn't include rows
-- that were needed in order to make the many to many dimension work
-- correctly.


INSERT INTO fact_sale_territories_xref(key_sale, business_unit_id, is_in_sale_list_territory_names, last_updated_by_task_id)
SELECT foo.key_sale, bu.business_unit_id, 1, @task_id
FROM (
    SELECT key_sale, ltrim(rtrim(s2.id)) AS business_unit_code
    FROM #changes
    CROSS apply
    dbo.fnsplit(stage_territory_names, '|') AS s2
    WHERE merge_action = 'INSERT' OR (merge_action = 'UPDATE' AND old_dim_territory_names <> stage_territory_names)
    ) AS foo
    INNER JOIN business_units AS bu
ON bu.business_unit_code = foo.business_unit_code
    LEFT OUTER JOIN fact_sale_territories_xref AS xr ON xr.business_unit_id = bu.business_unit_id AND xr.key_sale = foo.key_sale
WHERE xr.key_sale IS NULL

SET @created_territories_xref = @@rowcount

--=================================================


/*
Removed 21/09 AS Natalie tells us once a sale has appeard on the sale list in territory names it always stays there as a "deal"
update fact_sale_territories_xref
  set is_in_sale_list_territory_names = case when y.key_sale is null then 0 else 1 end
from fact_sale_territories_xref as x
 left outer join #fact_sale_territories_xref as y
	on x.key_sale = y.key_sale and x.business_unit_id = y.business_unit_id
where x.is_in_sale_list_territory_names <> case when y.key_sale is null then 0 else 1 end
and x.key_sale in (select key_sale from stage_sales)
*/



UPDATE fact_sale_territories_xref
SET is_in_sale_list_territory_names = 1,
    last_updated_by_task_id         = @task_id
FROM fact_sale_territories_xref AS x
    INNER JOIN
    (SELECT key_sale, business_unit_id
    FROM
    ( SELECT key_sale, ltrim(rtrim(s2.id)) AS business_unit_code
    FROM #changes
    CROSS apply
    dbo.fnsplit(stage_territory_names, '|') AS s2
    WHERE merge_action = 'INSERT' OR (merge_action = 'UPDATE' AND old_dim_territory_names <> stage_territory_names)
    ) AS foo
    INNER JOIN business_units AS bu
    ON bu.business_unit_code = foo.business_unit_code
    ) AS foo2

ON foo2.key_sale = x.key_sale AND foo2.business_unit_id = x.business_unit_id
    AND x.is_in_sale_list_territory_names <> 1


--*********************************************************

UPDATE dbo.fact_sale_territories_xref
SET repeat_latency_days     = foo.repeat_latency_days,
    last_updated_by_task_id = @task_id
FROM
    dbo.fact_sale_territories_xref AS xr
    INNER JOIN
    (
    SELECT
    ts_to_update.key_sale,
    ts_to_update.business_unit_id,
    isnull
    (
    (
    SELECT
    top 1
    DATEDIFF(DAY, ts_other.sale_start_date, ts_to_update.sale_start_date) AS repeat_latency_days
    FROM dbo.dim_territory_sales_v AS ts_other
    WHERE
    ts_other.key_sale != ts_to_update.key_sale                    -- different sale
    AND ts_other.company_id = ts_to_update.company_id             -- same company
    AND ts_other.source_id = ts_to_update.source_id               -- same source
    AND ts_other.business_unit_id = ts_to_update.business_unit_id -- same territory
    AND ts_other.sale_start_date < ts_to_update.sale_start_date   -- earlier start date
    AND ts_other.sale_start_date != '19000101'
--			and ts_other.is_in_sale_list_territory_names = 1
    ORDER BY ts_other.sale_start_date DESC
    ),
    0
    ) AS repeat_latency_days
    FROM
    dbo.dim_territory_sales_v AS ts_to_update
--		where ts_to_update.is_in_sale_list_territory_names = 1
    ) AS foo
ON foo.key_sale = xr.key_sale
    AND foo.business_unit_id = xr.business_unit_id
WHERE
    (xr.repeat_latency_days != foo.repeat_latency_days
   OR xr.repeat_latency_days IS NULL)
  AND
    xr.key_sale IN ( SELECT key_sale FROM #changes)

--and xr.is_in_sale_list_territory_names = 1

--*********************************************************

--=================================================


--------------------------------------------------------------------

---- Delete any xref entries for sales where the territory names have changed.

--delete from fact_sale_territories_xref
--	where key_Sale in (select key_sale from #changes where merge_action = 'UPDATE' and old_dim_territory_names <> stage_territory_names)
--set @deleted_territories_xref = @@ROWCOUNT


---- And insert territory_xref entries for new or changed rows

--insert into fact_sale_territories_xref(key_sale, business_unit_id)

--select key_sale, business_unit_id
--from (
--	select key_sale,ltrim(rtrim(s2.id)) as business_unit_code
--	from #changes
--	cross apply
--	dbo.fnsplit(stage_territory_names, '|')  as s2
--	where merge_action = 'INSERT' or (merge_action = 'UPDATE' and old_dim_territory_names <> stage_territory_names)
--) as foo
--inner join business_units as bu
--	on bu.business_unit_code = foo.business_unit_code
--set @created_territories_xref = @@rowcount


-- Delete any xref entries for sales where the dp_territory names have changed.
-- We no longer delete them ONce seen in the dp_territories we keep them there as per Carmen email 21/09/2018

--delete from fact_sale_dp_territories_xref
--	where key_Sale in (select key_sale from #changes where merge_action = 'UPDATE' and old_dim_dp_territory_names <> stage_dp_territory_names)
--set @deleted_dp_territories_xref = @@ROWCOUNT


-- And insert territory_dp_xref entries for new or changed rows

INSERT INTO fact_sale_dp_territories_xref(key_sale, business_unit_id, last_updated_by_task_id)

SELECT foo.key_sale, bu.business_unit_id, @task_id
FROM (
    SELECT key_sale, ltrim(rtrim(s2.id)) AS business_unit_code
    FROM #changes
    CROSS apply
    dbo.fnsplit(stage_dp_territory_names, '|') AS s2
    WHERE merge_action = 'INSERT' OR (merge_action = 'UPDATE' AND old_dim_dp_territory_names <> stage_dp_territory_names)
    ) AS foo
    INNER JOIN business_units AS bu
ON bu.business_unit_code = foo.business_unit_code
    LEFT OUTER JOIN fact_sale_dp_territories_xref AS xr ON xr.business_unit_id = bu.business_unit_id AND xr.key_sale = foo.key_sale
WHERE xr.key_sale IS NULL

SET @created_dp_territories_xref = @@rowcount



SET @rows_processed = @created + @updated
    exec log_task_statistics @task_id = @task_id,@action = 'c', @tsh_name = 'dim_tag_keywords', @number_affected = @created_tag_keywords
    exec log_task_statistics @task_id = @task_id,@action = 'c', @tsh_name = 'fact_tag_keywords_xref', @number_affected = @created_keyword_xref
    exec log_task_statistics @task_id = @task_id,@action = 'd', @tsh_name = 'fact_tag_keywords_xref', @number_affected = @deleted_keyword_xref
    exec log_task_statistics @task_id = @task_id,@action = 'c', @tsh_name = 'fact_sale_territories_xref', @number_affected = @created_territories_xref
    exec log_task_statistics @task_id = @task_id,@action = 'd', @tsh_name = 'fact_sale_territories_xref', @number_affected = @deleted_territories_xref
    exec log_task_statistics @task_id = @task_id,@action = 'c', @tsh_name = 'fact_sale_dp_territories_xref', @number_affected = @created_dp_territories_xref
    exec log_task_statistics @task_id = @task_id,@action = 'd', @tsh_name = 'fact_sale_dp_territories_xref', @number_affected = @deleted_dp_territories_xref
    exec log_task_statistics @task_id = @task_id,@action = 'c', @tsh_name = 'dim_sales', @number_affected = @created
    exec log_task_statistics @task_id = @task_id,@action = 'u', @tsh_name = 'dim_sales', @number_affected = @updated, @task_process_step_id = @task_process_step_id, @rows_processed = @rows_processed
    return 0
    END TRY

BEGIN CATCH
SELECT error_number()    AS errornumber,
       error_severity()  AS errorseverity,
       error_state()     AS errorstate,
       error_procedure() AS errorprocedure,
       error_line()      AS errorline,
       error_message()   AS errormessage,
    @errno AS Last_Errno,
    @rcount AS Last_Rowcount
SET @msg = 'Error Detected in "' + error_procedure() +'" ' + error_message()
    raiserror(@msg,16,1)
    Return ERROR_NUMBER()
    END CATCH;

END

GO
