------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------raw code----------------------------------------------------------
UPDATE stage_sales
SET posu_region_id = pr.posu_region_id
FROM stage_sales AS s
         INNER JOIN posu_regions AS pr
                    ON pr.country = isnull(s.country, '')
                        AND pr.division = isnull(s.division, '')
                        AND pr.city = isnull(s.city, '')
                        AND pr.key_product = 3
WHERE s.key_product = 3
------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------

--where a sale is a package sale, set posu region id to the id for any entry in the posu regions table that has the
--same country, division and city for all package posu regions

SELECT * FROM data_vault_mvp.chiasma_sql_server_snapshots.dim_products_snapshot dps;
SELECT * FROM data_vault_mvp.chiasma_sql_server_snapshots.posu_regions_snapshot prs;

------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------raw code----------------------------------------------------------
UPDATE stage_sales
SET posu_region_id = pr.posu_region_id
FROM stage_sales AS s
         INNER JOIN posu_regions AS pr
                    ON pr.country = isnull(s.country, '')
                        AND pr.division = isnull(s.division, '')
                        AND pr.city = isnull(s.city, '')
                        AND pr.key_product IS NULL
WHERE s.key_product <> 3
------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------
--where a sale is not a package sale, set posu region id to the id for any entry in the posu regions table that has the
--same country, division and city for all non package posu regions

--step 1: straight matches
--posu regions has a list of mappings from country, division and city to a posu region and sub region, this is split by package
--and non package. So there will be a mapping for country X, division Y and city Z to posu region and posu sub region for both
--package and then another mapping for XYZ for non package.

------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------raw code----------------------------------------------------------
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
------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------

--step 2: sales that don't match the joins to posu regions table (on previous logic)
--for sales that don't have a straight mapping join on the first entry in the posu regions table with a join on country where the
--product type matches

------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------raw code----------------------------------------------------------
-- And if we do not have one get the one for NULL product
UPDATE #WORK
SET posu_region_id = (SELECT MIN (posu_region_id) FROM posu_regions AS p
    WHERE p.country = #WORK.country
    AND p.key_product IS NULL)
WHERE posu_region_id IS NULL
------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------

--step 3: sales that still dont have a posu region because there's no match for product type
--for sales that don't have a mapping to just country and product type match to default product type (null) and use the first
--entry in the posu regions table for that country


------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------raw code----------------------------------------------------------
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
------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------
-- step 4: insert new posu sub regions into posu table
-- for entries of posu region that don't have a sub region and don't have a sub region 'other' in the posu regions table
-- add a new entry into the posu regions table with posu sub region as 'Other'



------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------raw code----------------------------------------------------------
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
------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------
-- step 5: tidy up the 'Other' sub regions in posu table
-- for package posu regions that are currently set to 'Other' select the first posu sub region from the rest of the table by
-- matching on country and division that is also a package posu region and isn't the same region or another region with sub region
-- set to other. Do the same for non package.

------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------raw code----------------------------------------------------------
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
------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------
--step 6: update stage table with posu (sub)region matches.
--rejoin stage sales now that posu regions table has been updated and join on country division city by product type