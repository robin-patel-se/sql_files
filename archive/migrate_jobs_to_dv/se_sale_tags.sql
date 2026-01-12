python biapp/
bau/
hygiene/
gen_hygiene_files.py \
    --data_source cms_mysql \
    --name tags \
    --primary_key_cols id \


SELECT *
FROM raw_vault_mvp.cms_mysql.tag_links tl;
SELECT *
FROM raw_vault_mvp.cms_mysql.tags t;

DROP TABLE data_vault_mvp.cms_mysql_snapshots.tags_snapshot;

CREATE OR REPLACE TRANSIENT TABLE raw_vault_mvp_dev_robin.cms_mysql.tags CLONE raw_vault_mvp.cms_mysql.tags;
CREATE OR REPLACE TRANSIENT TABLE raw_vault_mvp_dev_robin.cms_mysql.tag_links CLONE raw_vault_mvp.cms_mysql.tag_links;

SELECT MIN(loaded_at)
FROM raw_vault_mvp.cms_mysql.tags t; -- 2020-03-24 12:59:50.998347000
airflow backfill --start_date '2020-03-24 00:00:00' --end_date '2020-03-24 00:00:00' --task_regex '.*' hygiene_snapshots__cms_mysql__tags__daily_at_01h00

self_describing_task --include 'staging/hygiene/cms_mysql/tags.py'  --method 'run' --start '2018-01-01 00:00:00' --end '2018-01-01 00:00:00'
self_describing_task --include 'staging/hygiene/cms_mysql/tag_links.py'  --method 'run' --start '2018-01-01 00:00:00' --end '2018-01-01 00:00:00'

self_describing_task --include 'staging/hygiene_snapshots/cms_mysql/tags.py'  --method 'run' --start '2018-01-01 00:00:00' --end '2018-01-01 00:00:00'
self_describing_task --include 'staging/hygiene_snapshots/cms_mysql/tag_links.py'  --method 'run' --start '2018-01-01 00:00:00' --end '2018-01-01 00:00:00'

DROP TABLE hygiene_vault_mvp_dev_robin.cms_mysql.tag_links;

SELECT sst.se_sale_id,
       sst.tag_name
FROM se.data.se_sale_tags sst;


self_describing_task --include 'dv/dwh/transactional/se_sale_tags.py'  --method 'run' --start '2021-07-21 00:00:00' --end '2021-07-21 00:00:00'


SELECT ss.se_sale_id,
       t.name AS tag_name
FROM data_vault_mvp_dev_robin.dwh.se_sale ss
         --odm tag links
         LEFT JOIN hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.tag_links tl1 ON ss.sale_id = tl1.tag_ref AND tl1.type = 'sale'
                       --ndm tag links
         LEFT JOIN hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.tag_links tl2 ON ss.base_sale_id = tl2.tag_ref

         LEFT JOIN hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.tags t ON COALESCE(tl1.tag_id, tl2.tag_id) = t.id
WHERE t.name IS NOT NULL;


    CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_sale_tags__model_data COPY GRANTS(

        SELECT ss.se_sale_id,
               t.name as tag_name
        FROM data_vault_mvp_dev_robin.dwh.se_sale ss
                 --odm tag links
                 LEFT JOIN hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.tag_links tl1 ON ss.sale_id = tl1.tag_ref AND tl1.type = 'sale'
                 --ndm tag links
                 LEFT JOIN hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.tag_links tl2 ON ss.base_sale_id = tl2.tag_ref

                 LEFT JOIN hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.tags t ON COALESCE(tl1.tag_id, tl2.tag_id) = t.id
        WHERE t.name IS NOT NULL
    );


SELECT * FROM data_vault_mvp_dev_robin.dwh.se_sale_tags;

self_describing_task --include 'se/data/dwh/se_sale_tags.py'  --method 'run' --start '2021-07-21 00:00:00' --end '2021-07-21 00:00:00'

CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.se_sale_tags AS SELECT * FROM se.data.se_sale_tags;
CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp_dev_robin.cms_mysql.tag_links clone hygiene_snapshot_vault_mvp.cms_mysql.tag_links;

SELECT * FROM scratch.robinpatel.se_sale_tags sst
EXCEPT
SELECT * FROm se_dev_robin.data.se_sale_tags s;


SELECT * FROM scratch.robinpatel.se_sale_tags sst
EXCEPT
SELECT * FROm se_dev_robin.data.se_sale_tags s;