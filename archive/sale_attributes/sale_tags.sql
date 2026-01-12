SELECT ssa.se_sale_id,
       t.name AS tag_name
FROM se.data.se_sale_attributes ssa
         --odm tag links
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.tag_links_snapshot tl1 ON ssa.sale_id = tl1.tag_ref AND tl1.type = 'sale'
    --ndm tag links
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.tag_links_snapshot tl2 ON ssa.base_sale_id = tl2.tag_ref
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.tags_snapshot t ON COALESCE(tl1.tag_id, tl2.tag_id) = t.id
    self_describing_task --include 'se/data/se_sale_tags.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

SELECT st.se_sale_id,
       st.tag_name
FROM se_dev_robin.data.se_sale_tags AS st
GROUP BY 1
ORDER BY 2 DESC;

SELECT *
FROM se_dev_robin.data.se_sale_tags
WHERE se_sale_id = 'A9999'


SELECT *
FROM raw_vault_mvp.sfmc.jobs_list jl
WHERE jl.email_name = 'BookingSummary';

SELECT jl.email_name,
       count(*)
FROM raw_vault_mvp.sfmc.jobs_list jl
GROUP BY jl.email_name
ORDER BY 2 DESC;
;

SELECT * FROM se.data.se_sale_tags;


select
*
from se.data.se_booking sb
WHERE TRANSACTION_ID in
('A11007-12328-1876260',
'A14122-13959-1878191',
'A11825-12657-1876445',
'A13679-13756-1876638',
'A11112-12431-1929641',
'A13733-13792-1930175',
'A12160-13010-1929749',
'A11540-12717-1930487')

