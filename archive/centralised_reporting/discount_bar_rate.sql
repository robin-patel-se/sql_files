/
USERS/
robin/
myrepos/
one-DATA-pipeline/
biapp/
task_catalogue/
se/
DATA/
se_room_rates.py
self_describing_task --include 'se/data/se_room_rates.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

/
USERS/
robin/
myrepos/
one-DATA-pipeline/
biapp/
task_catalogue/
se/
DATA/
se_room_type_rooms_and_rates.py
self_describing_task --include 'se/data/se_room_type_rooms_and_rates.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

SELECT *
FROM se_dev_robin.data.se_room_type_rooms_and_rates srtrar;


self_describing_task --include 'se/data/se_hotel_rooms_and_rates.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'


SELECT *
FROM se_dev_robin.data.se_hotel_rooms_and_rates srtrar
WHERE srtrar.hotel_code = '001w000001DVHS5';

SELECT sst.se_sale_id,
       COALESCE(MAX(sst.tag_name LIKE '%_NoJetlore'), FALSE) AS jetlore_sale
FROM se.data.dim_sale ds
LEFT JOIN se.data.se_sale_tags sst ON ds.se_sale_id = sst.se_sale_id
GROUP BY 1;


