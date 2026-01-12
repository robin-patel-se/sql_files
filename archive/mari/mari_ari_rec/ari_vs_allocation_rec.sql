--ari vs allocation

SELECT srtrar.room_type_id,
       srtrar.rate_date,
       srtrar.hotel_name,
       srtrar.hotel_code,
       rts.name as room_name,
       listagg(srtrar.rate_currency, ' | ')

FROM se.data.se_room_type_rooms_and_rates srtrar
LEFT JOIN data_vault_mvp.mari_snapshots.room_type_snapshot rts ON srtrar.room_type_id = rts.id
GROUP BY 1, 2, 3, 4, 5
HAVING count(*) > 1;



