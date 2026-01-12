SELECT ial.log_timestamp,
       ial.category,
       ial.user_email,
       ial.fields,
       ial.fields:source::VARCHAR                                    AS platform_source,
       ial.fields['action']::VARCHAR                                 AS action,
       ial.fields['hotel_code']::VARCHAR                             AS hotel_code,
       ial.fields['room_type_code']::VARCHAR                         AS room_type_code,
       ial.fields['inventory_date']::VARCHAR                         AS inventory_date,
       ial.fields['available_items_change']                          AS available_items_change,
       ial.fields['available_items_change']['from_value']::VARCHAR   AS available_items_change_from_value,
       ial.fields['available_items_change']['to_value']::VARCHAR     AS available_items_change_to_value,
       ial.fields['blacked_out_items_change']                        AS blacked_out_items_change,
       ial.fields['blacked_out_items_change']['from_value']::VARCHAR AS blacked_out_items_change_from_value,
       ial.fields['blacked_out_items_change']['to_value']::VARCHAR   AS blacked_out_items_change_to_value,
       ial.fields['last_updated_item_state']                         AS last_updated_item_state,
       ial.fields['last_updated_item_state']['from_value']::VARCHAR  AS last_updated_item_state_from_value,
       ial.fields['last_updated_item_state']['to_value']::VARCHAR    AS last_updated_item_state_to_value,
       ial.fields['bulk_update_id']::VARCHAR IS NOT NULL             AS is_bulk_update,
       ial.fields['bulk_update_id']::VARCHAR                         AS bulk_update_id
FROM latest_vault.mari.inventory_audit_log ial
;

self_describing_task --include 'se/data/mari/mari_inventory_audit_log.py'  --method 'run' --start '2022-03-07 00:00:00' --end '2022-03-07 00:00:00'


CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.mari.inventory_audit_log CLONE latest_vault.mari.inventory_audit_log;



SELECT * FROM se.data.mari_inventory_audit_log;



