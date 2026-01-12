self_describing_task --include 'hygiene_snapshots/cms_mysql/booking_note.py'  --method 'run' --start '2021-01-15 00:00:00' --end '2021-01-15 00:00:00'

CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp_dev_robin.cms_mysql.booking_note CLONE hygiene_vault_mvp.cms_mysql.booking_note;

SELECT MIN(updated_at)
FROM hygiene_vault_mvp_dev_robin.cms_mysql.booking_note; --2021-01-15 17:51:47.913000000

MERGE INTO hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.booking_note AS target
    USING hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.booking_note__step02__dedupe AS batch
    ON target.id = batch.id
    WHEN MATCHED AND target.row_loaded_at < batch.row_loaded_at
        THEN UPDATE SET
        target.schedule_tstamp = '2021-01-14 01:00:00',
        target.run_tstamp = '2021-01-19 14:02:43',
        target.operation_id =
                'ScriptOperator__/usr/local/one-data-pipeline/biapp/task_catalogue/staging/hygiene_snapshots/cms_mysql/booking_note.py__20210114T010000__daily_at_01h00',
        target.updated_at = CURRENT_TIMESTAMP()::TIMESTAMP,


        -- (lineage) original metadata of row itself
        target.row_dataset_name = batch.row_dataset_name,
        target.row_dataset_source = batch.row_dataset_source,
        target.row_loaded_at = batch.row_loaded_at,
        target.row_schedule_tstamp = batch.row_schedule_tstamp,
        target.row_run_tstamp = batch.row_run_tstamp,
        target.row_filename = batch.row_filename,
        target.row_file_row_number = batch.row_file_row_number,
        target.row_extract_metadata = batch.row_extract_metadata,

        -- deduped columns from hygiene step

        target.booking_id = batch.booking_id,
        target.actioned_by_email = batch.actioned_by_email,
        target.actioned_by_email_domain = batch.actioned_by_email_domain,
        target.requested_by_email = batch.requested_by_email,
        target.requested_by_email_domain = batch.requested_by_email_domain,
        target.id = batch.id,
        target.version = batch.version,
        target.booking_id__o = batch.booking_id__o,
        target.content = batch.content,
        target.date_created = batch.date_created,
        target.last_updated = batch.last_updated,
        target.reservation_id = batch.reservation_id
    WHEN NOT MATCHED
        THEN INSERT VALUES ('2021-01-14 01:00:00',
                            '2021-01-19 14:02:43',
                            'ScriptOperator__/usr/local/one-data-pipeline/biapp/task_catalogue/staging/hygiene_snapshots/cms_mysql/booking_note.py__20210114T010000__daily_at_01h00',
                            CURRENT_TIMESTAMP()::TIMESTAMP,
                            CURRENT_TIMESTAMP()::TIMESTAMP,
                            batch.row_dataset_name,
                            batch.row_dataset_source,
                            batch.row_loaded_at,
                            batch.row_schedule_tstamp,
                            batch.row_run_tstamp,
                            batch.row_filename,
                            batch.row_file_row_number,
                            batch.row_extract_metadata,
                            batch.booking_id,
                            batch.actioned_by_email,
                            batch.actioned_by_email_domain,
                            batch.requested_by_email,
                            batch.requested_by_email_domain,
                            batch.id,
                            batch.version,
                            batch.booking_id__o,
                            batch.content,
                            batch.date_created,
                            batch.last_updated,
                            batch.reservation_id)