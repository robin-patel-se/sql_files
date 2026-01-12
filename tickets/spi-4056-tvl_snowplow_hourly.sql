self_describing_task --include 'biapp/task_catalogue/staging/outgoing/travelist/atomic_events_hourly/modelling.py'  --method 'run' --start '2023-08-29 00:00:00' --end '2023-08-29 00:00:00'
dataset_task --include 'travelist.atomic_events_hourly' --operation UnloadOperation --method 'run' --start '2023-08-29 00:00:00' --end '2023-08-29 00:00:00'
dataset_task --include 'travelist.atomic_events_hourly' --operation DistributeOperation --method 'run' --start '2023-08-29 00:00:00' --end '2023-08-29 00:00:00'

SELECT *
FROM unload_vault_mvp_dev_robin.travelist.atomic_events__20230828t020000__daily_at_02h00
;

         SELECT
                COUNT(*)
            FROM unload_vault_mvp_dev_robin.travelist.atomic_events_hourly__20230828T020000__daily_at_02h00;

