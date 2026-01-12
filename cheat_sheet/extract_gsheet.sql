https://github.com/secretescapes/one-data-pipeline/wiki/data-pipeline--bau--creating-a-gsheet-extract-ingest-operation


--extract gsheet to s3
dataset_task \
--include 'fpa_gsheets.posu_categorisations*' \
--operation ExtractOperation \
--method 'run' \
--start '2020-08-20 00:00:00' \
--end '2020-08-20 00:00:00'

--load s3 to transient table
dataset_task \
--include 'fpa_gsheets.posu_categorisations*' \
--operation RawIngestOperation \
--method 'run' \
--start '2020-08-20 00:00:00' \
--end '2020-08-20 00:00:00'

--load transient table to raw_vault table
dataset_task \
--include 'fpa_gsheets.posu_categorisations*' \
--operation ProductionIngestOperation \
--method 'run' \
--start '2020-08-20 00:00:00' \
--end '2020-08-20 00:00:00'