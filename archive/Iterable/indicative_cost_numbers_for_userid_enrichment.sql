SELECT *
FROM raw_vault_mvp.sfmc.events_sends es
WHERE es.loaded_at::DATE = CURRENT_DATE - 1;

--loaded at:
--2021-11-04

--8,926,980 events in that one day

USE WAREHOUSE pipe_default;

--create a temp table that just has email address
CREATE OR REPLACE TABLE scratch.robinpatel.iterable_user_id_test AS (
    SELECT es.*,
           ua.email
    FROM raw_vault_mvp.sfmc.events_sends es
        INNER JOIN data_vault_mvp.dwh.user_attributes ua ON TRY_TO_NUMBER(es.subscriber_key) = ua.shiro_user_id
    WHERE es.loaded_at::DATE = CURRENT_DATE - 1
);


CREATE OR REPLACE TABLE scratch.robinpatel.iterable_user_id_test_enriching AS (
    SELECT es.*,
           ua.shiro_user_id
    FROM scratch.robinpatel.iterable_user_id_test es
        INNER JOIN data_vault_mvp.dwh.user_attributes ua ON es.email = ua.email
    WHERE es.loaded_at::DATE = CURRENT_DATE - 1
);
--Query id: 01a01585-3200-fa75-0000-02ddbbc4cdca
--19 seconds on pipe default


SELECT COUNT(*) FROM scratch.robinpatel.iterable_user_id_test_enriching; -- 8,191,342
