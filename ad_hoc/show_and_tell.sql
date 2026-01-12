--crm performance
SELECT *
FROM se.data_pii.crm_events_sends;
SELECT *
FROM se.data_pii.crm_events_opens;
SELECT *
FROM se.data_pii.crm_events_clicks;
SELECT *
FROM se.data_pii.crm_jobs_list;
SELECT *
FROM se.data_pii.crm_email_segments;

-- v4 api, salesforce opportunity, promotion
SELECT *
FROM se.data.se_sale_attributes ssa;

--cancellation information included
SELECT *
FROM se.data.se_booking sb;

--athena email reporting
SELECT *
FROM data_vault_mvp.dwh.athena_email_reporting;

SELECT *
FROM data_vault_mvp.dwh.athena_email_reporting aer
WHERE aer.se_sale_id = 'A12499'
  AND aer.send_id = 1175561

SELECT device_platform, count(*)
FROM se.data.se_booking sb
GROUP BY 1;

SELECT * FROM hygiene_vault_mvp_dev_robin.cms_mongodb.booking_summary bs WHERE bs.device_platform = 'not specified';

------------------------------------------------------------------------------------------------------------------------
--2020-12-02
--athena email reporting
SELECT *
FROM se.data.athena_email_reporting aer;

--transaction model

--fact booking
SELECT *
FROM se.data.fact_booking fb

--demand model

