SELECT *
FROM se.data.crm_jobs_list cjl
WHERE cjl.send_id = 1198521


SELECT DISTINCT
       cjl.send_id,
       cjl.email_name,
       ces.data_source_name
FROM se.data.crm_jobs_list cjl
         LEFT JOIN se.data.crm_email_segments ces ON cjl.send_id = ces.send_id
WHERE cjl.sent_date = CURRENT_DATE - 1
  AND cjl.mapped_territory = 'DE';


SELECT *
FROM se.data.crm_jobs_list cjl
WHERE cjl.sent_date = CURRENT_DATE - 1
  AND cjl.mapped_territory = 'DE';

SELECT *
FROM se.data.crm_email_segments ces
WHERE ces.send_id IN ('1200137',
                      '1200141',
                      '1200145',
                      '1200149',
                      '1200153',
                      '1200197',
                      '1200201',
                      '1200205',
                      '1200217',
                      '1200228',
                      '1200233',
                      '1200238',
                      '1200241',
                      '1200242',
                      '1200245',
                      '1200253',
                      '1200254',
                      '1200259',
                      '1200260',
                      '1200261',
                      '1200268',
                      '1200278',
                      '1200288',
                      '1200294',
                      '1200298',
                      '1200304',
                      '1200305',
                      '1200308',
                      '1200312',
                      '1200314',
                      '1200318',
                      '1200319',
                      '1200322',
                      '1200328',
                      '1200332',
                      '1200336',
                      '1200340',
                      '1200344'
    )

SELECT *
FROM raw_vault_mvp.sfmc.jobs_sources js
WHERE js.jobid = 1200137

SELECT *
FROM se.data.crm_jobs_list cjl
         LEFT JOIN se.data.crm_email_segments ces ON cjl.send_id = ces.send_id
WHERE cjl.email_name = '20210214_DE_CORE_ATHENA_SundayBest';


SELECT *
FROM raw_vault_mvp.sfmc.jobs_sources js
WHERE js.jobid IN ('1199856',
                   '1200253',
                   '1200259',
                   '1200260',
                   '1200260',
                   '1200260',
                   '1200259',
                   '1200260',
                   '1200259',
                   '1200259',
                   '1200253',
                   '1200253',
                   '1200253',
                   '1200205'
    )