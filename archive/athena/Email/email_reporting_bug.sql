SELECT * FROm data_vault_mvp.dwh.athena_email_reporting__sales_in_send aersis
WHERE aersis.send_date = '2021-04-22'


            --get a list of all sales that are included in a send
            --content varies from email to email based on the recipient
            SELECT DISTINCT
                asl.deal_id AS se_sale_id,
                asl.job_id AS send_id,
                asl.log_date::DATE AS send_date,
                cjl.email_name,
                cjl.mapped_territory,
                js.data_source_name,
                IFF(ROW_NUMBER() OVER (PARTITION BY asl.job_id, asl.subscriber_key ORDER BY asl.section, asl.position_in_section) <= 10,
                        'inside top 10', 'outside top 10') AS sale_position_group,
                ss.sale_name,
                ss.company_name,
                ss.start_date::DATE AS start_date,
                ss.end_date::DATE AS end_date,
                ss.sale_type,
                ss.sale_product,
                ss.destination_type,
                ss.posu_city,
                ss.posu_country,
                ss.posu_division
            FROM hygiene_snapshot_vault_mvp.sfmc.athena_send_log asl
                LEFT JOIN hygiene_snapshot_vault_mvp.sfmc.jobs_list cjl ON asl.job_id = cjl.send_id
                INNER JOIN hygiene_snapshot_vault_mvp.sfmc.jobs_sources  js ON asl.job_id = js.send_id
                    AND asl.subscriber_key::VARCHAR = js.subscriber_key::VARCHAR
                LEFT JOIN data_vault_mvp.dwh.se_sale ss ON asl.deal_id = ss.se_sale_id
            WHERE
                asl.job_id = 1214194;


SELECT aersis.send_date::DATE,
       count(*)
FROM data_vault_mvp.dwh.athena_email_reporting__sales_in_send aersis
GROUP BY 1;


select * from se.data.athena_email_reporting where send_date = '2021-04-22';

--found late arriving events (loaded at) reran job and all apeared