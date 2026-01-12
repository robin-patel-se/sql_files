CREATE OR REPLACE PROCEDURE scratch.robinpatel.test_email_proc(
                                                              )
    RETURNS string
    EXECUTE AS CALLER
AS
$$
    begin
        -- any query you'd like to run
        SELECT 'now' AS what, to_char(CURRENT_TIMESTAMP) AS VALUE
        UNION ALL
        SELECT 'upcoming', DAY || ' ' ||holiday
        FROM TABLE(fh_db.public.holidays('US', [YEAR(CURRENT_DATE), YEAR(CURRENT_DATE)+1]))
        WHERE DAY BETWEEN CURRENT_DATE() AND CURRENT_DATE()+60
        ;
        -- call the stored procedure that formats and emails the results
        CALL email_last_results('robin.patel@secretescapes.com', 'upcoming holidays');
        return 'done';
    END
$$
;
CALL scratch.robinpatel.test_email_proc;


------------------------------------------------------------------------------------------------------------------------

