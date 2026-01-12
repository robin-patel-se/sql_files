CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.loop_test(
    date_insert DATE
);

CREATE OR REPLACE PROCEDURE scratch.robinpatel.seven_day_loop(start_date string, num_runs DOUBLE)
    RETURNS VARCHAR
    LANGUAGE JAVASCRIPT
    RETURNS NULL ON NULL INPUT
AS
$$
var i;
for (i = 0; i < NUM_RUNS; i++) {
    var sql_command = `SELECT '''' || TO_CHAR(DATEADD(DAY, ${i}*7, TO_DATE('${START_DATE}'))) || ''''`;
    var stmt = snowflake.createStatement( {sqlText: sql_command} );
    var res = stmt.execute();
    res.next()
    var date_var = res.getColumnValue(1);
    var sql_command =
        `
        INSERT INTO scratch.robinpatel.loop_test
            SELECT ${date_var}
        ;
        `;

    var stmt = snowflake.createStatement( {sqlText: sql_command} );
    stmt.execute();
};
$$;


CALL scratch.robinpatel.seven_day_loop('2023-01-01', 3);

SELECT * FROM scratch.robinpatel.loop_test;

