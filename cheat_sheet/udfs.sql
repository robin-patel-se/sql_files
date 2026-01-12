CREATE OR REPLACE FUNCTION se.finance.user_reference_by_ecn(ecn INT, userreference VARCHAR
                                                  )
    RETURNS VARCHAR
    LANGUAGE SQL
AS
$$
SELECT CASE
           WHEN ecn = 410233
               AND userreference
                    IN (
                        'RENTAL CAR',
                        'Rental Cars',
                        'Rental cars',
                        'Rentalcars',
                        'rentalcars'
                       ) THEN 'RentalCars'
           END AS user_reference
$$
;

------------------------------------------------------------------------------------------------------------------------
--test optional arg

CREATE OR REPLACE FUNCTION scratch.robinpatel.test_udf(arg1 INT, arg2 VARCHAR)
    RETURNS VARCHAR
    LANGUAGE SQL
AS
$$
SELECT arg1 || arg2 as arguments
$$
;


SELECT scratch.robinpatel.test_udf(1, 'test')