-- transaction example..

-- create a table just for demo purposes
CREATE OR REPLACE TRANSIENT TABLE scratch.donaldgatfield.test_trn(
    col INT
)
;
INSERT INTO scratch.donaldgatfield.test_trn
SELECT 1
;

BEGIN TRANSACTION;

SHOW TRANSACTIONS ;

-- script goes here..
UPDATE scratch.donaldgatfield.test_trn SET col = 2 WHERE col = 1;
UPDATE scratch.donaldgatfield.test_trn SET col = 2 WHERE col = 2;
UPDATE scratch.donaldgatfield.test_trn SET col = 2 WHERE col = 3;
UPDATE scratch.donaldgatfield.test_trn SET col = 2 WHERE col = 4;


-- do your select in the other window..

-- if its no good rollback!
ROLLBACK;

-- otherwise commit!
--
COMMIT;