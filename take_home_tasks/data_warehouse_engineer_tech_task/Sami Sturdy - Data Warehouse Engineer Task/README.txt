The SQL contained in the files with the prefix 'prereq_' will need to be run first to create views which are used in the final tasks.

The code in prereq_ddl.sql was used to create the tables for bookings, members, and events, but the Postgres import wizard was used to import the data from the CSVs into those tables.

Once the prerequisites views are created, the SQL in the files with the 'task_' prefixes can be run to view the output for the tasks.

Prereq 1 is needed for all tasks and only contains DDL.
Prereq 2 is needed for all tasks and contains sessionisation logic.
Prereq 3 is needed for task 2 and contains deduping logic for bookings data. 
Prereq 4 is needed for task 3 and contains logic for deduping members data and assigning age buckets.

Answers to the 'if you had more time' questions can be found in the answers.txt file.