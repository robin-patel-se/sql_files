/*
Step 1: Export from Google Slides
You need to convert your presentation into a format Snowflake can "read" (PDF is the gold standard for this).

Open your Trading Deck in Google Slides.

Go to File > Download > PDF Document (.pdf).

Pro-Tip: If you have detailed analysis in the Speaker Notes, it is better to go to File > Print settings and preview > Select "1 slide with notes" > Click Download as PDF. This ensures SnowMind gets the "narrative" as well as the charts.

Step 2: Create a Stage in Snowflake
If you don't already have a dedicated place for these decks, create a "Stage" (think of it as a secure folder inside Snowflake).

Run this in a Snowflake Worksheet:

SQL

-- Create a secure internal stage for your trading decks
CREATE OR REPLACE STAGE SE.DATA.TRADING_DECKS
DIRECTORY = (ENABLE = TRUE) -- Important: Enables SnowMind to "see" the files
ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE'); -- Enables server-side reading for AI tools
Step 3: Upload using Snowsight (UI)
You don’t need to write code to upload the files; you can use the Snowflake interface:

In the left-hand navigation menu of Snowsight, go to Data > Databases.

Drill down into your database (SE) and schema (DATA).

Select Stages and click on your new TRADING_DECKS stage.

In the top right corner, click the + Files button.

Drag and drop your exported PDFs into the window and click Upload.

Step 4: Verify the Upload
To make sure Snowflake has registered the files and can see them for search indexing, run:

SQL

-- List the files in your stage
LIST @SE.DATA.TRADING_DECKS;

-- Refresh the directory so the AI tools can find the new files
ALTER STAGE SE.DATA.TRADING_DECKS REFRESH;
What's next?
Now that the files are physically in Snowflake, we need to do the "Step 2" I mentioned earlier—running the PARSE_DOCUMENT script so SnowMind can actually read the text inside those PDFs and answer questions about them.

Would you like me to help you set up the "Auto-Parser" task so any new deck you upload is automatically read by SnowMind?
*/


-- Create a secure internal stage for your trading decks
CREATE OR REPLACE STAGE scratch.robinpatel.trading_decks DIRECTORY = (ENABLE = TRUE) -- Important: Enables SnowMind to "see" the files
ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
;

-- Enables server-side reading for AI tools


-- List the files in your stage
LIST @SCRATCH.ROBINPATEL.TRADING_DECKS
;

-- Refresh the directory so the AI tools can find the new files
ALTER STAGE se.data.trading_decks REFRESH
;


/*
Step 2: Transform with Cortex AI
Once the files are in a stage, you need to "read" them. Snowflake has a built-in function called PARSE_DOCUMENT that can extract text and layout from your slides.

Run this to create a searchable table of your slides:

SQL

CREATE OR REPLACE TABLE SE.DATA.TRADING_DECKS_TEXT AS
SELECT
    RELATIVE_PATH AS FILE_NAME,
    SNOWFLAKE.CORTEX.PARSE_DOCUMENT(
        '@SE.DATA.TRADING_DECKS',
        RELATIVE_PATH,
        {'mode': 'LAYOUT'}
    ):content::VARCHAR AS DECK_CONTENT
FROM DIRECTORY(@SE.DATA.TRADING_DECKS);
*/

TRUNCATE scratch.robinpatel.trading_decks_text;
USE WAREHOUSE pipe_xlarge;
CREATE OR REPLACE TABLE scratch.robinpatel.trading_decks_text AS
SELECT
	relative_path                               AS file_name,
	snowflake.cortex.parse_document(
			'@SCRATCH.ROBINPATEL.TRADING_DECKS',
			relative_path,
		{'mode': 'LAYOUT'}
    ):content::VARCHAR AS deck_content
FROM directory(@SCRATCH.ROBINPATEL.TRADING_DECKS)
;

/*
Step 3: Connect to SnowMind
Now, create a Cortex Search Service on that table and add it to your SnowMind Agent DDL under tools.

Best Practice for Trading Decks
Trading decks are often heavy on charts and tables. To ensure SnowMind understands them:

Use Speaker Notes: SnowMind can "read" the speaker notes if you export the slides as a text-heavy PDF or use a script to include them. This is often where the best "narrative" context lives.

Add Metadata: When you create your table in Step 2, add columns for Region, Fiscal_Quarter, and Deck_Type (e.g., 'Weekly Trading' vs 'Quarterly Review').

Update the Agent Instructions: Tell SnowMind: "When asked about historical context or trading commentary, refer to the 'trading_decks' tool."

*/

SELECT * FROM scratch.robinpatel.trading_decks_text

CREATE OR REPLACE
CORTEX SEARCH SERVICE SE.DATA.TRADING_DECKS_SEARCH_SERVICE
  ON DECK_CONTENT                  -- The column containing the slide text
  ATTRIBUTES FILE_NAME             -- Allows filtering by specific decks
  WAREHOUSE = <YOUR_WAREHOUSE_NAME> -- e.g., COMPUTE_WH
  TARGET_LAG = '1 hour'            -- How often to sync new uploads
  AS (
      SELECT
          FILE_NAME,
          DECK_CONTENT
      FROM SCRATCH.ROBINPATEL.TRADING_DECKS_TEXT
  );

