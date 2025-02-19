I am using PostgresSQL in this case study

Steps to run the code
- Make sure you have docker desktop installed
-Clone the repo
- Run docker-compose up, This will start PostgreSQL and any other required services
- Then run the db_setup.py python file. This script creates the necessary databases and connects to the PostgreSQL database.

Part 1
  - Data Model Diagram (ERD)
  - Table Creation: Tables are created in PostgreSQL.
  - Data Cleaning & Loading: Most data quality issues are addressed here before ingestion.

Part 2
 - Answers to six business questions.
 - Limitation: The second question couldn’t be answered due to only one month of scanned_date data. There's no prior data for comparison after cleaning.

Part 3
 - Identification of major data quality issues in the dataset.
 - Highlighting potential gaps in the data.


Part 4
 - Email draft to an executive stakeholder summarizing:
    Data issues and concerns
    Questions for further investigation
    Suggestions for resolution
    Optimization opportunities
    Performance & scaling concerns for future growth

The .env file is not removed to ensure easy setup and execution.
