# Dealpipe - a simple data pipeline experiment

## Requirements

Create a python program to read two flat files (excel and csv), validate them and to produce output files.

### Output File

The output should be the following files:
* Original file with extra columns including RowNo (A unique row identifier), AsOfDate (date/time of the processing), ProcessIdentifier (a unique process identifier), and RowHash (a calculated hash for each row). All extra fields should be placed at the end of the file except the RowNo which should be the starting field in each row.
* Parquet File (Parquet version of the above file)
* Error file (if there is any error involved in processing)

### Input File

Produce a series of input files that will test the logic in your python package. For example, a file containing character codes within a decimal field will produce an error file. The error file is to contain the reason for failure and the field and row on which it occurred.

1. CSV file - The input file should be a Deal list having 10 fields each with the following types:

    * 1 String field - for the Deal Name and it is mandatory
    * 5 Decimal fields (28,8) – any value is allowed (negative and positive) and you can name them D1 to D5
    * 1 Boolean field - indication Is Active flag with values Yes or No
    * 1 Country field (ISO standard) – create a lookup file with Code, Name
    * 1 Currency field (ISO standard) – create a lookup file with Code, Name
    * 1 Company field (Create a list of dummy companies with Id and Name ).
    * Make the company, currency, country, and one decimal field mandatory.

2. For Excel - Create two worksheets:
    * One containing the 10 fields specified above
    * The other containing the list of lookup codes (company, currency, and country)

The process should do the following validations:

* Validate data types
* Validate Country against a list
* Validate currency against a list
* Validate the company against the company lookup and include the company name in the output record too.

Expectation:

* The test can be done in a couple of hours, but we would like to see your quality of thinking, the best practices, and clean code so don't rush it
* The pipeline should have a fair set of unit tests representing different scenarios
* Give us a list of improvements and suggestions that can be done to make the code better.

## Implementation

Use the `dagster` python tool to implement a well-tested data pipeline that implements all requirements.