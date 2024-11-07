# JetBrains Data Engineering & Analysis Test Assignment

Ilona Míková

Used technology: AWS - Free tier, SQLServer, Excel, PowerBI



## Task 1: Data quality – the difference between NetSuite ERP and payment gateway
### Workflow description
The task was to transform given CSV files into queriable table in SQL Server and perform analysis on differences between said table and `[dea].[netsuite].[TRANSACTION_LINES]`. A list of all differences on transactional level was supposed to be delivered.

There are more ways I could think of to approach this. They differ in complexity and suitability. 

1. CSV files directly in SQL Server: 
  - assumption: CSV files have uniform and correct formatting - no ETL needed
  - **pros:** easiest option leveraging SQL Server Import Wizard / Bulk Insert, no scripting needed, minimal effort and setup of architecture/sw
  - **cons:** produces unnecesary amount of tables - table for each csv file which then have to be unioned onto one, no cost or optimization considered, not scalable, efficient or elegant

2. CSV files in S3 + AWS Glue
  - assumption: CSV files have uniform and correct formatting, no heavy ETL needed
  - more real-case approach, usable for one-time or irregular load, small amount of data
  - **pros:** leverages AWS storage and AWS Glue functionality, Glue and Athena are cost-effective for smaller amounts of data
  - **cons:** not robust and complex enough for handling data quality, reusability or automation

3. CSV files in S3 + AWS Glue + Full Scope Solution
  - assumption: heavy ETL needed to be performed both on CSV formatting and data itself, large dataset, automatization of pipeline
  - comprehensive solution suited for regular load of big amounts of data - complex pipeline
  - princip: using AWS Glue with Python script to perform heavy ETL on the data; setting up Athena linked server to SQL Server or using Redshift; adding explicit validations, error handling and logging along the way to promote robustness, using AWS Step Functions / Airflow for orchestration 
  - **pros:** robust, scalable, reusable solution for real-case problem
  - **cons:** requires the most setup and work

Data quality and validations have to be considered to make all of these options work. Let's highlight a few:
  - CSV formatting: UTF-8 encoding, LF endings, delimiter consistency
  - data handling: NULL values, duplicities, inconsistent data types
  - validations: count checks, value ranges
  - monitoring and logging
  - alerting in case of fails

I decided to go with the second option. The CSV files were ready to be used as they were and there was no need to prepare over-kill solution as is proposed by the third option. First option may be functional, however, it is insufficient on many stages.  Also I wanted to get myself familiar with AWS as I am a GCP developer. So getting to use another Cloud Platform was a challenge.


### Workflow:
Use AWS Glue to extract and transform the data from CSV files into S3: 
  - created s3 bucket casestudy-jb
  - uploaded CSV files to bucket, folder csv_files
  - created Glue (GlueETLRole) IAM Role - AmazonS3ReadOnlyAccess, CloudWatchLogsFullAccess, AWSGlueServiceRole
  - created database casestudy-jb-db
  - created classifier (semicolon_csv_classsifier) - to ensure correct parsing of data

```
{
    "Classifier": {
        "CsvClassifier": {
            "Name": "semicolon_csv_classifier",
            "CreationTime": "2024-11-05T11:11:39+00:00",
            "LastUpdated": "2024-11-05T11:11:39+00:00",
            "Version": 7,
            "Delimiter": ";",
            "QuoteSymbol": "'",
            "ContainsHeader": "PRESENT",
            "Header": [
                "COMPANY_ACCOUNT",
                "MERCHANT_ACCOUNT",
                "BATCH_NUMBER",
                "PSP_REFERENCE",
                "ORDER_REF",
                "PAYMENT_METHOD",
                "DATE",
                "TIMEZONE",
                "TYPE",
                "MODIFICATION_REFERENCE",
                "CURRENCY",
                "NET",
                "FEE",
                "GROSS"
            ],
            "DisableValueTrimming": true,
            "AllowSingleColumn": false,
            "CustomDatatypeConfigured": false,
            "CustomDatatypes": [],
            "Serde": "LazySimpleSerDe"
        }
    }
}
```

  - created Crawler (CSVCrawler) to get unioned table (csv_files) based on CSV files that is queriable in Athena

```
{
    "Crawler": {
        "Name": "CSVCrawler",
        "Role": "GlueETLRole",
        "Targets": {
            "S3Targets": [
                {
                    "Path": "s3://casestudy-jb/csv_files",
                    "Exclusions": [
                        "s3://casestudy-jb/unsaved"
                    ]
                }
            ],
            "JdbcTargets": [],
            "MongoDBTargets": [],
            "DynamoDBTargets": [],
            "CatalogTargets": [],
            "DeltaTargets": [],
            "IcebergTargets": [],
            "HudiTargets": []
        },
        "DatabaseName": "casestudy-jb-db",
        "Classifiers": [
            "semicolon_csv_classifier"
        ],
        "RecrawlPolicy": {
            "RecrawlBehavior": "CRAWL_EVERYTHING"
        },
        "SchemaChangePolicy": {
            "UpdateBehavior": "UPDATE_IN_DATABASE",
            "DeleteBehavior": "DEPRECATE_IN_DATABASE"
        },
        "LineageConfiguration": {
            "CrawlerLineageSettings": "DISABLE"
        },
        "State": "READY",
        "CrawlElapsedTime": 0,
        "CreationTime": "2024-11-05T10:18:23+00:00",
        "LastUpdated": "2024-11-05T14:42:23+00:00",
        "LastCrawl": {
            "Status": "SUCCEEDED",
            "LogGroup": "/aws-glue/crawlers",
            "LogStream": "CSVCrawler",
            "MessagePrefix": "d3314795-1023-48c3-aa6a-bf683f5587f9",
            "StartTime": "2024-11-05T14:45:53+00:00"
        },
        "Version": 9,
        "Configuration": "{\"Version\":1.0,\"CreatePartitionIndex\":true}",
        "LakeFormationConfiguration": {
            "UseLakeFormationCredentials": false,
            "AccountId": ""
        }
    }
}
```

Check data and save it back to S3 in a processed format:
  - data parsing
  - data values
  - row counts in line with csv files
  - gross and Payment Gateway produces same numbers for split by merchant_account and batch_number
  - use Athena to save query result into bucket (export_csv): `SELECT * FROM csv_files;`

Load table into SQL Server:
  - created table in `dbfour`:
```
CREATE TABLE settlement_reports (
    company_account VARCHAR(50),
    merchant_account VARCHAR(50),
    batch_number VARCHAR(50),
    psp_reference VARCHAR(50),
    order_ref VARCHAR(50),
    payment_method VARCHAR(50),
    date VARCHAR(50),
    timezone VARCHAR(50),
    type VARCHAR(50),
    modification_reference VARCHAR(50),
    currency VARCHAR(50),
    net DECIMAL(15,2),
    fee DECIMAL(15,2),
    gross DECIMAL(15,2)
);
```
  - imported data via Import Wizard

### Analysis
Differences between the two data sources were probably caused by the migration or full automatization of the process.

There are some missing transactions in Netsuite for `JetBrainsEUR, batch_number IN (138, 139)` and `JetBrainsGBP, batch_number = 141`. There are some transactions in Netsuite missing batch_number (NULL) - `JetBrainsAmericasUSD, batch_number IN (139, 141)`. And there are even some differences caused by different transaction_date - `JetBrainsEUR, bacth_number = 140`. These were not visible by merchant and batch_number split provided in the task specification. 

|Merchant Account|Batch Number|NetSuite|Payment Gateway|
| ------------- | ------ | ------ | ------ |
|JetBrainsAmericasUSD|139|11,255,775|11,255,775|
|JetBrainsAmericasUSD|141|11,042,873|11,042,873|
|JetBrainsEUR|138|0|83,251|
|JetBrainsEUR|139|0|-11,529|
|JetBrainsEUR|140|13,268,388|13,268,388|
|JetBrainsGBP|141|0|1,021,258|

List of all differences on transaction-level in stored in `netsuite_reports_diffs` table. It was created by following scripts:
```
CREATE TABLE dbo.netsuite_reports_diffs (
    TRANSACTION_ID VARCHAR(50),
    NETSUITE_TRANSACTION_TYPE VARCHAR(50),
    SETREP_TRANSACTION_TYPE VARCHAR(50),
    NETSUITE_PAYMENT_METHOD VARCHAR(50),
    NETSUITE_TRANDATE DATE,
    SETREP_TRANDATE DATE,
    NETSUITE_ACCOUNT_NAME VARCHAR(50),
    NETSUITE_ORDER_REF VARCHAR(50),
    SETREP_ORDER_REF VARCHAR(50),
    NETSUITE_MERCHANT_ACCOUNT VARCHAR(50),
    SETREP_MERCHANT_ACCOUNT VARCHAR(50),
    NETSUITE_BATCH_NUMBER NUMERIC,
    SETREP_BATCH_NUMBER NUMERIC,
    NETSUITE_SUBSIDIARY_NAME VARCHAR(50),
    NETSUITE_CURRENCY VARCHAR(3),
    SETREP_CURRENCY VARCHAR(3),
    NETSUITE_AMOUNT_FOREIGN DECIMAL(15,2),
    SETREP_PAYMENT_GATEWAY DECIMAL(15,2),
);

INSERT INTO dbo.netsuite_reports_diffs 
(
    TRANSACTION_ID,
    NETSUITE_TRANSACTION_TYPE,
    SETREP_TRANSACTION_TYPE,
    NETSUITE_PAYMENT_METHOD,
    NETSUITE_TRANDATE,
    SETREP_TRANDATE,
    NETSUITE_ACCOUNT_NAME,
    NETSUITE_ORDER_REF,
    SETREP_ORDER_REF,
    NETSUITE_MERCHANT_ACCOUNT,
    SETREP_MERCHANT_ACCOUNT,
    NETSUITE_BATCH_NUMBER,
    SETREP_BATCH_NUMBER,
    NETSUITE_SUBSIDIARY_NAME,
    NETSUITE_CURRENCY,
    SETREP_CURRENCY,
    NETSUITE_AMOUNT_FOREIGN,
    SETREP_PAYMENT_GATEWAY
)
SELECT ns.TRANSACTION_ID AS TRANSACTION_ID
      ,ns.TRANSACTION_TYPE AS NETSUITE_TRANSACTION_TYPE
      ,sr.TRANSACTION_TYPE AS SETREP_TRANSACTION_TYPE
      ,sr.PAYMENT_METHOD AS NETSUITE_PAYMENT_METHOD
      ,ns.TRANDATE AS NETSUITE_TRANDATE
      ,sr.TRANDATE AS SETREP_TRANDATE
      ,ns.ACCOUNT_NAME AS NETSUITE_ACCOUNT_NAME
      ,ns.ORDER_REF AS NETSUITE_ORDER_REF
      ,sr.ORDER_REF AS SETREP_ORDER_REF
      ,ns.MERCHANT_ACCOUNT AS NETSUITE_MERCHANT_ACCOUNT
      ,sr.MERCHANT_ACCOUNT AS SETREP_MERCHANT_ACCOUNT
      ,ns.BATCH_NUMBER AS NETSUITE_BATCH_NUMBER
      ,sr.BATCH_NUMBER AS SETREP_BATCH_NUMBER
      ,ns.SUBS_NAME AS NETSUITE_SUBSIDIARY_NAME
      ,ns.CURRENCY AS NETSUITE_CURRENCY
      ,sr.CURRENCY AS SETREP_CURRENCY
      ,ns.AMOUNT_FOREIGN AS NETSUITE_AMOUNT_FOREIGN
      ,sr.PAYMENT_GATEWAY AS SETREP_PAYMENT_GATEWAY
FROM 
    (
        SELECT trl.TRANSACTION_ID
              ,trl.AMOUNT_FOREIGN
              ,acc.NAME AS ACCOUNT_NAME
              ,trs.TRANSACTION_TYPE
              ,trs.TRANDATE AS TRANDATE
              ,trs.ORDER_REF
              ,trs.MERCHANT_ACCOUNT
              ,trs.BATCH_NUMBER
              ,sub.NAME AS SUBS_NAME
              ,cur.SYMBOL AS CURRENCY
        FROM [dea].[netsuite].[TRANSACTION_LINES] trl
        LEFT JOIN [dea].[netsuite].[ACCOUNTS] acc ON acc.ACCOUNT_ID = trl.ACCOUNT_ID
        LEFT JOIN [dea].[netsuite].[TRANSACTIONS] trs ON trs.TRANSACTION_ID = trl.TRANSACTION_ID
        LEFT JOIN [dea].[netsuite].[SUBSIDIARIES] sub ON sub.SUBSIDIARY_ID = trl.SUBSIDIARY_ID
        LEFT JOIN [dea].[netsuite].[CURRENCIES] cur ON cur.CURRENCY_ID = trs.CURRENCY_ID
        WHERE acc.ACCOUNTNUMBER IN ('315700', '315710', '315720', '315800', '548201')
    ) ns
FULL JOIN 
    (
        SELECT merchant_account AS MERCHANT_ACCOUNT
              ,batch_number AS BATCH_NUMBER
              ,order_ref AS ORDER_REF
              ,payment_method AS PAYMENT_METHOD
              ,date AS TRANDATE
              ,type AS TRANSACTION_TYPE
              ,currency AS CURRENCY
              ,CONCAT('JBCZ: Receivables against ADYEN-', currency) AS ACCOUNT_NAME
              ,NET AS PAYMENT_GATEWAY
        FROM [dbfour].[dbo].[settlement_reports]

        UNION ALL

        SELECT merchant_account AS MERCHANT_ACCOUNT
              ,batch_number AS BATCH_NUMBER
              ,order_ref AS ORDER_REF
              ,payment_method AS PAYMENT_METHOD
              ,date AS TRANDATE
              ,type AS TRANSACTION_TYPE
              ,currency AS CURRENCY
              ,'Other operating costs' AS ACCOUNT_NAME
              ,fee AS PAYMENT_GATEWAY
        FROM [dbfour].[dbo].[settlement_reports]
    ) sr
    ON ns.ORDER_REF = sr.ORDER_REF
    AND ns.TRANDATE = sr.TRANDATE -- can be commented out
    AND ns.ACCOUNT_NAME = sr.ACCOUNT_NAME
    AND ns.AMOUNT_FOREIGN = sr.PAYMENT_GATEWAY
WHERE ns.BATCH_NUMBER IS NULL OR sr.BATCH_NUMBER IS NULL;
```

### Sample Queries
To replicate comparison table by Merchant ID and Batch Number:
```
SELECT COALESCE(sr.MERCHANT_ACCOUNT, ns.MERCHANT_ACCOUNT) AS MERCHANT_ACCOUNT,
       COALESCE(sr.BATCH_NUMBER, ns.BATCH_NUMBER) AS BATCH_NUMBER,
       COALESCE(ns.AMOUNT_FOREIGN, 0) AS NetSuite,
       COALESCE(sr.PAYMENT_GATEWAY, 0) AS PAYMENT_GATEWAY,
       COALESCE(ns.AMOUNT_FOREIGN, 0) - COALESCE(sr.PAYMENT_GATEWAY, 0) AS difference
FROM 
   (--Settlement Reports part
      SELECT merchant_account AS MERCHANT_ACCOUNT
            ,batch_number AS BATCH_NUMBER
            ,SUM(gross) AS PAYMENT_GATEWAY
      FROM [dbfour].[dbo].[settlement_reports] -- exported table based on CSV files
      GROUP BY merchant_account
              ,batch_number
    ) sr
FULL JOIN
    (--NetSuite part
      SELECT trs.MERCHANT_ACCOUNT
            ,trs.BATCH_NUMBER
            ,SUM(trl.AMOUNT_FOREIGN) AS AMOUNT_FOREIGN
      FROM [dea].[netsuite].[TRANSACTION_LINES] trl
      LEFT JOIN [dea].[netsuite].[ACCOUNTS] acc ON acc.ACCOUNT_ID = trl.ACCOUNT_ID
      LEFT JOIN [dea].[netsuite].[TRANSACTIONS] trs ON trs.TRANSACTION_ID = trl.TRANSACTION_ID
      LEFT JOIN [dea].[netsuite].[SUBSIDIARIES] sub ON sub.SUBSIDIARY_ID = trl.SUBSIDIARY_ID
      LEFT JOIN [dea].[netsuite].[CURRENCIES] cur ON cur.CURRENCY_ID = trs.CURRENCY_ID
      WHERE acc.ACCOUNTNUMBER IN ('315700', '315710', '315720', '315800', '548201')
      GROUP BY trs.MERCHANT_ACCOUNT
              ,trs.BATCH_NUMBER
    ) ns
ON sr.merchant_account = ns.MERCHANT_ACCOUNT
  AND sr.batch_number = ns.BATCH_NUMBER
ORDER BY 1, 2;
```
  
To see the differences only:
```
-- NetSuite transactions
WITH NetSuiteTransactions AS (
    SELECT trl.TRANSACTION_ID
          ,trl.AMOUNT_FOREIGN
          ,acc.NAME AS ACCOUNT_NAME
          ,trs.TRANSACTION_TYPE
          ,trs.TRANDATE AS TRANDATE
          ,trs.ORDER_REF
          ,trs.MERCHANT_ACCOUNT
          ,trs.BATCH_NUMBER
          ,sub.NAME AS SUBS_NAME
          ,cur.SYMBOL AS CURRENCY
    FROM [dea].[netsuite].[TRANSACTION_LINES] trl
    LEFT JOIN [dea].[netsuite].[ACCOUNTS] acc ON acc.ACCOUNT_ID = trl.ACCOUNT_ID
    LEFT JOIN [dea].[netsuite].[TRANSACTIONS] trs ON trs.TRANSACTION_ID = trl.TRANSACTION_ID
    LEFT JOIN [dea].[netsuite].[SUBSIDIARIES] sub ON sub.SUBSIDIARY_ID = trl.SUBSIDIARY_ID
    LEFT JOIN [dea].[netsuite].[CURRENCIES] cur ON cur.CURRENCY_ID = trs.CURRENCY_ID
    WHERE acc.ACCOUNTNUMBER IN ('315700', '315710', '315720', '315800', '548201')
),

-- Settlement Reports based on CSV files, pivoting fee and net columns to PAYMENT_GATEWAY
SettlementReports AS (
    SELECT merchant_account AS MERCHANT_ACCOUNT
          ,batch_number AS BATCH_NUMBER
          ,order_ref AS ORDER_REF
          ,payment_method AS PAYMENT_METHOD
          ,date AS TRANDATE
          ,type AS TRANSACTION_TYPE
          ,currency AS CURRENCY
          ,CONCAT('JBCZ: Receivables against ADYEN-', currency) AS ACCOUNT_NAME
          ,net AS PAYMENT_GATEWAY
    FROM [dbfour].[dbo].[settlement_reports]

    UNION ALL

    SELECT merchant_account AS MERCHANT_ACCOUNT
          ,batch_number AS BATCH_NUMBER
          ,order_ref AS ORDER_REF
          ,payment_method AS PAYMENT_METHOD
          ,date AS TRANDATE
          ,type AS TRANSACTION_TYPE
          ,currency AS CURRENCY
          ,'Other operating costs' AS ACCOUNT_NAME
          ,fee AS PAYMENT_GATEWAY
    FROM [dbfour].[dbo].[settlement_reports]
)

-- Combine NetSuite and Settlement Report data
SELECT COALESCE(ns.MERCHANT_ACCOUNT, sr.MERCHANT_ACCOUNT) AS MERCHANT_ACCOUNT
      ,COALESCE(sr.BATCH_NUMBER, ns.BATCH_NUMBER) AS BATCH_NUMBER
      ,SUM(COALESCE(ns.AMOUNT_FOREIGN, 0)) AS NETSUITE
      ,SUM(COALESCE(sr.PAYMENT_GATEWAY, 0)) AS PAYMENT_GATEWAY
FROM NetSuiteTransactions ns
FULL JOIN SettlementReports sr
    ON ns.ORDER_REF = sr.ORDER_REF
    AND ns.TRANDATE = sr.TRANDATE -- Optional condition, can be commented out if transaction date discrepancies are not relevant
    AND ns.ACCOUNT_NAME = sr.ACCOUNT_NAME
    AND ns.AMOUNT_FOREIGN = sr.PAYMENT_GATEWAY
WHERE ns.BATCH_NUMBER IS NULL OR sr.BATCH_NUMBER IS NULL
GROUP BY COALESCE(ns.MERCHANT_ACCOUNT, sr.MERCHANT_ACCOUNT)
        ,COALESCE(sr.BATCH_NUMBER, ns.BATCH_NUMBER)
ORDER BY 1, 2;

```

Or using created table:
```
SELECT COALESCE(NETSUITE_MERCHANT_ACCOUNT, SETREP_MERCHANT_ACCOUNT) AS MERCHANT_ACCOUNT
      ,COALESCE(SETREP_BATCH_NUMBER, NETSUITE_BATCH_NUMBER) AS BATCH_NUMBER
      ,SUM(COALESCE(NETSUITE_AMOUNT_FOREIGN, 0)) AS NETSUITE
      ,SUM(COALESCE(SETREP_PAYMENT_GATEWAY, 0)) AS PAYMENT_GATEWAY
FROM [dbo].[netsuite_reports_diffs]
GROUP BY COALESCE(NETSUITE_MERCHANT_ACCOUNT, SETREP_MERCHANT_ACCOUNT)
      ,COALESCE(SETREP_BATCH_NUMBER, NETSUITE_BATCH_NUMBER)
```


## Task 2: Sale analysis – revenue decline in ROW region
The goal was to generate insight and provide explanation of revenue decline in ROW region happening in H1 for comparing Y2018 and Y2019. 

**Inicial hypothesis were:**
  - causal event - e.g. missing sources for revenue, real-world infuence - bans etc.
  - currency devaluation
  - Market behaviour - sales cycles, ...

**Conclusion:**
The company’s revenue metric for ROW markets was impacted by currency devaluation in relation to USD. This means that the value of USD was reduced and so convertion of local currencies to USD resulted in lower revenue metric. This is supported by the fact that actual sales volume and performance in local currencies remained strong.
I would suggest more localized point of view into reporting these numbers. Consider supplementing USD-based reporting with local currency reports, especially for ROW teams. Use FX Rate metrics to compare yearly and cross markets revenue metrics.


### Workflow
I wrote a SQL query to generate insights into the behaviour of the markets. I exported the result into CSV and used Excel (Pivot Table) to investigate. I prepared a PowerBI report to visualize my findings. You can find it this repo - `RevenueAnalysis.pbix`

I assumed with no way of checking that all relevant sources were included and data is complete. I also assumed that data is correct - valid currency and reliable rates, actual prices, ...

Inital checks and questions:
  - ordItm.amount_total = ordItm.price_item * ordItm.quantity
  - cou.currency = ord.currency
  - prod.price = ordItm.price
  - price of product is independent of region - YES (OrdItm checked)
  - price did not change during the two years
  - quantity did not dramamticcaly change during the two years

### Deepdive analysis:
  - H1 performance diff: ROW(2018-2019) = 4645339.045 USD, US(2018-2019) = 142896 USD
  - **monthly view:**
    - to identify when the ROW decline started
    - ROW Sales significantly worse over whole H1 - no sudden drop, up to 8.5% monthly decrease in performance,
    - ROW Jun2019 best performing SalesUSD
    - ROW no. of paid purchased products for 2019H1 slightly higher than 2018H1 
    - US Sales for Jan2019 and Feb2019 slightly better than 2018; up to 2% monthly decrease in performance
    - US no. of paid purchased products for 2019H1 slightly lower than 2018H1
    - ROW Sales2018 and Sales2019 have same seasonal trends 
    - monthly view doesn't suggest any causal event that would start the underperfomance 
  - **split by products:**
    - trends are visible across all products - it is not limited to only some products
    - most noticible ROW Sales drop for II (IntelliJ IDEA), YT (YouTrack InCloud), All (All Products Pack)
    - as price and quantity of sold products remained the same the decrease could be caused by purchasing cheaper products - choosing shorter licenses maybe
  - **split by license_type:**
    - to see if ROW is purchasing more long-term licenses to have longer sales cycles
    - did not provide insight supporting any hypothesis
  - **rates influence:** 
    - adjusting 2019 revenue as if 2018 exchange rates were in effect - fixed rate insight
    - difference in performance vanished - now it even seems like ROW performed better than US
    - reasoning: due to unfavorable exchange rate movements rather than actual sales performance

### Summary:
  - Seasonal and Trend Analysis: 
    - no. of paid purchased products is comparable over the years for both markets- max. diviance is 2% for US, less than 1% for ROW 
    - and yet ROW SalesUSD are more than 8% lower than previous H1 
    - and even for months with higher quantity sales than previous year
  - Fixed Rate Analysis:
    - using exchange-rates from 2018 revealed that there is no such drop in performance as much as drop of economy resulting in unfavourable rates for USD
  - Sales cycles or conversion rates may have influenced the difference in performace ROW vs. US - but there is not enough information in the data to analyze this


### SQL Query used for analysis
All insight was generated based on this query:
```
SELECT cou.REGION
      ,ord.CURRENCY AS ORD_CURRENCY
      ,MONTH(ord.EXEC_DATE) AS MONTH_NUM
      ,prod.NAME AS PROD_NAME
      ,ordItm.PRICE_ITEM AS ORDITM_PRICE
      ,ordItm.LICENSE_TYPE
      ,SUM(CASE WHEN YEAR(ord.EXEC_DATE) = 2018 THEN ordItm.AMOUNT_TOTAL / exRate.RATE ELSE 0.00 END) AS SalesUsd2018
      ,SUM(CASE WHEN YEAR(ord.EXEC_DATE) = 2019 THEN ordItm.AMOUNT_TOTAL / exRate.RATE ELSE 0.00 END) AS SalesUsd2019
      ,SUM(CASE WHEN YEAR(ord.EXEC_DATE) = 2019 THEN ordItm.AMOUNT_TOTAL / exRatePrev.RATE ELSE 0.00 END) AS SalesUsd2019FXRate
      ,SUM(CASE WHEN YEAR(ord.EXEC_DATE) = 2018 THEN ordItm.AMOUNT_TOTAL ELSE 0.00 END) AS Sales2018
      ,SUM(CASE WHEN YEAR(ord.EXEC_DATE) = 2019 THEN ordItm.AMOUNT_TOTAL ELSE 0.00 END) AS Sales2019
      ,SUM(CASE WHEN YEAR(ord.EXEC_DATE) = 2018 THEN ordItm.QUANTITY ELSE 0 END) AS NoOrds2018
      ,SUM(CASE WHEN YEAR(ord.EXEC_DATE) = 2019 THEN ordItm.QUANTITY ELSE 0 END) AS NoOrds2019
FROM [dea].[sales].[Orders] ord
JOIN [dea].[sales].[OrderItems] ordItm ON ord.ID = ordItm.ORDER_ID
JOIN [dea].[sales].[Customer] cust ON ord.CUSTOMER = cust.ID
JOIN [dea].[sales].[Country] cou ON cust.COUNTRY_ID = cou.ID
JOIN [dea].[sales].[ExchangeRate] exRate ON ord.EXEC_DATE = exRate.DATE AND ord.CURRENCY = exRate.CURRENCY
JOIN [dea].[sales].[ExchangeRate] exRatePrev ON FORMAT(ord.EXEC_DATE, '2018-MM-dd') = exRatePrev.DATE AND ord.CURRENCY = exRatePrev.CURRENCY
JOIN [dea].[sales].[Product] prod ON ordItm.PRODUCT_ID = prod.PRODUCT_ID
WHERE ord.IS_PAID = 1 -- Only paid orders, exclude pre-orders
   AND YEAR(ord.EXEC_DATE) IN (2018, 2019) -- Year 2018, 2019
  AND MONTH(ord.EXEC_DATE) BETWEEN 1 AND 6 -- H1
GROUP BY cou.REGION
        ,ord.CURRENCY
        ,MONTH(ord.EXEC_DATE)
        ,prod.NAME
        ,ordItm.PRICE_ITEM
        ,ordItm.LICENSE_TYPE;
```
