# JetBrains Data Engineering & Analysis Test Assignment

Ilona Míková

Used technology: AWS - Free tier, SQLServer, Excel, PowerBI



## Task 1: Data quality – the difference between NetSuite ERP and payment gateway
### Workflow description
The task was to transform given CSV files into queriable table and perform analysis of differences between these CSV files and `[dea].[netsuite].[TRANSACTION_LINES]` table. There are more way I could think of to approach this. They differ in complexity and suitability. 

- CSV files directly in SQL Server: 
  - assumption: CSV files have uniform and correct formatting - no ETL needed
  - easiest option leveraging SQL Server Import Wizard / Bulk Insert
  - produces unnecesary amount of tables - table for each csv file which then have to be unioned onto one
  - no scripting needed, minimal effort and setup
- CSV files in S3 + AWS Glue
  - assumption: no heavy ETL needed, CSV files have uniform and correct formatting
  - one-time or irregular load, small amount of data
  - leverages AWS Glue to extract data from S3, ETL and load into another S3 bucket using Athena, then load into SQL Server via Import Wizard/Bulk Insert
  - more real-case approach, leverages AWS storage, some configuration and setup needed
- CSV files in S3 + AWS Glue + Python for ETL
  - assumption: heavy ETL needed to be performed both on CSV formatting and data itself
  - comprehensive solution suited for regular load of big amount of data - complex pipeline
  - using AWS Glue with Python script to perform heavy ETL on the data, setting up Athena linked server to SQL Server 
  - requires the most setup and work, however robust and reusable solution 

I decided to go with the second option. The CSV files were ready to be used as they were and there was no need to prepare over-kill solution. I wanted to get myself familiar with AWS as I am a GCP developer. Also first option is pretty ugly so that is why I didn't go with the first option. 

### Workflow:
Use AWS Glue to extract and transform the data from CSV files into S3: 
  - created s3 bucket casestudy-jb
  - uploaded CSV files to bucket, folder csv_files
  - created Glue (GlueETLRole) IAM Role - AmazonS3ReadOnlyAccess, CloudWatchLogsFullAccess, AWSGlueServiceRole
  - created database casestudy-jb-db
  - created classifier (semicolon_csv_classsifier) - to ensure correct parsing of data; complicated formatting issues can be reprocessed with Python script

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

  - created Crawler (CSVCrawler) to create unioned table (csv_files) based on CSV files

Crawler configuration:
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
  - gross and Payment GW produces same numbers for split by merchant and batch_number
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
Differences between the two data sources were probably caused by the migration. There are some missing transactions in Netsuite for `JetBrainsEUR, batch_number IN (138, 139)` and `JetBrainsGBP, batch_number = 141`. There are some transactions in Netsuite missing batch_number (NULL) - `JetBrainsAmericasUSD, batch_number IN (139, 141)`. And there are even some differences caused by different transaction_date - `JetBrainsEUR, bacth_number = 140`. These were not visible by merchant and batch_number split. 

|Merchant Account|Batch Number|NetSuite|Payment Gateway|
| ------------- | ------ | ------ | ------ |
|JetBrainsAmericasUSD|139|11,255,775|11,255,775|
|JetBrainsAmericasUSD|141|11,042,873|11,042,873|
|JetBrainsEUR|138|0|83,251|
|JetBrainsEUR|139|0|-11,529|
|JetBrainsEUR|140|13,268,388|13,268,388|
|JetBrainsGBP|141|0|1,021,258|


To replicate comparison table by Merchant ID and Batch Number:
```
SELECT COALESCE(sr.MERCHANT_ACCOUNT, ns.MERCHANT_ACCOUNT) AS MERCHANT_ACCOUNT,
       COALESCE(sr.BATCH_NUMBER, ns.BATCH_NUMBER) AS BATCH_NUMBER,
	   COALESCE(ns.AMOUNT_FOREIGN, 0) AS NetSuite,
	   COALESCE(sr.PAYMENT_GATEWAY, 0) AS PAYMENT_GATEWAY,
	   COALESCE(ns.AMOUNT_FOREIGN, 0) - COALESCE(sr.PAYMENT_GATEWAY, 0) AS difference
FROM 
   (
        SELECT merchant_account AS MERCHANT_ACCOUNT
              ,batch_number AS BATCH_NUMBER
        	  ,SUM(gross) AS PAYMENT_GATEWAY
	    FROM [dbfour].[dbo].[settlement_reports]
        GROUP BY merchant_account
                ,batch_number
    ) sr
FULL JOIN
    (
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
SELECT COALESCE(ns.MERCHANT_ACCOUNT, sr.MERCHANT_ACCOUNT) AS MERCHANT_ACCOUNT
      ,COALESCE(sr.BATCH_NUMBER, ns.BATCH_NUMBER) AS BATCH_NUMBER
      ,SUM(COALESCE(ns.AMOUNT_FOREIGN, 0)) AS NETSUITE
      ,SUM(COALESCE(sr.PAYMENT_GATEWAY, 0)) AS PAYMENT_GATEWAY
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

Used script to create table `netsuite_reports_diffs`:
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


## Task 2: Sale analysis – revenue decline in ROW region
### Workflow
I wrote a SQL query to generate insights into the behaviour of the markets. I exported the result into CSV and used Excel with Pivot Table to investigate. I prepared a PowerBI report to visualize my findings. You can find it this repo - `RevenueAnalysis.pbix`

### Assumptions:
  I assumed with no way of checking that all relevant sources were included and data is complete. I also assumed that data correct - valid currency and rates etc.

### Inital checks and questions:
  - ordItm.amount_total = ordItm.price_item * ordItm.quantity
  - cou.currency = ord.currency
  - prod.price = ordItm.price
  - price of product is independent of region - YES (OrdItm checked)
  - price did not change during the two years
  - quantity did not dramamticcaly change during the two years

### Trend and Metrics analysis:
  - H1 performance diff: ROW(2018-2019) = 4645339.045 USD, US(2018-2019) = 142896 USD
  - monthly view:
    - to identify when the ROW decline started
    - ROW Sales significantly worse over whole H1 (diffs(2018H1-2019H1)=(708390.265, 1018517.446, 929517.9682, 979074.8374, 647085.1295, 362753.399)),
    - ROW Jun2019 best performing Sales
    - ROW no. of paid purchased products for 2019H1 higher than 2018H1 (is_paid = 1, SUM(quantity)), diff(2019H1-2018H1) = 4155
    - ROW QuantityByMonth(2018-2019) = (-1612, 1797, -3062, 173, -285, -1166)
    - US(Jan2019-Feb2019) slightly better than 2018; diffs(01to06)=(-16155, -90009, 41332, 143944, 5995, 57789)
    - US no. of paid purchased products for 2019H1 lower than 2018H1, diff(2019H1-2018H1) = -1116
    - US QuantityByMonth(2018-2019) = (-1023, -3186, 599, 4865, -2713, 2574)
    - ROW Sales2018 and Sales2019 have same trends by months 
    - monthly view doesn't suggest any causal event that would start the underperfomance 
  - split by products:
    - trends are across all products - it is not limited to only some products
    - most visible ROW Sales drop for II (IntelliJ IDEA), YT (YouTrack InCloud), All (All Products Pack)
    - as price and quantity remained the same the drop could be caused by purchasing cheaper products - refuted by exchange_rate analysis
  - split by license_type:
    - to see if ROW is purchasing more long-term licenses to have longer sales cycles
    - did not provide any insight
  - rates influence: 
    - adjusting 2019 revenue as if 2018 exchange rates were in effect - fixed rate insight
    - difference in performance vanished - now it even seems like ROW performed better than US
    - reasoning: due to unfavorable exchange rate movements rather than actual sales performance

### Outputs:
  - Quantity Analysis: 
    - no. of paid purchased products is comparable over the years 
    - and yet ROW Sales are lower 
    - and even for months with higher quantity sales
    - this points to possible change in market - people are purchasing same quantity of products just cheaper ones
  - Fixed Rate Analysis:
    - using exchange-rates from 2018 revealed that there is no such drop in performance as much as drop of economy resulting in unfavourable rates
  - Sales cycles or conversion rates may have influenced the difference in performace ROW vs. US - however not th edifferences by yearly

### Reasoning and Suggestions: 
The company’s revenue metric for ROW markets was impacted by currency devaluation in relation to USD. This means that the value of USD was reduced and so convertion local currencies to USD resulted in lower numbers. This is supported by the fact that actual sales volume and performance in local currencies remained strong.
I would suggest more localized point of view into reporting these numbers. Consider supplementing USD-based reporting with local currency reports, especially for ROW teams. Use FX Rate metrics to compare yearly and cross markets revenue metrics.

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
