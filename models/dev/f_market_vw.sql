WITH BASE AS (

    SELECT
        "Date",
        "Open", 
        "High", 
        "Low", 
        "Close", 
        "Adj_Close", 
        "Volume",
        NULL AS "Supply", 
        NULL AS "MarketCap", 
        NULL AS "Price",
        'Tesla' AS SOURCE
    FROM {{ ref('TSLA') }}

    UNION ALL

    SELECT 
        "Date", 
        "Open", 
        "High", 
        "Low", 
        "Close", 
        "Adj_Close", 
        "Volume",
        NULL AS  "Supply", 
        NULL AS  "MarketCap", 
        NULL AS  "Price",
        'Microsoft' AS SOURCE 
    FROM {{ ref('MSFT') }}

    UNION ALL

    SELECT 
        DATE("date") AS DATE, 
        "open",
        high,
        low,
        NULL AS CLOSE,
        close_last, 
        volume,
        NULL AS "Supply", 
        NULL AS "MarketCap", 
        NULL AS "Price",   
        'RobinHood' AS SOURCE
    FROM {{ ref('HOOD') }}

    UNION ALL

    SELECT 
        DATE ("Date(UTC)"),
        NULL AS "Open", 
        NULL AS "High", 
        NULL AS "Low", 
        NULL AS "Close", 
        NULL AS "Adj_Close", 
        NULL AS "Volume", 
        "Supply"::VARCHAR, 
        "MarketCap"::VARCHAR, 
        "Price"::VARCHAR,
        'Ethereum' AS SOURCE
    FROM {{ ref('ETHHIST') }}

    UNION ALL

    SELECT 
        DATE("date"),
        "open",
        high,
        low, 
        close_last,
        NULL AS "Adj_Close", 
        volume,   
        NULL AS "Supply", 
        NULL AS "MarketCap", 
        NULL AS "Price",   
        'Airbnb' AS SOURCE
    FROM {{ ref('ABNB') }}

    UNION ALL

    SELECT
        "Date", 
        "Open", 
        "High", 
        "Low", 
        "Close", 
        "Adj_Close", 
        "Volume",
        NULL AS "Supply", 
        NULL AS "MarketCap", 
        NULL AS "Price",   
        'Airbnb' AS SOURCE
    FROM {{ ref('AAPL') }}

),
FINAL AS (

    SELECT 
        "Date", 
        "Open", 
        "High", 
        "Low", 
        "Close", 
        "Adj_Close", 
        "Volume", 
        "Supply", 
        "MarketCap", 
        "Price", 
        "source"
    FROM BASE

)
SELECT * FROM FINAL