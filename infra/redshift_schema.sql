-- ForeSite Analytics — Redshift star schema
-- Executed statement-by-statement via the Redshift Data API in infra/setup.py

CREATE TABLE IF NOT EXISTS dim_geography (
    geography_id    INT IDENTITY(1,1) PRIMARY KEY,
    cma_name        VARCHAR(100)    NOT NULL UNIQUE,
    province        VARCHAR(50)     NOT NULL,
    cma_code        VARCHAR(10)
)
~~~
INSERT INTO dim_geography (cma_name, province, cma_code)
SELECT src.cma_name, src.province, src.cma_code FROM (
    SELECT 'Calgary' AS cma_name, 'Alberta' AS province, '825' AS cma_code UNION ALL
    SELECT 'Charlottetown', 'Prince Edward Island', '105' UNION ALL
    SELECT 'Edmonton', 'Alberta', '835' UNION ALL
    SELECT 'Halifax', 'Nova Scotia', '205' UNION ALL
    SELECT 'Montreal', 'Quebec', '462' UNION ALL
    SELECT 'Ottawa-Gatineau', 'Ontario/Quebec', '505' UNION ALL
    SELECT 'Quebec City', 'Quebec', '421' UNION ALL
    SELECT 'Regina', 'Saskatchewan', '705' UNION ALL
    SELECT 'Saint John', 'New Brunswick', '310' UNION ALL
    SELECT 'Saskatoon', 'Saskatchewan', '725' UNION ALL
    SELECT 'St. John''s', 'Newfoundland and Labrador', '001' UNION ALL
    SELECT 'Thunder Bay', 'Ontario', '595' UNION ALL
    SELECT 'Toronto', 'Ontario', '535' UNION ALL
    SELECT 'Vancouver', 'British Columbia', '933' UNION ALL
    SELECT 'Victoria', 'British Columbia', '935' UNION ALL
    SELECT 'Winnipeg', 'Manitoba', '602' UNION ALL
    SELECT 'Canada', 'Canada', '000'
) src
WHERE NOT EXISTS (SELECT 1 FROM dim_geography WHERE cma_name = src.cma_name)
~~~
CREATE TABLE IF NOT EXISTS dim_date (
    date_id     INT PRIMARY KEY,
    year        SMALLINT    NOT NULL,
    month       SMALLINT    NOT NULL,
    quarter     SMALLINT,
    is_annual   BOOLEAN     NOT NULL DEFAULT FALSE
)
~~~
INSERT INTO dim_date (date_id, year, month, quarter, is_annual)
SELECT
    yr * 100 + mo,
    yr,
    mo,
    CASE WHEN mo BETWEEN 1 AND 3 THEN 1 WHEN mo BETWEEN 4 AND 6 THEN 2 WHEN mo BETWEEN 7 AND 9 THEN 3 ELSE 4 END,
    FALSE
FROM (
    SELECT 2015 AS yr UNION ALL SELECT 2016 UNION ALL SELECT 2017 UNION ALL SELECT 2018 UNION ALL
    SELECT 2019 UNION ALL SELECT 2020 UNION ALL SELECT 2021 UNION ALL SELECT 2022 UNION ALL
    SELECT 2023 UNION ALL SELECT 2024 UNION ALL SELECT 2025 UNION ALL SELECT 2026 UNION ALL
    SELECT 2027 UNION ALL SELECT 2028 UNION ALL SELECT 2029 UNION ALL SELECT 2030
) years
CROSS JOIN (
    SELECT 1 AS mo UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL
    SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL
    SELECT 9 UNION ALL SELECT 10 UNION ALL SELECT 11 UNION ALL SELECT 12
) months
WHERE NOT EXISTS (SELECT 1 FROM dim_date WHERE date_id = yr * 100 + mo)
~~~
INSERT INTO dim_date (date_id, year, month, quarter, is_annual)
SELECT yr * 100, yr, 0, NULL, TRUE
FROM (
    SELECT 2015 AS yr UNION ALL SELECT 2016 UNION ALL SELECT 2017 UNION ALL SELECT 2018 UNION ALL
    SELECT 2019 UNION ALL SELECT 2020 UNION ALL SELECT 2021 UNION ALL SELECT 2022 UNION ALL
    SELECT 2023 UNION ALL SELECT 2024 UNION ALL SELECT 2025 UNION ALL SELECT 2026 UNION ALL
    SELECT 2027 UNION ALL SELECT 2028 UNION ALL SELECT 2029 UNION ALL SELECT 2030
) years
WHERE NOT EXISTS (SELECT 1 FROM dim_date WHERE date_id = yr * 100)
~~~
CREATE TABLE IF NOT EXISTS dim_indicator (
    indicator_id    INT IDENTITY(1,1) PRIMARY KEY,
    indicator_name  VARCHAR(200)    NOT NULL,
    source          VARCHAR(50)     NOT NULL,
    unit            VARCHAR(50)     NOT NULL,
    category        VARCHAR(100)    NOT NULL,
    frequency       VARCHAR(10)     NOT NULL,
    UNIQUE (indicator_name, source)
)
~~~
INSERT INTO dim_indicator (indicator_name, source, unit, category, frequency)
SELECT src.indicator_name, src.source, src.unit, src.category, src.frequency FROM (
    SELECT 'CPI - All-items' AS indicator_name, 'StatCan' AS source, 'Index (2002=100)' AS unit, 'CPI' AS category, 'monthly' AS frequency UNION ALL
    SELECT 'CPI - Food', 'StatCan', 'Index (2002=100)', 'CPI', 'monthly' UNION ALL
    SELECT 'CPI - Shelter', 'StatCan', 'Index (2002=100)', 'CPI', 'monthly' UNION ALL
    SELECT 'CPI - Transportation', 'StatCan', 'Index (2002=100)', 'CPI', 'monthly' UNION ALL
    SELECT 'CPI - Energy', 'StatCan', 'Index (2002=100)', 'CPI', 'monthly' UNION ALL
    SELECT 'Food price - Bread (per 675g)', 'StatCan', 'CAD', 'Food', 'monthly' UNION ALL
    SELECT 'Food price - Milk (per 2L)', 'StatCan', 'CAD', 'Food', 'monthly' UNION ALL
    SELECT 'Food price - Eggs (per dozen)', 'StatCan', 'CAD', 'Food', 'monthly' UNION ALL
    SELECT 'Gasoline price (per litre)', 'StatCan', 'Cents/Litre', 'Energy', 'monthly' UNION ALL
    SELECT 'NHPI - Total', 'StatCan', 'Index (201612=100)', 'Housing', 'monthly' UNION ALL
    SELECT 'NHPI - House only', 'StatCan', 'Index (201612=100)', 'Housing', 'monthly' UNION ALL
    SELECT 'NHPI - Land only', 'StatCan', 'Index (201612=100)', 'Housing', 'monthly' UNION ALL
    SELECT 'Avg rent - Bachelor', 'CMHC', 'CAD/month', 'Housing', 'annual' UNION ALL
    SELECT 'Avg rent - 1 Bedroom', 'CMHC', 'CAD/month', 'Housing', 'annual' UNION ALL
    SELECT 'Avg rent - 2 Bedroom', 'CMHC', 'CAD/month', 'Housing', 'annual' UNION ALL
    SELECT 'Avg rent - 3 Bedroom +', 'CMHC', 'CAD/month', 'Housing', 'annual' UNION ALL
    SELECT 'Avg rent - Total', 'CMHC', 'CAD/month', 'Housing', 'annual' UNION ALL
    SELECT 'Vacancy rate - Bachelor', 'CMHC', 'Percent', 'Housing', 'annual' UNION ALL
    SELECT 'Vacancy rate - 1 Bedroom', 'CMHC', 'Percent', 'Housing', 'annual' UNION ALL
    SELECT 'Vacancy rate - 2 Bedroom', 'CMHC', 'Percent', 'Housing', 'annual' UNION ALL
    SELECT 'Vacancy rate - 3 Bedroom +', 'CMHC', 'Percent', 'Housing', 'annual' UNION ALL
    SELECT 'Vacancy rate - Total', 'CMHC', 'Percent', 'Housing', 'annual' UNION ALL
    SELECT 'Mortgage rate - 1 year', 'CMHC', 'Percent', 'Mortgage', 'monthly' UNION ALL
    SELECT 'Mortgage rate - 3 year', 'CMHC', 'Percent', 'Mortgage', 'monthly' UNION ALL
    SELECT 'Mortgage rate - 5 year', 'CMHC', 'Percent', 'Mortgage', 'monthly' UNION ALL
    SELECT 'Mortgage delinquency rate', 'CMHC', 'Percent', 'Mortgage', 'quarterly' UNION ALL
    SELECT 'HELOC delinquency rate', 'CMHC', 'Percent', 'Credit', 'quarterly' UNION ALL
    SELECT 'Credit card delinquency rate', 'CMHC', 'Percent', 'Credit', 'quarterly' UNION ALL
    SELECT 'Auto loan delinquency rate', 'CMHC', 'Percent', 'Credit', 'quarterly' UNION ALL
    SELECT 'LOC delinquency rate', 'CMHC', 'Percent', 'Credit', 'quarterly' UNION ALL
    SELECT 'Avg credit score - Without mortgage', 'CMHC', 'Score', 'Credit', 'quarterly' UNION ALL
    SELECT 'Avg credit score - With mortgage', 'CMHC', 'Score', 'Credit', 'quarterly' UNION ALL
    SELECT 'Avg credit score - With new mortgage', 'CMHC', 'Score', 'Credit', 'quarterly' UNION ALL
    SELECT 'Avg monthly mortgage payment - Existing', 'CMHC', 'CAD/month', 'Mortgage', 'quarterly' UNION ALL
    SELECT 'Avg monthly mortgage payment - New', 'CMHC', 'CAD/month', 'Mortgage', 'quarterly'
) src
WHERE NOT EXISTS (
    SELECT 1 FROM dim_indicator
    WHERE indicator_name = src.indicator_name AND source = src.source
)
~~~
CREATE TABLE IF NOT EXISTS stg_fact_monthly (
    geography_id    INT,
    date_id         INT,
    indicator_id    INT,
    value           DECIMAL(12,4)
)
~~~
CREATE TABLE IF NOT EXISTS stg_fact_annual_income (
    geography_id    INT,
    date_id         INT,
    income_source   VARCHAR(100),
    age_group       VARCHAR(50),
    sex             VARCHAR(20),
    median_income   DECIMAL(12,2),
    avg_income      DECIMAL(12,2),
    num_persons     BIGINT
)
~~~
CREATE TABLE IF NOT EXISTS fact_monthly (
    id              BIGINT IDENTITY(1,1),
    geography_id    INT         NOT NULL REFERENCES dim_geography(geography_id),
    date_id         INT         NOT NULL REFERENCES dim_date(date_id),
    indicator_id    INT         NOT NULL REFERENCES dim_indicator(indicator_id),
    value           DECIMAL(12,4),
    loaded_at       TIMESTAMP   DEFAULT GETDATE(),
    PRIMARY KEY (geography_id, date_id, indicator_id)
) DISTKEY(geography_id) SORTKEY(date_id, indicator_id)
~~~
CREATE TABLE IF NOT EXISTS fact_annual_income (
    id              BIGINT IDENTITY(1,1),
    geography_id    INT          NOT NULL REFERENCES dim_geography(geography_id),
    date_id         INT          NOT NULL REFERENCES dim_date(date_id),
    income_source   VARCHAR(100) NOT NULL,
    age_group       VARCHAR(50)  NOT NULL,
    sex             VARCHAR(20)  NOT NULL,
    median_income   DECIMAL(12,2),
    avg_income      DECIMAL(12,2),
    num_persons     BIGINT,
    loaded_at       TIMESTAMP    DEFAULT GETDATE(),
    PRIMARY KEY (geography_id, date_id, income_source, age_group, sex)
) DISTKEY(geography_id) SORTKEY(date_id)
~~~
CREATE TABLE IF NOT EXISTS stg_fact_annual_income_tenure (
    geography_id    INT,
    date_id         INT,
    tenure          VARCHAR(20),
    median_income   DECIMAL(12,2),
    avg_income      DECIMAL(12,2)
)
~~~
CREATE TABLE IF NOT EXISTS fact_annual_income_tenure (
    id              BIGINT IDENTITY(1,1),
    geography_id    INT          NOT NULL REFERENCES dim_geography(geography_id),
    date_id         INT          NOT NULL,
    tenure          VARCHAR(20)  NOT NULL,
    median_income   DECIMAL(12,2),
    avg_income      DECIMAL(12,2),
    loaded_at       TIMESTAMP    DEFAULT GETDATE(),
    PRIMARY KEY (geography_id, date_id, tenure)
) DISTKEY(geography_id) SORTKEY(date_id)
