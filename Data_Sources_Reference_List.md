# HR Recruitment Tool - Data Sources

Hello,

Below are the data sources used in the HR Recruitment Tool (ForeSite Analytics). You can access the original data directly from these authoritative government and industry sources:

---

## **1. Statistics Canada**

### Consumer Price Index (CPI)
- **What it provides:** Monthly inflation data by city and category (food, transportation, shelter, etc.)
- **Access:** https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=1810000401
- **Frequency:** Monthly
- **Format:** CSV, API available

### Median Income by City
- **What it provides:** Median household and individual income by census metropolitan area
- **Access:** https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=1110023901
- **Frequency:** Annual (Census + updates)
- **Format:** CSV

### Food Prices
- **What it provides:** Average retail prices for food products by city
- **Access:** https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=1810024501
- **Frequency:** Monthly
- **Format:** CSV

### Gasoline Prices
- **What it provides:** Average pump prices by province and select cities
- **Access:** https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=1810000101
- **Frequency:** Monthly
- **Format:** CSV

---

## **2. Canada Mortgage and Housing Corporation (CMHC)**

### Rental Market Survey
- **What it provides:** Average rent by bedroom type, vacancy rates, and rental market trends by city
- **Access:** https://www.cmhc-schl.gc.ca/professionals/housing-markets-data-and-research/housing-data/data-tables/rental-market
- **Frequency:** Monthly and Annual Reports
- **Format:** Excel, PDF reports
- **Note:** Some data requires free CMHC account registration

### Specific Data Tables:
- **Average Rents:** Table 3.1.1 - Private Row (Townhouse) and Apartment Average Rents
- **Vacancy Rates:** Table 1.1.1 - Vacancy Rates by Bedroom Type

---

## **3. Canadian Real Estate Association (CREA)**

### MLS Home Price Index (HPI)
- **What it provides:** Quality-adjusted home price trends by market (better than simple averages)
- **Access:** https://www.crea.ca/housing-market-stats/mls-home-price-index/
- **Frequency:** Monthly
- **Format:** Web-based data viewer, downloadable reports
- **Note:** Public aggregate data available; detailed MLS data requires REALTOR® membership

### Monthly Market Statistics
- **What it provides:** Sales activity, new listings, price trends by city
- **Access:** https://stats.crea.ca/en-CA/
- **Frequency:** Monthly
- **Format:** Interactive dashboard, PDF reports

---

## **Additional Notes:**

- **Data Grain:** Our tool combines monthly data (CPI, rent, HPI) with annual data (income) using a date dimension table for proper time-series analysis.
- **Geographic Coverage:** Focus on major Canadian census metropolitan areas (CMAs): Toronto, Vancouver, Calgary, Montreal, Ottawa, Edmonton, etc.
- **Data Cleaning:** We standardize city names, handle missing values, and align geography keys across sources since each organization uses different naming conventions.
- **Architecture:** All data is ingested into Microsoft Fabric (Delta Lake), cleaned with PySpark, and modeled in a star schema (15 relationships) for Power BI Direct Lake mode.

---

## **Quick Links Summary:**

| Source | Link |
|--------|------|
| Statistics Canada - Data Tables | https://www150.statcan.gc.ca/n1/en/type/data |
| CMHC - Housing Market Data | https://www.cmhc-schl.gc.ca/professionals/housing-markets-data-and-research |
| CREA - MLS HPI | https://www.crea.ca/housing-market-stats/mls-home-price-index/ |

---

If you have any questions about accessing or interpreting this data, feel free to reach out.

Best regards,
**ForeSite Analytics**
Kristen • Guy • Natalia • Jenna
SAIT Data Analytics Capstone | April 2026
