/* 
Project: Urban Food & Pharmacy Access Optimizer
Description: SQL ETL pipeline for cleaning and consolidating store data 
before visualization in Power BI.
*/

/* =====================================================
   SECTION 1: DATABASE SETUP
===================================================== */

CREATE DATABASE IF NOT EXISTS urban_access;
USE urban_access;

SHOW TABLES;


/* =====================================================
   SECTION 2: DATA VALIDATION
   (Checking row counts before cleaning)
===================================================== */

SELECT COUNT(*) AS Grocery_Count FROM grocery;
SELECT COUNT(*) AS Pharmacy_Count FROM pharmacy;
SELECT COUNT(*) AS Supermarket_Count FROM supermarket;


/* =====================================================
   SECTION 3: DATA CLEANING & STANDARDIZATION
   (Handling null and blank store names)
===================================================== */

SET sql_safe_updates = 0;

-- Clean Pharmacy Table
UPDATE pharmacy 
SET Store_name = 'Unknown Pharmacy'
WHERE Store_name IS NULL OR Trim(Store_name) = ' ';

-- Clean Supermarket Table
UPDATE supermarket 
SET Store_name = 'Unknown Supermarket'
WHERE Store_name IS NULL OR Trim(Store_name) = ' ';

-- Clean Grocery Table
UPDATE grocery 
SET Store_name = 'Unknown Grocery'
WHERE Store_name IS NULL OR Trim(Store_name) =' ';


/* =====================================================
   SECTION 4: DATA CONSOLIDATION (ETL VIEW CREATION)
   (Combining all tables into one analytical dataset)
===================================================== */

Drop view If Exists Urban_resources;
CREATE OR REPLACE VIEW urban_resources AS
SELECT Store_name, Category, Latitude, Longitude, City FROM supermarket
UNION ALL
SELECT Store_name, Category, Latitude, Longitude, City FROM grocery
UNION ALL
SELECT Store_name, Category, Latitude, Longitude, City FROM pharmacy;

-- Remove duplicate records

Create view If Exists Final_cleaned_data;
CREATE OR REPLACE VIEW final_cleaned_data AS
SELECT DISTINCT * FROM urban_resources;


/* =====================================================
   VALIDATION AFTER CLEANING
===================================================== */

SELECT Category,
       COUNT(*) AS Total_Records,
       SUM(CASE WHEN Store_name LIKE 'Unknown%' THEN 1 ELSE 0 END) AS Cleaned_Blanks
FROM final_cleaned_data
GROUP BY Category;

-- Check for missing coordinates
SELECT COUNT(*) AS Missing_Coordinates
FROM final_cleaned_data
WHERE Latitude IS NULL OR Longitude IS NULL;


SET sql_safe_updates = 1;
