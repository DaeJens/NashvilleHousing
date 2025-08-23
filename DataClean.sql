/*
=====================================================================
NASHVILLE HOUSING DATA CLEANING PROJECT
=====================================================================
Author: Daelin Jensen
Database: MySQL
Dataset: Nashville Housing Sales Data
Purpose: Clean and standardize raw Nashville housing data for 
		 downstream analysis.

This script demonstrates comprehensive data cleaning techniques:
	- Data duplication and staging
    - Duplicate removal
    - Data type conversions
    - String parsing
    - Missing value imputation
    - Data validation and quality checks
=====================================================================
*/

-- ==================================================================
-- 1. INITIAL DATA EXPLORATION
-- ==================================================================
DESCRIBE housingdata;

SELECT *
FROM housingdata
LIMIT 15;

SELECT COUNT(*) AS total_records,
	COUNT(DISTINCT UniqueID) AS unique_records
FROM housingdata;

-- ==================================================================
-- 2. CREATE STAGING TABLE FOR SAFE DATA MANIPULATION
-- ==================================================================
-- Create duplicate staging table structure
CREATE TABLE housingdata_staging1
LIKE housingdata;

-- Copy all data from housingdata into staging table
INSERT housingdata_staging1
SELECT *
FROM housingdata;

SELECT *
FROM housingdata_staging1;

-- ==================================================================
-- 3. IDENTIFY AND REMOVE DUPLICATE ENTRIES
-- ==================================================================
-- Count duplicate records
WITH duplicate_cte AS (
	SELECT *,
	ROW_NUMBER() OVER(
		PARTITION BY
			ParcelID,
			PropertyAddress,
			SaleDate,
			SalePrice,
			LegalReference,
			OwnerName,
			OwnerAddress,
			Acreage,
			TaxDistrict,
			LandValue,
			BuildingValue,
			TotalValue
		ORDER BY UniqueID
	) AS row_num
FROM housingdata_staging1
) SELECT COUNT(*)
FROM duplicate_cte
WHERE row_num > 1;
-- There are 104 duplicate records in this dataset.

-- Remove duplicate records by creating a temporary table and self-joining to staging table
CREATE TEMPORARY TABLE temp_duplicates AS
SELECT UniqueID,
ROW_NUMBER() OVER(
PARTITION BY
    ParcelID,
    PropertyAddress,
    SaleDate,
    SalePrice,
    LegalReference,
    OwnerName,
    OwnerAddress,
    Acreage,
    TaxDistrict,
    LandValue,
    BuildingValue,
    TotalValue
    ORDER BY UniqueID
    ) AS row_num
FROM housingdata_staging1;

DELETE h FROM housingdata_staging1 h
JOIN temp_duplicates t ON h.UniqueID = t.UniqueID
WHERE row_num > 1;

DROP TEMPORARY TABLE temp_duplicates;

-- Verify duplicate removal
SELECT COUNT(*) AS records_after_dup_removal
FROM housingdata_staging1;

-- ==================================================================
-- 4. STANDARDIZE DATA FORMATS
-- ==================================================================
-- Convert SaleDate column from VARCHAR (i.e. '18-Jan-13') to DATE type
UPDATE housingdata_staging1
SET SaleDate = 
	STR_TO_DATE(SaleDate, '%d-%b-%y');

-- ==================================================================
-- 5. HANDLE MISSING PROPERTY ADDRESSES
-- ==================================================================
-- Check for NULL property addresses
SELECT *
FROM housingdata_staging1
WHERE PropertyAddress IS NULL;

-- Populate missing PropertyAddress values using ParcelID matching
-- Properties with same ParcelID should have same address
UPDATE housingdata_staging1 a
JOIN housingdata_staging1 b
	ON a.ParcelID = b.ParcelID
    AND a.UniqueID != b.UniqueID
SET a.PropertyAddress = b.PropertyAddress
WHERE a.PropertyAddress is null;

-- Verify missing address replacement
SELECT COUNT(*) AS remaining_null_addresses
FROM housingdata_staging1
WHERE PropertyAddress IS NULL;

-- ==================================================================
-- 6. PARSE AND STANDARDIZE ADDRESSES
-- ==================================================================

SELECT PropertyAddress
FROM housingdata_staging1;

-- 6a. Parse PropertyAddress into separate Address and City columns
SELECT
SUBSTRING(PropertyAddress, 1, LOCATE(',', PropertyAddress)-1) AS Address,
SUBSTRING(PropertyAddress, LOCATE(',', PropertyAddress)+1) AS City
FROM housingdata_staging1;

-- Add new columns for parsed address and city
ALTER TABLE housingdata_staging1
ADD PropertySplitAddress VARCHAR(100);

ALTER TABLE housingdata_staging1
ADD PropertySplitCity VARCHAR(100);

-- Populate parsed address and city columns
UPDATE housingdata_staging1
SET PropertySplitAddress = SUBSTRING(PropertyAddress, 1, LOCATE(',', PropertyAddress)-1);

UPDATE housingdata_staging1
SET PropertySplitCity = SUBSTRING(PropertyAddress, LOCATE(',', PropertyAddress)+1);

SELECT PropertyAddress, PropertySplitAddress, PropertySplitCity
FROM housingdata_staging1;

-- 6b. Parse Owner Address into separate Address, City, and State columns
SELECT OwnerAddress
FROM housingdata_staging1;

SELECT
SUBSTRING_INDEX(OwnerAddress, ',', -1) AS State,
SUBSTRING_INDEX(SUBSTRING_INDEX(OwnerAddress, ',', -2), ',', 1) AS City,
SUBSTRING_INDEX(SUBSTRING_INDEX(OwnerAddress, ',', -3), ',', 1) AS Address
FROM housingdata_staging1;

-- Add new columns for parsed address, city, and state
ALTER TABLE housingdata_staging1
ADD OwnerSplitAddress VARCHAR(100);

ALTER TABLE housingdata_staging1
ADD OwnerSplitCity VARCHAR(100);

ALTER TABLE housingdata_staging1
ADD OwnerSplitState VARCHAR(5);

-- Populate parsed address, city, and state columns
UPDATE housingdata_staging1
SET OwnerSplitAddress = SUBSTRING_INDEX(OwnerAddress, ',', 1);

UPDATE housingdata_staging1
SET OwnerSplitCity = SUBSTRING_INDEX(SUBSTRING_INDEX(OwnerAddress, ',', -2), ',', 1);

UPDATE housingdata_staging1
SET OwnerSplitState = SUBSTRING_INDEX(OwnerAddress, ',', -1);

-- Verify address parsing results
SELECT OwnerAddress, OwnerSplitAddress, OwnerSplitCity, OwnerSplitState
FROM housingdata_staging1;

-- ==================================================================
-- 7. CONVERT DATA TYPES FOR NUMERIC COLUMNS
-- ==================================================================
-- 7a. Clean and convert SalePrice
-- Check for non-numeric values
SELECT SalePrice, COUNT(*) as count
FROM housingdata_staging1 
WHERE SalePrice IS NOT NULL 
	AND SalePrice NOT REGEXP '^[0-9]+$'
GROUP BY SalePrice;
-- There are 12 entries with ',' and '$' that need to be cleaned
-- Duplicate the SalePrice column, and replace the ',' and '$'
ALTER TABLE housingdata_staging1 
ADD COLUMN SalePrice_clean INT;

UPDATE housingdata_staging1 
SET SalePrice_clean = CAST(REPLACE(REPLACE(SalePrice, '$',''),',','') AS UNSIGNED)
WHERE SalePrice IS NOT NULL
	AND SalePrice != '';

-- Verify the conversion to INT and drop original SalePrice column.
SELECT SalePrice_clean, COUNT(*) as count
FROM housingdata_staging1 
WHERE SalePrice_clean IS NOT NULL 
  AND SalePrice_clean NOT REGEXP '^[0-9]+$'
GROUP BY SalePrice_clean;

ALTER TABLE housingdata_staging1
DROP COLUMN SalePrice;

ALTER TABLE housingdata_staging1
CHANGE COLUMN SalePrice_clean SalePrice INT;

-- 7b. Convert property value columns to INT
-- LandValue conversion
-- Check for non-numeric values
SELECT LandValue, COUNT(*) as count
FROM housingdata_staging1
WHERE LandValue IS NOT NULL
	AND LandValue NOT REGEXP '^[0-9]+$'
GROUP BY LandValue;
-- There are no non-numeric values in LandValue, so I will convert directly to INT.

ALTER TABLE housingdata_staging1
MODIFY COLUMN LandValue INT;

-- BuildingValue conversion
-- Check for non-numeric values
SELECT BuildingValue, COUNT(*) as count
FROM housingdata_staging1
WHERE BuildingValue IS NOT NULL
	AND BuildingValue NOT REGEXP '^[0-9]+$'
GROUP BY BuildingValue;
-- There are no non-numeric values in BuildingValue, so I will convert directly to INT.

ALTER TABLE housingdata_staging1
MODIFY COLUMN BuildingValue INT;

-- TotalValue conversion
-- Check for non-numeric values
SELECT TotalValue, COUNT(*) as count
FROM housingdata_staging1
WHERE TotalValue IS NOT NULL
	AND TotalValue NOT REGEXP '^[0-9]+$'
GROUP BY TotalValue;
-- There are no non-numeric values in TotalValue, so I will convert directly to INT.

ALTER TABLE housingdata_staging1
MODIFY COLUMN TotalValue INT;

-- 7c. Convert Acreage to FLOAT (contains decimal values)
-- Check for non-numeric values such as decimals
SELECT Acreage, COUNT(*) as count
FROM housingdata_staging1
WHERE Acreage IS NOT NULL
	AND Acreage REGEXP '^[0-9]+\.[0-9]+$'
GROUP BY Acreage;
-- Many values are expressed as decimals, so Acreage should be converted into FLOAT instead of INT

ALTER TABLE housingdata_staging1
MODIFY COLUMN Acreage FLOAT;

-- 7d. Convert room count columns to INT
ALTER TABLE housingdata_staging1
MODIFY COLUMN Bedrooms INT;

ALTER TABLE housingdata_staging1
MODIFY COLUMN FullBath INT;

ALTER TABLE housingdata_staging1
MODIFY COLUMN HalfBath INT;

-- ==================================================================
-- 8. STANDARDIZE CATEGORICAL VALUES
-- ==================================================================
-- Standardize SoldAsVacant column
SELECT DISTINCT(SoldAsVacant), COUNT(SoldAsVacant)
FROM housingdata_staging1
GROUP BY SoldAsVacant;
-- There is a mix of Yes, No, Y, and N values in this column. Here, I will standardize all values
-- to be in Yes/No format

UPDATE housingdata_staging1
SET SoldAsVacant = 
	CASE
		WHEN SoldAsVacant = "Y" THEN "Yes"
        WHEN SoldAsVacant = "N" THEN "No"
        ELSE SoldAsVacant
	END;
-- Verify standardization
SELECT DISTINCT(SoldAsVacant), COUNT(SoldAsVacant)
FROM housingdata_staging1
GROUP BY SoldAsVacant;

-- ==================================================================
-- 9. REMOVE REDUNDANT COLUMNS
-- ==================================================================
ALTER TABLE housingdata_staging1
DROP COLUMN OwnerAddress;

ALTER TABLE housingdata_staging1
DROP COLUMN PropertyAddress;

-- ==================================================================
-- FINAL DATA QUALITY VALIDATION
-- ==================================================================

-- Check final dataset structure
DESCRIBE housingdata_staging1;

-- Validate data completeness for key columns
SELECT 
    'Data Quality Summary' AS metric,
    COUNT(*) AS total_records,
    COUNT(ParcelID) AS parcel_id_complete,
    COUNT(SaleDate) AS sale_date_complete,
    COUNT(SalePrice) AS sale_price_complete,
    COUNT(PropertySplitAddress) AS property_address_complete,
    ROUND(COUNT(SalePrice) * 100.0 / COUNT(*), 2) AS price_completion_rate
FROM housingdata_staging1;

-- Display sample of cleaned data
SELECT *
FROM housingdata_staging1
LIMIT 15; 