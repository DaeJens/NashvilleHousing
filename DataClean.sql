-- Data Cleaning

SELECT *
FROM housingdata;

-- 1. Duplicate Dataset
CREATE TABLE housingdata_staging1
LIKE housingdata;

INSERT housingdata_staging1
SELECT *
FROM housingdata;

SELECT *
FROM housingdata_staging1;

-- 2. Remove Duplicates
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

-- 3. Standardize Date Format
-- 3a. Convert SaleDate column from VARCHAR to DATE
UPDATE housingdata_staging1
SET SaleDate = 
	STR_TO_DATE(SaleDate, '%d-%b-%y');

-- 4. Standardize Address Format
-- 4a. Populate Property Address Data
SELECT *
FROM housingdata_staging1
WHERE PropertyAddress IS NULL;

-- Some entries are missing PropertyAddress. Entries with identical ParcelIDs seem to have the same PropertyAddress, 
-- so here I will use a self-join on matching ParcelIDs to populate missing PropertyAddress values.
UPDATE housingdata_staging1 a
JOIN housingdata_staging1 b
	ON a.ParcelID = b.ParcelID
    AND a.UniqueID != b.UniqueID
SET a.PropertyAddress = b.PropertyAddress
WHERE a.PropertyAddress is null;

-- 4b. Break PropertyAddress into Individual Columns (Address, City)
SELECT PropertyAddress
FROM housingdata_staging1;

SELECT
SUBSTRING(PropertyAddress, 1, LOCATE(',', PropertyAddress)-1) AS Address,
SUBSTRING(PropertyAddress, LOCATE(',', PropertyAddress)+1) AS City
FROM housingdata_staging1;

ALTER TABLE housingdata_staging1
ADD PropertySplitAddress VARCHAR(100);

UPDATE housingdata_staging1
SET PropertySplitAddress = SUBSTRING(PropertyAddress, 1, LOCATE(',', PropertyAddress)-1);

ALTER TABLE housingdata_staging1
ADD PropertySplitCity VARCHAR(100);

UPDATE housingdata_staging1
SET PropertySplitCity = SUBSTRING(PropertyAddress, LOCATE(',', PropertyAddress)+1);

SELECT PropertyAddress, PropertySplitAddress, PropertySplitCity
FROM housingdata_staging1;

-- 4c. Break OwnerAddress into Individual Columns (Address, City, State)
SELECT OwnerAddress
FROM housingdata_staging1;

SELECT
SUBSTRING_INDEX(OwnerAddress, ',', -1) AS State,
SUBSTRING_INDEX(SUBSTRING_INDEX(OwnerAddress, ',', -2), ',', 1) AS City,
SUBSTRING_INDEX(SUBSTRING_INDEX(OwnerAddress, ',', -3), ',', 1) AS Address
FROM housingdata_staging1;

ALTER TABLE housingdata_staging1
ADD OwnerSplitAddress VARCHAR(100);

UPDATE housingdata_staging1
SET OwnerSplitAddress = SUBSTRING_INDEX(OwnerAddress, ',', 1);

ALTER TABLE housingdata_staging1
ADD OwnerSplitCity VARCHAR(100);

UPDATE housingdata_staging1
SET OwnerSplitCity = SUBSTRING_INDEX(SUBSTRING_INDEX(OwnerAddress, ',', -2), ',', 1);

ALTER TABLE housingdata_staging1
ADD OwnerSplitState VARCHAR(5);

UPDATE housingdata_staging1
SET OwnerSplitState = SUBSTRING_INDEX(OwnerAddress, ',', -1);

SELECT OwnerAddress, OwnerSplitAddress, OwnerSplitCity, OwnerSplitState
FROM housingdata_staging1;

-- 5. Change Y/N to Yes/No in SoldAsVacant column
SELECT DISTINCT(SoldAsVacant), COUNT(SoldAsVacant)
FROM housingdata_staging1
GROUP BY SoldAsVacant;

SELECT SoldAsVacant,
	CASE
		WHEN SoldAsVacant = "Y" THEN "Yes"
        WHEN SoldAsVacant = "N" THEN "No"
        ELSE SoldAsVacant
	END
FROM housingdata_staging1;

UPDATE housingdata_staging1
SET SoldAsVacant = 
	CASE
		WHEN SoldAsVacant = "Y" THEN "Yes"
        WHEN SoldAsVacant = "N" THEN "No"
        ELSE SoldAsVacant
	END;
    
SELECT DISTINCT(SoldAsVacant), COUNT(SoldAsVacant)
FROM housingdata_staging1
GROUP BY SoldAsVacant;

-- 6. Delete Unused Columns
-- Since I split PropertyAddress and OwnerAddress earlier, we no longer need those columns.
ALTER TABLE housingdata_staging1
DROP COLUMN OwnerAddress;

ALTER TABLE housingdata_staging1
DROP COLUMN PropertyAddress;
