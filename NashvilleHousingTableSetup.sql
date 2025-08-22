-- Create Nashville Housing Data table

CREATE TABLE housingdata (
	UniqueID VARCHAR(20) PRIMARY KEY,
    ParcelID VARCHAR(50),
    LandUse VARCHAR(25),
    PropertyAddress VARCHAR(150),
    SaleDate VARCHAR(60),
    SalePrice VARCHAR(20),
    LegalReference VARCHAR(50),
    SoldAsVacant VARCHAR(5),
    OwnerName VARCHAR(255),
    OwnerAddress VARCHAR(255),
    Acreage VARCHAR(10),
    TaxDistrict VARCHAR(100),
    LandValue VARCHAR(10),
    BuildingValue VARCHAR(10),
    TotalValue VARCHAR(10),
    YearBuilt VARCHAR(10),
    Bedrooms VARCHAR(10),
    FullBath VARCHAR(10),
    HalfBath VARCHAR(10)
);

LOAD DATA LOCAL INFILE 'PATH'
INTO TABLE housingdata
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\\r\\n'
IGNORE 1 ROWS;

SELECT *
FROM housingdata
LIMIT 10;