# Nashville Housing Data Cleaning Project
### Overview
This project showcases end-to-end cleaning processes on a Nashville housing dataset containing over 56,000 property records. The raw data required extensive cleaning and standardization to make it analysis ready, demonstrating practical data cleaning skills essential for data analyst roles.

### Problem
The raw Nashville housing dataset contained multiple data quality issues that prevented meaningful analysis:
- Duplicate records
- Inconsistent date formates
- Missing (NULL) property addresses
- Unstructured addresses
- Mixed data types with inconsistent formatting issues
- Inconsistent categorical values

### Technical Solutions Implemented
- **Advanced Duplicate Detection** utilizing Window Functions and CTEs to identify and remove 104 duplicate records
- **Missing Value Imputation** by applying Self-Joins to fill missing property address values
- **String Parsing and Standardization** using String Functions to transform unstructured address data into separate, analyzable components
- **Categorical Data Standardization** utilizing Conditional Logic to standardize inconsistent values
