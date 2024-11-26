-- Data Cleaning 
-- World Layoff Data: https://www.kaggle.com/datasets/swaptr/layoffs-2022




SELECT *
FROM layoffs;

-- Create new table for staging 
CREATE TABLE layoffs_staging_tweaking 
LIKE layoffs;

SELECT * 
FROM layoffs_staging_tweaking;

INSERT layoffs_staging_tweaking
SELECT *
FROM layoffs;

-- To Do's
-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Look at null Values or blank values 
-- 4. Remove unecessary columns and rows




-- 1. Remove Duplicates
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging_tweaking;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised) AS row_num
FROM layoffs_staging_tweaking
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

SELECT *
FROM layoffs_staging
WHERE company = '23andMe'; #checks the duplicates to see if it's really a duplicate, there are some. i want to keep 1 copy

CREATE TABLE `layoffs_staging_tweaking2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised` text,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging_tweaking2 #new empty table
WHERE row_num > 1; #after doing insert below, now we filter row_num 2

INSERT INTO layoffs_staging_tweaking2 #insert previous data to new empty table
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised) AS row_num
FROM layoffs_staging;

SET SQL_SAFE_UPDATES = 0; 

DELETE
FROM layoffs_staging_tweaking2 #new empty table
WHERE row_num > 1;

SELECT *
FROM layoffs_staging_tweaking2;




-- 2. Standardize the Data
# set date definition to date to prepare for viz
ALTER TABLE layoffs_staging_tweaking2 
MODIFY COLUMN `date` DATE;

SELECT *
FROM layoffs_staging_tweaking2;




-- 3. Null Values or blank values
-- null values in total_laid_off, percentage_laid_off look normal. 
SELECT *
FROM layoffs_staging_tweaking2
WHERE percentage_laid_off IS NULL
OR percentage_laid_off = '';

UPDATE layoffs_staging_tweaking2
SET percentage_laid_off = NULL
WHERE percentage_laid_off = ''; #set blank values to Null

UPDATE layoffs_staging_tweaking2
SET industry = NULL
WHERE industry = '';




-- 4. Remove any unecessary columns and rows
-- Could delete sections where there is null in total laid off and percentage laid off if that is the purpose
SELECT *
FROM layoffs_staging_tweaking2
WHERE total_laid_off IS NULL
OR total_laid_off = '';

-- Delete row_num column bc it is no longer needed
ALTER TABLE layoffs_staging_tweaking2
DROP COLUMN row_num;
