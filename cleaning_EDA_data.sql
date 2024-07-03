use layoffs
-- SQL Project - Data Cleaning

-- https://www.kaggle.com/datasets/swaptr/layoffs-2022


SELECT * 
FROM layoffs;



-- first thing we want to do is create a staging table. This is the one we will work in and clean the data.
-- We want a table with the raw data in case something happens
CREATE TABLE layoffs_staging 
LIKE layoffs;
INSERT layoffs_staging 
SELECT * FROM layoffs;

-- now when we are data cleaning we usually follow a few steps
-- 1. check for duplicates and remove any
-- 2. standardize data and fix errors
-- 3. Look at null values and see what 
-- 4. remove any columns and rows that are not necessary - few ways
SELECT * FROM layoffs_staging;

-- 1. Remove Duplicates

SELECT *
FROM (
	SELECT company, industry, total_laid_off,`date`,
		ROW_NUMBER() OVER (
			PARTITION BY company, industry, total_laid_off,`date`
			) AS row_num
	FROM 
		layoffs_staging
) duplicates
WHERE 
	row_num > 1;
    
-- let's just look at oda to confirm
SELECT *
FROM layoffs_staging
WHERE company = 'Oda'
-- it looks like these are all legitimate entries and shouldn't be deleted.
-- We need to really look at every single row to be accurate

-- these are our real duplicates 
SELECT *
FROM (
	SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		layoffs_staging
) duplicates
WHERE 
	row_num > 1;
    
-- these are the ones we want to delete where the row number is > 1 or 2or greater essentially

-- now you may want to write it like this:
WITH DELETE_CTE AS 
(
SELECT *
FROM (
	SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		layoffs_staging
) duplicates
WHERE 
	row_num > 1
)
DELETE
FROM DELETE_CTE
WHERE 
	row_num > 1
;
  -- we can not update cte and delete is like update so ..
DROP TABLE IF EXISTS layoffs_staging2;

CREATE TABLE `layoffs_staging2` (
`company` text,
`location`text,
`industry`text,
`total_laid_off` INT,
`percentage_laid_off` text,
`date` text,
`stage`text,
`country` text,
`funds_raised_millions` int,
`row_num` INT
);
INSERT INTO `layoffs_staging2`
(`company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
`row_num`)
SELECT `company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		layoffs_staging;
select * from layoffs_staging2

-- now that we have this we can delete rows were row_num is greater than 2

DELETE FROM layoffs_staging2
WHERE row_num >= 2;
-- check duplicate
select * from layoffs_staging2
WHERE row_num >= 2;

-- 2. Standardize Data

SELECT * 
FROM layoffs_staging2;

-- if we look at industry it looks like we have some null and empty rows, let's take a look at these
select company,trim(company)
from  layoffs_staging2
-- deleting space
update layoffs_staging2
set company=trim(company)

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY industry;
-- I found industry Crypto and CryptoCurrency which are Identical
SELECT *
FROM layoffs_staging2
WHERE TRIM(industry) LIKE 'Crypto%';

Update layoffs_staging2
set industry='Crypto'
where industry like 'Crypto%'

-- it looks good
SELECT DISTINCT location
FROM layoffs_staging2

-- repetitive name
SELECT DISTINCT country
FROM layoffs_staging2
order by 1

SELECT DISTINCT country
FROM layoffs_staging2
where country LIKE 'United States%'

UPDATE layoffs_staging2
SET country='United States'
WHERE country LIKE 'United States%'

-- correct format of date 
SELECT date,
STR_TO_DATE(date,'%m/%d/%Y')
FROM layoffs_staging2

 UPDATE layoffs_staging2
 SET date=STR_TO_DATE(date,'%m/%d/%Y')
 
SELECT date
FROM layoffs_staging2

ALTER TABLE layoffs_staging2
MODIFY COlUMN date DATE;

-- 3. Look at Null Values
-- the null values in total_laid_off, percentage_laid_off, and funds_raised_millions all look normal. I don't think I want to change that
-- I like having them null because it makes it easier for calculations during the EDA phase
-- so there isn't anything I want to change with the null values
-- 4. remove any columns and rows we need to


-- filling  null or blank values
SELECT *
FROM layoffs_staging2
where company='Airbnb'

UPDATE layoffs_staging2
SET industry=NULL
WHERE industry=''

SELECT * FROM 
layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company=t2.company
WHERE t1.industry IS NULL
  AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2 ON t1.company=t2.company
SET t1.industry=t2.industry
WHERE t1.industry IS NULL  AND t2.industry IS NOT NULL;
------
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Delete Useless data we can't really use
DELETE FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * 
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

-- EDA

-- Here we are jsut going to explore the data and find trends or patterns or anything interesting like outliers

-- with this info we are just going to look around and see what we find!

SELECT * 
FROM layoffs_staging2;

SELECT MAX(total_laid_off)
FROM layoffs_staging2;

-- Looking at Percentage to see how big these layoffs were
SELECT MAX(percentage_laid_off),  MIN(percentage_laid_off)
FROM layoffs_staging2
WHERE  percentage_laid_off IS NOT NULL;

-- Which companies had 1 which is basically 100 percent of they company laid off
SELECT *
FROM layoffs_staging2
WHERE  percentage_laid_off = 1;


-- these are mostly startups it looks like who all went out of business during this time

-- if we order by funcs_raised_millions we can see how big some of these companies were
SELECT *
FROM layoffs_staging2
WHERE  percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;
-- BritishVolt looks like  2 billion dollars and went under 


-- SOMEWHAT TOUGHER AND MOSTLY USING GROUP BY-----------

-- Companies with the biggest single Layoff

SELECT company, total_laid_off
FROM layoffs_staging
ORDER BY 2 DESC
LIMIT 5;

-- now that's just on a single day

-- Companies with the most Total Layoffs
SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC
LIMIT 10;

-- by location
SELECT location, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY location
ORDER BY 2 DESC
LIMIT 10;

SELECT YEAR(date), SUM(total_laid_off)
FROM  layoffs_staging2
GROUP BY YEAR(date)
ORDER BY 1 ASC;

SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;


SELECT industry, SUM(total_laid_off)
FROM  layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

-- Earlier we looked at Companies with the most Layoffs. Now let's look at that per year. 
-- I want to look at 
WITH Company_Year AS 
(
  SELECT company, YEAR(date) AS years, SUM(total_laid_off) AS total_laid_off
  FROM layoffs_staging2
  GROUP BY company, YEAR(date)
)
, Company_Year_Rank AS (
  SELECT company, years, total_laid_off, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
  FROM Company_Year
)
SELECT company, years, total_laid_off, ranking
FROM Company_Year_Rank
WHERE ranking <= 3
AND years IS NOT NULL
ORDER BY years ASC, total_laid_off DESC;

-- Rolling Total of Layoffs Per Month
SELECT SUBSTRING(date,1,7) as dates, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY dates
ORDER BY dates ASC;

-- now use it in a CTE so we can query off of it
WITH DATE_CTE AS 
(
SELECT SUBSTRING(date,1,7) as dates, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY dates
ORDER BY dates ASC
)
SELECT dates, SUM(total_laid_off) OVER (ORDER BY dates ASC) as rolling_total_layoffs
FROM DATE_CTE
ORDER BY dates ASC;
