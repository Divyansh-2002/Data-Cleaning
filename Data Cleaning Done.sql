use world_layoffs;
select *
from layoffs
;

DROP TABLE IF EXISTS layoffs_staging;
CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;  

INSERT layoffs_staging
SELECT *
FROM layoffs;

select *
from layoffs_staging;

select company,location,industry,total_laid_off,`date`,country,funds_raised_millions,
ROW_NUMBER() OVER(PARTITION BY company,location,industry,total_laid_off,`date`,country,funds_raised_millions) as row_num
from layoffs_staging;

create table layoff_staging2
like layoffs_staging;


select row_num
from layoffs_staging
where row_num >1;

WITH CTE_REMOVE_DUPLI AS
(
SELECT *,
ROW_NUMBER() 
OVER(PARTITION BY company,location,industry,total_laid_off,`date`,country,funds_raised_millions) as row_num
from layoffs_staging)
select *
from CTE_REMOVE_DUPLI
where row_num >1;


CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2;  

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() 
OVER(PARTITION BY company,location,industry,total_laid_off,`date`,country,funds_raised_millions) as row_num
from layoffs_staging;

DELETE 
FROM layoffs_staging2
WHERE row_num >1;

-- ---------REMOVE DUPLICATES DONE---------------------------------------------------

-- STANDARDIZING ---------------------------------------------------


select distinct industry
from layoffs_staging2;

-- TRIM ------------------------------------
select company, TRIM(company)
from layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT DISTINCT company
from layoffs_staging2;

SELECT distinct industry
FROM layoffs_staging2
where industry like 'crypto%';  

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry like 'Crypto%';

SELECT distinct country
from layoffs_staging2
order by country;

UPDATE layoffs_staging2
SET country = 'United States'
WHERE country like 'United States_';

select country
from layoffs_staging2
where country like 'United States_';

SELECT distinct industry
FROM layoffs_staging2
;

SELECT country , TRIM(TRAILING ('.') FROM country)
from layoffs_staging2;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING ('.') FROM country);


select `date`,
STR_TO_DATE(`date`,'%m/%d/%Y')
from layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`,'%m/%d/%Y');

ALTER table layoffs_staging2
MODIFY column `date` DATE;

select company,industry
from layoffs_staging2
where industry ='';

select company,industry
from layoffs_staging2
where company like 'Air%';

SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- REMOVAL-------------------------------------

DELETE 
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

SELECT *	
FROM layoffs_staging2;

ALTER table layoffs_staging2
DROP COLUMN row_num;
