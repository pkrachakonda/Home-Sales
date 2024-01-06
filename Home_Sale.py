# Import findspark and initialise.
import findspark
findspark.init()

# Import packages
from pyspark.sql import SparkSession
import time

# Create a SparkSession
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

# 1. Read in the AWS S3 bucket into a DataFrame.
from pyspark import SparkFiles
url = "https://2u-data-curriculum-team.s3.amazonaws.com/dataviz-classroom/v1.2/22-big-data/home_sales_revised.csv"
spark.sparkContext.addFile(url)
HomeSales_df = spark.read.csv(SparkFiles.get("home_sales_revised.csv"), sep=",", header=True)
HomeSales_df.show()

# 2. Create a temporary view of the DataFrame.
HomeSales_df.createOrReplaceTempView('HomeSales')

# 3. What is the average price for a four bedroom house sold in each year rounded to two decimal places?
query = """
(SELECT YEAR(date) AS YEAR, ROUND(AVG(price),2) AS AVERAGE_PRICE
FROM HomeSales
WHERE bedrooms = 4
GROUP BY YEAR(date), bedrooms
ORDER BY YEAR DESC
)
"""
spark.sql(query).show()

# 4. What is the average price of a home for each year the home was built that have 3 bedrooms and 3 bathrooms rounded to two decimal places?
query = """
(SELECT YEAR(date_built) AS YEAR, ROUND(AVG(price),2) AS AVERAGE_PRICE
FROM HomeSales
WHERE bedrooms = 3 AND bathrooms = 3
GROUP BY YEAR(date_built), bedrooms, bathrooms
ORDER BY YEAR DESC
)
"""
spark.sql(query).show()

#  5. What is the average price of a home for each year built that have 3 bedrooms, 3 bathrooms, with two floors,
# and are greater than or equal to 2,000 square feet rounded to two decimal places?
query = """
(SELECT date_built AS YEAR_BUILT, ROUND(AVG(price),2) AS AVERAGE_PRICE
FROM HomeSales
WHERE bedrooms = 3 AND bathrooms = 3 AND floors =2 AND (sqft_living > 2000 OR sqft_living = 2000)
GROUP BY YEAR_BUILT, bedrooms, bathrooms, floors
ORDER BY YEAR_BUILT DESC
)
"""
spark.sql(query).show()

# 6. What is the "view" rating for the average price of a home, rounded to two decimal places, where the homes are greater than
# or equal to $350,000? Although this is a small dataset, determine the run time for this query.

start_time = time.time()

spark.sql("""select view, ROUND(AVG(price),2) AS AVERAGE_PRICE from HomeSales group by 1 HAVING AVERAGE_PRICE >= 350000 ORDER BY view DESC""").show()

print("--- %s seconds ---" % (time.time() - start_time))

# 7. Cache the the temporary table home_sales.
spark.sql("cache table HomeSales")

# 8. Check if the table is cached.
spark.catalog.isCached('HomeSales')

# 9. Using the cached data, run the query that filters out the view ratings with average price
#  greater than or equal to $350,000. Determine the runtime and compare it to uncached runtime.

start_time = time.time()

spark.sql("""select view, ROUND(AVG(price),2) AS AVERAGE_PRICE from HomeSales group by 1 HAVING AVERAGE_PRICE >= 350000 ORDER BY view DESC""").show()

print("--- %s seconds ---" % (time.time() - start_time))

# 10. Partition by the "date_built" field on the formatted parquet home sales data
HomeSales_df.write.partitionBy("date_built").mode("overwrite").parquet("HomeSales/Sales_Partitioned")

# 11. Read the formatted parquet data.
Parq_HomeSale_df = spark.read.parquet('HomeSales/Sales_Partitioned')

# 12. Create a temporary table for the parquet data.
Parq_HomeSale_df.createOrReplaceTempView('Parq_Sales')

# 13. Run the query that filters out the view ratings with average price of greater than or eqaul to $350,000
# with the parquet DataFrame. Round your average to two decimal places.
# Determine the runtime and compare it to the cached version.

start_time = time.time()

spark.sql("""select view, ROUND(AVG(price),2) AS AVERAGE_PRICE from Parq_Sales group by 1 HAVING AVERAGE_PRICE >= 350000 ORDER BY view DESC""").show()

print("--- %s seconds ---" % (time.time() - start_time))

# 14. Uncache the home_sales temporary table.
spark.sql("uncache table HomeSales")

# 15. Check if the home_sales is no longer cached
if spark.catalog.isCached('HomeSales'):
  print('sales is still cached')
else:
  print('all clear')