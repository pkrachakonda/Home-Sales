# Home-Sales

## Introduction
As part of this Challenge Pyspark is used to predict the key metrics for the home sales data using SparkSQL queries.

## Results
***01.What is the average price for a four-bedroom house sold for each year? Round off your answer to two decimal places.***
      
   Based on the output provided, average price for a four-bedroom varied is approximately 300,000 with maximum price being recorded in 2021, while 2022 had a comparably lower average price.

  ![image](https://github.com/pkrachakonda/Home-Sales/assets/20739237/4cb012e1-a7f5-40b0-a139-44cf2ff293aa)

***02. What is the average price of a home for each year it was built that has three bedrooms and three bathrooms? Round off your answer to two decimal places.***
       
   Like four-bed-room trend, analysis has indicated that that there is no significant change in house price three bedroom and bathrooms, constructed between 2010 - 2017.

   ![image](https://github.com/pkrachakonda/Home-Sales/assets/20739237/ada27e41-390e-460c-ada9-3eced401a46a)

***03.What is the average price of a home for each year that has three bedrooms, three bathrooms, two floors, and is greater than or equal to 2,000 square feet? Round off your answer to two decimal places.***

   Average House prices varied from 276553 to 307539 for houses with three bedrooms, three bathrooms, and two floors, where the total square footage of the living area is greater than or equal to 2000, between 2010 - 2017. The fluctuations in the average price across the years are comparatively minimal. 

   ![image](https://github.com/pkrachakonda/Home-Sales/assets/20739237/57545b34-be55-41b8-9953-f649eaa66398)

***04. What is the "view" rating for homes costing more than or equal to $350,000? Determine the run time for this query, and round off your answer to two decimal places.***
   
   - Uncached Data
    
  ![image](https://github.com/pkrachakonda/Home-Sales/assets/20739237/e8b7096a-3727-45e1-8244-2f6a03b36c51)

  - Cached Data

  ![image](https://github.com/pkrachakonda/Home-Sales/assets/20739237/163dd077-ff49-408a-9636-1346cff87ec8)

  - Partitioned Data

  ![image](https://github.com/pkrachakonda/Home-Sales/assets/20739237/d612508d-4ef8-4561-ba9a-7d27e118637d)

  Based on analysis, cached data have faster runtime in comparison to uncached and the partitioned data. The partitioned data has highest runtime, indicating that partition of data was not required for this scenario. 

