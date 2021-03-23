# Amazon_Vine_Analysis
Google Colaboratory Notebook files for ETL pipeline of Amazon music reviews to
AWS PostgreSQL database and analysis of the ratio of five star reviews as it
relates to participation in the Vine program. 

## Overview of the analysis
This project includes two Google Colaboratory Notebook files:
[`Amazon_Reviews_ETL.ipynb`](Amazon_Reviews_ETL.ipynb) which employs an
AWS-based ETL pipeline to clean and transfer a data set of Amazon music
reviews into four separate tables in a PostgreSQL database, along with
[`Vine_Review_Analysis.ipynb`](Vine_Review_Analysis.ipynb) which compares the
number of five star reviews between those participating in the vine program
and those who are not.

### Resources
- Data Source:
    - [`amazon_reviews_us_Music_v1_00.tsv.gz`](https://s3.amazonaws.com/amazon-reviews-pds/tsv/amazon_reviews_us_Music_v1_00.tsv.gz)
        - See [`index.txt`](Resources/index.txt) for links to all Amazon review data sets
- Software:
    - [Google Colaboratory](https://colab.research.google.com/notebooks/welcome.ipynb)
    - Python 3.7.10
    - Spark 3.1.1
    - PySpark 3.1.1
    - AWS RDS (Managed Relational Database Service)
    - PostgreSQL 12.5-R1
    - pgAdmin 4 4.28.0

## Results
We first run our AWS RDS ETL Pipeline
[`Amazon_Reviews_ETL.ipynb`](Amazon_Reviews_ETL.ipynb) to populate the four
tables in our PostgreSQL database as shown in the following images:
- [`review_id_table`](Images/review_id_table.png)
- [`products_table`](Images/products_table.png)
- [`customers_table`](Images/customers_table.png)
- [`vine_table`](Images/vine_table.png)

We then continue our analysis to compare the number and ratio of 5 star
reviews between those included in the Vine program (paid) and those not (unpaid).
Before making this comparison, we filter the Vine data set to only contain
rows with the following conditions:

- At least 20 total votes
- Majority of votes considered helpful, i.e
`helpful_votes / total_votes >= 0.5`

We then split the resulting dataframe in two, with one dataframe containing
paid reviews, i.e `vine == "Y"`, and the other containing unpaid reviews, i.e
`vine == "N"`. We then use `.groupby("star_rating").agg(count("star_rating"))`
to obtain the count of each rating level for each dataframe, as shown in
[Rating Counts Summary](Images/rating_counts_summary.png). We then summarize
this information to obtain the dataframe shown in
[Results](Images/vine_results_df.png). In summary, we find:

- Total Number of Vine Reviews: 7, Total Number of Non-Vine Reviews: 105979
- Number of 5 Star Vine Reviews: 0, Number of 5 Star Non-Vine Reviews: 67580
- Percentage of 5 Star Vine Reviews: 0.0%, Percentage of 5 Star Non-Vine Reviews: 63.76%

## Summary
Comparing the number and ratio of 5 star reviews between those included and
not included in the Vine program, we see there is no clear positivity bias for
reviews in the program (0.0% Vine 5 star review while 63.76% Non-Vine 5 star reviews).
However, the much larger number of non-Vine reviews relative to Vine reviews
(105979 versus 7) likely indicates that this conclusion is not statistically
significant. To obtain a better understanding, increased vine review data is necessary.
From here, one could formulate a two-sample T-test with the following hypotheses:
```
H_0 : The mean rating (in number of stars) is the same between Vine and non-Vine
      reviews. i.e:
      mean_rating_vine = mean_rating_nonvine
H_a : The mean rating (in number of stars) for Vine reviews is greater than that
      of non-Vine reviews, i.e there is positivity bias, and:
      mean_rating_vine > mean_rating_nonvine
```
This would then determine if the mean rating for Vine reviews is significantly
greater than the mean rating for non-Vine reviews.
