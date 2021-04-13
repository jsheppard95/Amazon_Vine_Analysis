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

We then split the resulting DataFrame in two, with one DataFrame containing
paid reviews, i.e `vine == "Y"`, and the other containing unpaid reviews, i.e
`vine == "N"`. We then use `.groupby("star_rating").agg(count("star_rating"))`
to obtain the count of each rating level for each DataFrame, as shown in
[Rating Counts Summary](Images/rating_counts_summary.png). We then summarize
this information to obtain the DataFrame shown in
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

## Usage
The majority of the code for this analysis is contained in the two Jupyter
Notebook files [`Amazon_Reviews_ETL.ipynb`](Amazon_Reviews_ETL.ipynb) and
[`Vine_Review_Analysis.ipynb`](Vine_Review_Analysis.ipynb). Both however
require the `Spark` dependency and are thus best run using
[Google Colaboratory](https://colab.research.google.com/notebooks/welcome.ipynb).
Therefore to replicate this analysis, first clone this repository and navigate to
Google Colaboratory. 

### `Amazon_Reviews_ETL.ipynb`
Open the notebook `Amazon_Reviews_ETL.ipynb` using "File > Open notebook".
Prior to running all cells, the user should create an AWS RDS instance as
follows:

1. Navigate to the [AWS Management Console](https://aws.amazon.com/console/)
and sign in.
2. Search for "RDS" (Managed Relational Database Service) and select the first
result.
3. On the resulting page, select "Create database" and change the following
from the default options:
- "Engine options > Engine type": "PostgreSQL"
- "Templates" > "Free tier"
- "Settings > DB instance identifier": `<Database Name>`
- "Master username": `<Username>`, or use default `postgres`
- "Master password": `<Password>`, separate from pgAdmin4 password
- "Connectivity > Public access": "Yes"
4. Select "Create database".
5. After creating the database, update which IP addresses can access it by
first navigating to the database instance on AWS (select "Databases" in side
pane and choose the recently created database).
6. Scroll down to "Security group rules" and select the first "Security group".
7. Choose the only "Security group ID" shown.
8. Select "Edit inbound rules"
9. For the first entry shown under "Inbound rules", change the "Type" to
"PostgreSQL" and the "Source" to "Anywhere".
- This is not the best practice for production, but in this example it
simplifies connecting to the database.
10. Select "Save rules".
11. Now select the "Outbound rules" tab and then "Edit outbound rules".
12. For the first entry shown under "Outbound rules", change the "Type" to
"All traffic" and the "Destination" to "Anywhere".
13. Select "Save rules".
14. The database is now instantiated and accessible from any IP address,
though the database password is still required.

With the RDS instance now created, the user should connect pgAdmin4 to its
endpoint for local access. This is accomplished as follows:

1. Navigate to the created RDS instance on the AWS Management Console.
2. Copy the "Endpoint" shown under "Connectivity & security"
3. Open and log into pgAdmin4.
4. Select "Add New Server" and set the following:
- Under the "General" tab, name the connection something like "AWS".
- Choose the "Connection" tab and paste the copied RDS endpoint to the
"Host name/address" setting and use the default Port 5432.
- Leave the "Username" as "postgres" unless a different username was chosen
during creation of the RDS instance.
- Fill in the "Password" with the same password set during creation of the RDS
instance.
5. Choose "Save" to establish the connection.
7. Establish the necessary database structure by using the "Query Tool" for
the instantiated database.
8. Open and run the queries contained in [`schema.sql`](schema.sql)

With the RDS instance created, connection to pgAdmin4 established, and
database schema defined, the user can now establish connection in
`Amazon_Revies_ETL.ipynb`:

1. Before returning to Google Colaboratory, copy the `<connection_string>` for
the RDS instance:
- In pgAdmin4 right-click on the "AWS" connection shown in the "Server"
directory
- Select "Properties"
- Select the "Connection" tab
- Copy the address in the "Host name/address" field, this is the
`<connection_string>`
2. Return to Google Colaboratory, and in the first cell under "Connect to the
AWS RDS instance and write each DataFrame to its table", replace this
`<connection_string>` along with the `<Database Name>`, `<Username>`, and
`<Password>` that are currently shown with those created in previous steps:
```
mode = "append"
jdbc_url = "jdbc:postgresql://<connection_string>:5432/<Database Name>"
config = {"user": "<Username>", 
          "password": "<Password>", 
          "driver": "org.postgresql.Driver"}
```
3. One can then run all cells in `Amazon_Reviews_ETL.ipynb`, return to
pgAdmin4, and query the recently created tables to confirm the data loading
into the AWS RDS instance was successful.

After completion of this AWS RDS pipeline, the user should shut down its
instance to ensure they do not incur unexpected charges. This is accomplished
as follows:

1. Navigate to the RDS Service page from the AWS Management Console.
2. Select "DB Instances".
3. Select the checkbox for the recently created database.
4. Under the "Actions" dropdown menu, select "Delete" and confirm deletion.
- There is no need to "Create final snapshot" or "Retain automated backups"
and so these option should not be selected.

### `Vine_Review_Analysis.ipynb`
`Vine_Review_Analysis.ipynb` does not require a user-defined AWS RDS instance
and therefore one can simply open this notebook in Google Colaboratory and run
all cells. This will include reading the Amazon review data set from an AWS
S3 instance into a Spark DataFrame. Subsequent cells then include filtering
this DataFrame and parsing the result to determine the number and percent of
five star reviews among the Vine program participants and non-participants.
