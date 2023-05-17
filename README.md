# A-CLINICAL-TRIAL-ANALYSIS-USING-RDD-DATAFRAME-AND-SQL

A big data tool is software that gathers data from numerous intricate data sets and types, evaluate it, and then offers useful data. Businesses utilise the best big data technologies that make managing big data simple since traditional databases cannot handle enormous amounts of data. Big data techniques are methods used when gathering data. For this course work, we would be using the Machine Learning, Decision-Tree, Random Forest Classifier and Logistic Regression techniques to analyse our data set.
The main tool we would be using for this analysis is Databricks. Databricks is a tool that can be used in building, sharing, deploying, and sustaining enterprise grade data solution at scale. It is important to use Databricks because its applications are for managing data management and the workloads of data pipelines. It can also be used for data visualization, as well as to implement machine learning models. Machine learning development can be done using R, Python, and Spark programming.
For this coursework three python notebooks were created on Databricks for the RDD Data-frame and SQL, while a machine learning notebook was created for the machine learning implementation.
A cluster was created on Databricks for the implementation of RDD, Data-frame and SQL, while a different cluster was created for the machine learning implementation as this would enable the different clusters to run.

## DATA CLEANING AND DATA PREPARATION

## IMPEMENTATION OF DATASET FOR RDD
A cluster Big Data Course work was created. After creating the cluster, a notebook was created for the implementation of RDD, Data frame and SQL.

![image](https://github.com/Orlawlardey/A-CLINICAL-TRIAL-ANALYSIS/assets/124607057/de80c2e7-ef6e-445d-99bd-8155c2a79c9a)

All files clinicaltrial_2019, clinicaltrial_2020, clinicaltrial_2021 and the pharma file were imported to the Databricks file store for processing. After importing the files, rerunnable codes were created for RDD, Data-frame and SQL. This is to ensure that any year inputted would generate the accurate result.

![image](https://github.com/Orlawlardey/A-CLINICAL-TRIAL-ANALYSIS/assets/124607057/ff570991-4240-40fd-af14-79dcd1b1f293)


For RDD, the year in the code above ensures that the string variable called year is defined and ensures that it is assigned to the value ‘2021’. The file root ensures that the filename of the ZIP file is more flexible and manageable. The dbutils.fs.cp ensures that we can move the files from one location to another.

![image](https://github.com/Orlawlardey/A-CLINICAL-TRIAL-ANALYSIS/assets/124607057/6a273720-c5d7-4215-9f68-d72ffce02dc9)


The clinicaltrial_2021 zipped file was unzipped using reusable environment variables which moves the file to the local node, unzips, renames and returns the file to the Databricks file store for processing. 

![image](https://github.com/Orlawlardey/A-CLINICAL-TRIAL-ANALYSIS/assets/124607057/fe493ce5-3333-4537-a11f-4c5934e05361)


![image](https://github.com/Orlawlardey/A-CLINICAL-TRIAL-ANALYSIS/assets/124607057/c8460c9d-1d0b-40ee-9fdd-f626c7da9d94)


After unzipping the file, a new directory was created for the unzipped file. This is because a zipped file might include a lot of files and directories when it is extracted, making it challenging to handle if they are all extracted into one directory. The extracted files and folders can be organised and grouped together to make them easier to identify and manage by establishing a new directory and extracting the contents into it. It is also important to move the content of the file to the newly created directory.

![image](https://github.com/Orlawlardey/A-CLINICAL-TRIAL-ANALYSIS/assets/124607057/4f094d99-f5e8-45ea-abe4-50fa3cf1204c)

We can therefore view the content of the file that was unzipped and newly created.

![image](https://github.com/Orlawlardey/A-CLINICAL-TRIAL-ANALYSIS/assets/124607057/27a7299e-0761-40c5-b6de-a1d2803340d4)


After we have successfully created the right directory for the clinicaltrial_2021 file, an RDD clinicalrdd was created by reading in the variable file root which is assigned to the 2021 clinical trial file.

## IMPLEMENTATION OF DATASET USING DATA-FRAME

For data-frame implementation, the file root variable was created to enable it to be rerunnable at any given time. It was important to import the OS function, this is to enable the file to be read from the file. The environment variable is used to enable access to the created variable in the command line.

![image](https://github.com/Orlawlardey/A-CLINICAL-TRIAL-ANALYSIS/assets/124607057/c4807873-b625-400b-8915-262237ac80b6)


## IMPLEMENTATION OF DATASET FOR SQL

![image](https://github.com/Orlawlardey/A-CLINICAL-TRIAL-ANALYSIS/assets/124607057/cf6af258-4164-4721-869d-80d7e249cc2f)


An SQL clinicaltrial_2021 was created using spark.read.csv, the file delimiter and header are indicated in the options argument. A temporary view was also created as this is a Data Frame that resembles a table and may be used to execute SQL queries against the data. The Spark SQL API may be used to perform SQL commands on a temporary view after it has been built, making it simpler to analyse the data.

## DISTINCT COUNT OF STUDIES
•	**RDD IMPLEMENTATION AND RESULTS **

The assumption here is that the clinical trial 2021 file contains distinct count of studies.
An RDD clinicalrdd is created by reading in the variable file root which is assigned to the 2021 clinical trial file.

![image](https://github.com/Orlawlardey/A-CLINICAL-TRIAL-ANALYSIS/assets/124607057/58d8c84c-0204-4c94-b401-df3e08f091ae)


![image](https://github.com/Orlawlardey/A-CLINICAL-TRIAL-ANALYSIS/assets/124607057/c5836244-bf5d-4db1-8316-0a305fd77977)


![image](https://github.com/Orlawlardey/A-CLINICAL-TRIAL-ANALYSIS/assets/124607057/f95b9958-5faf-40e4-b953-7d882bb66395)


Image Fig 8 helps us view the header in the file and also helps us take out the header in the file. This is to ensure that there is accuracy in the results derived. The first line of the record which are the header columns are filtered out upon creation of the RDD using the filter transformation, the filter matches ‘Id’ of the header columns on the first line of records.

![image](https://github.com/Orlawlardey/A-CLINICAL-TRIAL-ANALYSIS/assets/124607057/396c21f6-f682-4a88-bd9c-40255aaae94a)


From the image in Fig 7, when the RDD was created, the first line records the header columns. However, it is important that the filter transformation is used to filter out the headers. This is to ensure that RDD contains the actual data related the clinical trial data for 2021. Image in Fig 7, shows the content of the file with the headers while image in Fig 9, shows the headers have been filtered out.

![image](https://github.com/Orlawlardey/A-CLINICAL-TRIAL-ANALYSIS/assets/124607057/7d7b9312-d076-4c71-afd9-432bbc5fd880)

The .map function in the code ran in Fig 10, ensures that the lambda function splits each line of text using the "|" character as the delimiter, and then  creates a list of values for each line while the .filter function filters out any line that contains only one value.

![image](https://github.com/Orlawlardey/A-CLINICAL-TRIAL-ANALYSIS/assets/124607057/cfadeeb1-fd22-4e5d-a80c-9e1dffcaef0a)

![image](https://github.com/Orlawlardey/A-CLINICAL-TRIAL-ANALYSIS/assets/124607057/e500c740-a344-4ac6-85f6-a8c87936bfb0)


From images Fig 11 and Fig 12, we have been able to split the data in the clinicaltrial_2021 file into a list.

![image](https://github.com/Orlawlardey/A-CLINICAL-TRIAL-ANALYSIS/assets/124607057/37d7e2db-7a42-4f7f-9ba8-b3405fc3f6ad)


From image in Fig 13, we can see that the distinct count of studies for the clinical trial in the year 2021 was 387,261 as against the studies in 2019 and 2020 which amounted to 326,348 and 356,466 respectively. This therefore means that more clinical trial studies were carried out in the year 2021.


•	**DATA-FRAME IMPLEMENTATION AND RESULT**

A Data frame clinicalDf was created using spark.read.csv, the file delimiter and header are indicated in the options argument and the file is read in from the file root variable.

![image](https://github.com/Orlawlardey/A-CLINICAL-TRIAL-ANALYSIS/assets/124607057/5b66d89a-eb4a-4e78-86e3-e2ad242e7d39)


To obtain the number of distinct studies in the clinical trial in 2021, the count() operation is applied on the clinicalDf.

![image](https://github.com/Orlawlardey/A-CLINICAL-TRIAL-ANALYSIS/assets/124607057/1aec4f25-d6f9-4f09-9e86-b0edee7e2c80)


From the image in Fig 15, the distinct count of the number of clinical trial analysis done in 2021 was 387,261 using the data-frame implementation method.

•	**SQL IMPLEMENTATION AND RESULT**

To run the SQL implementation the SELECT DISTINCT statement with a COUNT and FROM was used to obtain the distinct number of studies in the clinical trial analysis in 2021.

![image](https://github.com/Orlawlardey/A-CLINICAL-TRIAL-ANALYSIS/assets/124607057/2322543a-0aae-472a-af3d-31daaed9fc5a)


From the image in fig 16, the total number of distinct count of studies done during the clinical trial analysis in 2021 was 387,261.
The values derived using both RDD, Data frame and SQL to count the total number of clinical trial analysis in 2021 were 387,261.


## TYPES OF STUDIES AND THEIR FREQUENCIES

The assumption in this question is that studies are classified as one of the following Interventional, Observational, Observational [Patient Registry] and Expanded Access, and all unnamed studies are invalid.

•	**RDD IMPLEMENTATION AND RESULT**

From the files given, the different type of studies was classified as Interventional, Observational, Observational [Patient Registry] and Expanded Access while other studies that were unnamed are invalid.

![image](https://github.com/Orlawlardey/A-CLINICAL-TRIAL-ANALYSIS/assets/124607057/7b1aa32b-1cfd-440e-a2d4-33e7afe1b02e)


To obtain the types of studies and their frequencies, the map() function was implemented to ensure that each column of RDD clinicalrdd is extracted using the Type attribute. The groupByKey() was implemented to ensure that the extracted tuples are grouped according to their data type. The mapValues() function was implemented to ensure that the number of tuples in each group is counted. The sortBy() function was implemented to sort the groups by their count in descending order.
From the image in fig 17, we derived that 4 types of studies were carried out during the clinical trial analysis in 2021. The interventional type of studies was a total of 301,472, Observational type of studies was 77,540, Observational with patient registry was 8,180 while Expanded Access study was 69.
Comparing these figures to the types of studies done during the clinical analysis in 2019 and 2020. It has been observed that the Interventional type of study is the most frequent study used when carrying out clinical analysis.

•	**DATA-FRAME IMPLEMENTATION AND RESULTS**

Using Data-frame to obtain the types of clinical analysis done in 2021, the clinicalDf was grouped according to Type. We also had to ensure that the count function was implemented to the number of the types of studies. The groupBy() and count() function were implemented for this to be achieved.
This therefore created a new Data-Frame typeDF.  Using the sort() function, the typeDF is sorted by most frequent study, returning all study types in most frequent order. 

![image](https://github.com/Orlawlardey/A-CLINICAL-TRIAL-ANALYSIS/assets/124607057/16402b74-1a5d-468e-8ebc-f93b1c51428e)

Using the Data-Frame implementation, the type of study frequently used is the interventional study with a count of 301,472 studies in 2021. The other studies used during the 2021 clinical trial analysis were observational with a count of 77,450, observational (patient registry) with a count of 8,180 and the expanded access study with a count of 68 studies.

•	**SQL IMPLEMENTATION AND RESULT**

Using SQL to obtain the type of studies carried out during the clinical trial analysis in 2021 the SELECT statement was used to retrieve the Type column, the COUNT function was used to count the number of studies carried out during the analysis, the AS function was used to ensure that the studies counted are counted as frequency, while the FROM statement ensured that the type of study that was counted was from the clinicaltrial_2021 file. The GROUP BY statement ensured that the studies were grouped by their type, while the ORDER BY statement ensured that the type of studies already grouped are arranged in descending order. The count column is represented by the Frequency, and the most prevalent studies in the clinical trial in 2021 are the outcome.

![image](https://github.com/Orlawlardey/A-CLINICAL-TRIAL-ANALYSIS/assets/124607057/ef52ec34-3457-441e-b295-039d7f055069)

From the image in fig 19, the result derived using the SQL implementation to determine the types of clinical trial analysis in 2021 is the same as the results derived when the RDD implementation and Data- Frame implementation was implemented. 

## TOP FIVE (5) CONDITIONS AND THEIR FREQUENCIES

The assumption here is that the condition column contains the different types of conditions.

•	**RDD IMPLEMENTATION AND RESULT**

The clinicalrdd.map(lambda col: col[Conditions]) function makes sure that each row in RDD is mapped to the 'Condition' value in order to calculate the number of different sorts of conditions that are present in the clinical trial analysis performed in 2021 using the 'Conditions' column. Consequently, this would guarantee that a new RDD column is created for "Conditions". map(lambda x: (x,1)) makes sure that each condition is associated with the number 1.
 .groupByKey().mapValues(len) counts the number of times each condition occurs when grouping the new RDD key by the condition value. .sortBy(lambda x: x[1], ascending=False) ensures that each RDD is classified according to how frequently it occurs.


With the image in fig 20, we can see a breakdown of the different conditions that were analysed during the 2021 clinical trial analysis.

![image](https://github.com/Orlawlardey/A-CLINICAL-TRIAL-ANALYSIS/assets/124607057/9dea84fb-51bc-4e11-86c5-aed6b5b8a79a)

However, from the result derived it was observed that more than one condition was listed for some conditions. It is therefore important that these conditions are splitted into individual conditions to enable us to get the accurate top 10 conditions that were present during this analysis.

![image](https://github.com/Orlawlardey/A-CLINICAL-TRIAL-ANALYSIS/assets/124607057/505a4fd1-aba2-400c-a687-a9b9f806468c)


The clinicalrdd is chained in a sequence of map, filter, and flatMap transformations. This chain of transformation ensures that the Conditions column is extracted, the null values are removed and ensure that there is a split of more than one condition joined together. From the image in fig 22, we can see that there has been a split done for the different conditions in the clinical trial analysis in 2021.

![image](https://github.com/Orlawlardey/A-CLINICAL-TRIAL-ANALYSIS/assets/124607057/5b18d290-c189-4578-8ea0-816129052b6f)


From the result derived in fig 23, the top 5 conditions present during the clinical trial analysis done in 2021 are Carcinoma with a total of 13,389 patients, a total of 11,080 patients had Diabetes Mellitus, a total of 9,371 patients had Neoplasms, a total of 8,640 patients had Breast Neoplasms while a total of 8,032 patients had Syndrome.

•	**DATA-FRAME IMPLEMENTATION AND RESULT**

For the data-frame implementation, it was important to use the pyspark.sql.functions to import split, explode, trim and count. The split function was used to extract specific values from the string column, the explode function was used to split the array column into several rows, the trim function was used to trim the spaces from both ends for the specified string column, while the count function was used to count the number of Conditions.

![image](https://github.com/Orlawlardey/A-CLINICAL-TRIAL-ANALYSIS/assets/124607057/2d6c2193-f666-4b06-9af5-048b135e9b63)


![image](https://github.com/Orlawlardey/A-CLINICAL-TRIAL-ANALYSIS/assets/124607057/f1d3c45a-0777-425f-b0c8-952a80ce0ebf)


The new data-frame condDf was created to ensure that only the Conditions with values in the ClinicalDf data-frame are selected without any null values.

![image](https://github.com/Orlawlardey/A-CLINICAL-TRIAL-ANALYSIS/assets/124607057/cc15fb99-a1c9-473f-b35a-80b4c7ca6b2b)


From the image in fig 26, we can see that the condDf data frame selected only conditions with values.

![image](https://github.com/Orlawlardey/A-CLINICAL-TRIAL-ANALYSIS/assets/124607057/ed7c6501-349a-4a96-a50c-831316d76b2d)

From fig 27, we were able to use the explode function to split the conditions into several rows, while the split function was used to split the Conditions using comma (“,”).

![image](https://github.com/Orlawlardey/A-CLINICAL-TRIAL-ANALYSIS/assets/124607057/2f1272cc-d5b5-4080-bde9-f46cc25ea26d)


![image](https://github.com/Orlawlardey/A-CLINICAL-TRIAL-ANALYSIS/assets/124607057/173c48e1-c6c2-487d-9223-e7cbb4acaa94)


The result for the data frame implantation for the top 5 common conditions and their frequencies is the same as the result derived from the RDD implementation where the patients with Carcinoma were a total of 13,389, Diabetes Mellitus patients were a total of 11,080, Neoplasms patients were a total of 9,371, Breast Neoplasms were a total of 8,640 while a total of 8,032 patients had Syndrome.

•**	SQL IMPLEMENTATION AND RESULT**

The split function was implemented to ensure that the Condition column in the clinicaltrial_2021 analysis creates a different column that contains the conditions on each row. The explode function was implemented to ensure that there is only a single condition on each row. The SELECT statement was implemented to ensure that the Conditions column is selected FROM clinicaltrial_2021. The COUNT function returns the number of records, this is incorporated using the GROUP BY clause and ordered by descending number of conditions AS Frequency. The result is limited to display the top 5 conditions and their frequencies.


![image](https://github.com/Orlawlardey/A-CLINICAL-TRIAL-ANALYSIS/assets/124607057/1579b20e-ae96-4f8f-be50-d3c807345616)


The result derived in the SQL implementation is the same as the result derived from the RDD and Data frame implementation for the top 5 conditions that were predominant in the 2021 clinical trial analysis.

****TOP 10 SPONSOR COMPANIES 

The assumption here is that the parent company column contains all the possible pharmaceutical companies.

•	**RDD IMPLEMENTATION AND RESULT**

For this implementation, the pharma file was imported into the workspace. The file was unzipped and stored into a new file directory.

![image](https://github.com/Orlawlardey/A-CLINICAL-TRIAL-ANALYSIS/assets/124607057/318c61f7-155f-4ee8-aac8-aa14c332b0ce)


![image](https://github.com/Orlawlardey/A-CLINICAL-TRIAL-ANALYSIS/assets/124607057/5785684e-98b8-456e-a6d1-2908b673642b)

The pharmardd data frame was created to read the text file.

![image](https://github.com/Orlawlardey/A-CLINICAL-TRIAL-ANALYSIS/assets/124607057/252dcda4-4ce7-42eb-b54c-a519290a4ade)

The pharma file header was defined and removed. This would enable accurate computation of the results from the dataset.

![image](https://github.com/Orlawlardey/A-CLINICAL-TRIAL-ANALYSIS/assets/124607057/ec14641b-6374-4259-b2a0-07ed34820f14)


The map function was used in the transformation of the data set, this therefore applies a lambda function to each element of clinicalrdd_clean. The lambda function splits each element by the | delimiter and extracts the second column (index 1), which is then returned as a new element in clinical_col_rdd. 

![image](https://github.com/Orlawlardey/A-CLINICAL-TRIAL-ANALYSIS/assets/124607057/94d59c05-6718-4b3b-845e-010330007a9c)

![image](https://github.com/Orlawlardey/A-CLINICAL-TRIAL-ANALYSIS/assets/124607057/641cb5ee-4f1d-4888-85f3-ebcabbe50048)


The map() function was used to splits each element in pharmardd by the string "," and returns the second element (index 1) of the resulting list.

![image](https://github.com/Orlawlardey/A-CLINICAL-TRIAL-ANALYSIS/assets/124607057/b1893d57-430b-4ab7-b645-1fe166524b31)


The subtract() function was used to  take the elements in clinical_col_rdd and removes the elements that are also present in pharma_col_rdd2.

![image](https://github.com/Orlawlardey/A-CLINICAL-TRIAL-ANALYSIS/assets/124607057/19684b18-f3ce-4ccc-84fd-b0e550f747ed)


The map function transforms each element of the clinical_only_rdd into a tuple where the clinical data is the key and the value is 1. 
The reduceByKey function was used to reduce the operations on the tuples with the same key.
The result derived shows the top 10 sponsors that are not pharmaceutical companies.

## DATA-FRAME IMPLEMENTATION AND RESULT

Using the Data-frame implementation, a pharmadf was created and the spark.read.csv option was used in creating this data frame. The select() function was used to extract the pharmadf and create a new data-frame parent_companydf which contains information of all the pharmaceutical companies in the file.


![image](https://github.com/Orlawlardey/A-CLINICAL-TRIAL-ANALYSIS/assets/124607057/69cd3969-a8b1-487e-825c-a264e1e256c7)

![image](https://github.com/Orlawlardey/A-CLINICAL-TRIAL-ANALYSIS/assets/124607057/310cf6e5-82bb-4559-a322-1cd6015974cb)


![image](https://github.com/Orlawlardey/A-CLINICAL-TRIAL-ANALYSIS/assets/124607057/7292718f-d461-451d-829c-ea9eb108c6f0)


The image above shows the content of the pharmadf and the parent_companydf data-frame created.

![image](https://github.com/Orlawlardey/A-CLINICAL-TRIAL-ANALYSIS/assets/124607057/1ccf3683-5d35-4517-8dae-cc7fed099d7e)

The sponsordf data frame was created by using the clinicalDf data frame to extract the details of sponsors in the clinical trial file.

![image](https://github.com/Orlawlardey/A-CLINICAL-TRIAL-ANALYSIS/assets/124607057/264fba1f-7eb7-49f6-9df6-7aaa1515d9b8)


![image](https://github.com/Orlawlardey/A-CLINICAL-TRIAL-ANALYSIS/assets/124607057/9c92d914-fb92-43bf-ad9a-bcba31adf402)


In the pharma_sponsordf data frame, a left anti join was used to connect two data frames, sponsordf and parent_companydf, based on the columns Sponsor and Parent_Company, respectively. This means that only records from sponsordf that do not have a matching record in parent_companydf based on the specified columns will be returned. 
The pharma_sponsordf1 data frame ensures that the data frame is grouped by the Sponsor column and then the count() function is applied to get the number of occurrences of each sponsor.

The pharma_sponsordf1.sort code ensures that the sponsors are sorted in descending order based on the count column and limiting the output to the top 10 records of the most common sponsors that are not pharmaceutical companies.

From the results derived, we can see that National Cancer Institute (NCI) is the most common non pharmaceutical company with a count of 3,218 sponsors in the clinical trial analysis carried out in 2021.

Comparing the data derived from the clinical trial analysis carried out in 2019 and 2020, we can see that the National Cancer Institute (NCI) has had the highest number of nonpharmaceutical sponsors with a count of 3,003 sponsors in 2019 and 3,100 sponsors in 2020.

•**	SQL IMPLEMENTATION AND RESULTS**

For the SQL implementation, the createOrReplaceTempView() function was implemented to create or replace a temporary view for the pharm data frame.

![image](https://github.com/Orlawlardey/A-CLINICAL-TRIAL-ANALYSIS/assets/124607057/451fab55-1393-441a-acdc-750b3b81b521)

![image](https://github.com/Orlawlardey/A-CLINICAL-TRIAL-ANALYSIS/assets/124607057/326de98d-bf76-405a-bf69-dabc3086cd88)

![image](https://github.com/Orlawlardey/A-CLINICAL-TRIAL-ANALYSIS/assets/124607057/5abfcddf-be07-48b4-b932-a66792599749)

The SELECT statement was used to select the sponsor column FROM the clinicaltrial_2021 file. The COUNT function was used to count the number of sponsors in the sponsors column. The LEFT JOIN function joins the clinicaltrial_2021 table with the Pharma table selecting the Sponsor and the Parent_Company column.
The WHERE clause ensure that the results derived are filtered to show only records where the Parent_Company value in Pharma is null and void.
The GROUPBY function was applied to group the nonpharmaceutical sponsors and the count function was used to count the number of sponsors as freq.
The ORDER BY function was used to sort the count of sponsors in descending order.
The results derived is the same as the results derived from the RDD and Data Frame implementation, where the National Cancer Institute (NCI) had the highest number of nonpharmaceutical vendors (3,218) during the clinical trial analysis carried out in 2021.

## NUMBER OF COMPLETED STUDIES EACH MONTH

The assumption here is that there was no complete analysis done for all the months in 2021.

•	**RDD IMPLEMENTATION**
For the RDD implementation, date time and calendar were imported. The completed studies data frame was created.

![image](https://github.com/Orlawlardey/A-CLINICAL-TRIAL-ANALYSIS/assets/124607057/299421b5-eeba-40f6-8c2b-93ead4ef62c6)


![image](https://github.com/Orlawlardey/A-CLINICAL-TRIAL-ANALYSIS/assets/124607057/52d84473-ee5f-454b-84e7-051e43b07d67)


The results derived shows that there were completed studies done from January to October 2021. However, there were no completed studies in November and December 2021.

## THE VISUALIZATION OF COMPLETED STUDIES

![image](https://github.com/Orlawlardey/A-CLINICAL-TRIAL-ANALYSIS/assets/124607057/0d6eaec4-bdc9-4151-abff-ea4de5a832d8)

![image](https://github.com/Orlawlardey/A-CLINICAL-TRIAL-ANALYSIS/assets/124607057/ad8b82d3-c29e-405d-91f5-92fe446fdc77)


For the visualization the mat plot library was imported to enable the visualization.
The visualization result shows that there was a high number of completed studies carried out in the month of March 2021.

•	**DATA-FRAME IMPLEMENTATION AND RESULTS**

Using Dataframe, completed_df was created to enable us to select the status of each study and when each study was completed using the cilnicalDf. From the results, many studies have been completed, some are recruiting, some are active but not recruiting while some statuses are unknown.

![image](https://github.com/Orlawlardey/A-CLINICAL-TRIAL-ANALYSIS/assets/124607057/d9cb9c8f-6f08-48b0-bc53-a0e4ec0f3cd3)

![image](https://github.com/Orlawlardey/A-CLINICAL-TRIAL-ANALYSIS/assets/124607057/17b3f1a7-c621-4de0-8d81-b262d49c9bfe)

The pyspark.sql.functions was implemented to import the col which would be used to select the preferred column, the to_date would be used a timestamp column to a date column, while the date_format would be used to format a date or timestamp column as a string.

![image](https://github.com/Orlawlardey/A-CLINICAL-TRIAL-ANALYSIS/assets/124607057/80695026-9892-448b-9b41-956d59bb87ee)


The select function in the completedstatus_df dataframe was implemented to ensure that only the Status and Completed columns were selected. The .filter function was implemented to ensure that the only the status that shows completed is selected with the year the study was completed.

![image](https://github.com/Orlawlardey/A-CLINICAL-TRIAL-ANALYSIS/assets/124607057/f4dc6bda-15b2-4e1e-aea5-3a6291447085)


![image](https://github.com/Orlawlardey/A-CLINICAL-TRIAL-ANALYSIS/assets/124607057/e07f7306-0b50-47ca-bc94-d56374737566)


The completedstatus_df data frame was implemented to select the Completion column and the to_date function was implemented to get the completion format in the month and year format.
From the results derived, the highest number of studies completed during the clinical trial analysis in 2021 was in March which had a count of 1,227. The analysis carried out in 2019 and 2020 showed that in December 2019 the highest number of completed studies was 2,690 while the highest number of completed studies in 2020 was 2,084 in December.  Comparing this to the clinical trial analysis carried out in these years it can be said that the highest number of completed studies so far was in December 2019.

•	**SQL IMPLEMENTATION AND RESULTS**

For the SQL implementation, the SELECT statement was used to select the Completion column AS months, while the COUNT function was used to count the number of completed studies as Number_of_Completed_Studies. The FROM statement was used to ensure that the Completion column was selected from Clinicaltrial_2021. The WHERE function was used to ensure that only where completion was LIKE 2021 AND Status was completed is selected. The GROUP BY function was used to group the completed studies by months.

![image](https://github.com/Orlawlardey/A-CLINICAL-TRIAL-ANALYSIS/assets/124607057/52005406-099e-4458-ba5a-f0f8ab5c81d3)


The results derived using the SQL implementation is the same as the results derived when implementing the RDD and Data Frame implementation. From the results derived, was also observed that there were no completed studies in the months of November and December during the clinical trial analysis carried out in 2021 compared to 2019 and 2020.

## FURTHER ANALYSIS SQL IMPLEMENTATION

![image](https://github.com/Orlawlardey/A-CLINICAL-TRIAL-ANALYSIS/assets/124607057/0768c203-67a8-46eb-9741-83db4cabffd1)


The SELECT statement was used to select the ownership structure column from the pharma file. The WHERE and AND statement was used to ensure that only rows with values and rows that do not contain USA as a value are selected. The GROUP BY statement was used to group the derived result as ownership structure and the ORDER BY statement was used to arrange the results in descending order.

The results derived shows that publicly traded ownership structure has a count of 894, privately held structure has a count of 50 while those out of business have a count of 21.

## FURTHER ANALYSIS DATAFRAME IMPLEMENTATION

![image](https://github.com/Orlawlardey/A-CLINICAL-TRIAL-ANALYSIS/assets/124607057/4eaeb630-9f47-4e9d-aa30-48853f858067)

From this analysis carried out, the result shows that many pharmaceutical companies have their headquarters in USA.

## FURTHER ANALYSIS RDD IMPLEMENTATION

![image](https://github.com/Orlawlardey/A-CLINICAL-TRIAL-ANALYSIS/assets/124607057/12d15989-f662-4241-981f-f47aa383ccde)

The RDD implementation was used to determine the distinct count of the group of offences for the pharmaceutical companies. The result derived shows that there are 9 distinct group of offences.










