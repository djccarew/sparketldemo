#  Spark ETL demo

This demo (with Watson Studio Jupyter notebooks  in both Python and Scala)  illustrates the use of a Spark cluster to perform ETL. It imports data in flat files into Spark DataFrames, manipulates the data, aggregates it and then writes the result out to a relational database. The advantage of using Spark for this is scalability (by using a larger cluster one can achieve close to linear scalability) and simplified error recovery (a failed attempt at running this ETL job can be repeated at any stage and the final result will be the same).

##  Setup

### 1 Sign up for Watson Studio 

If you are not already signed up for Watson Studio, [sign up here](https://www.ibm.com/cloud/watson-studio)

### 2 Create a Watson Studio Project 

2.1 From the Watson Studio home page click on **New Project**

2.2 Select **Complete** as the project type and click **OK**

2.3 Name the project *Spark ETL Demo* and click **Create**

### 3. Create an instance of the Spark Service

3.1 Click on the **Settings** tab 

3.2 Scroll down to the *Associated services* section and click on **Add service**

3.3 Follow the prompts to create a new instance of the IBM Cloud Spark service (Lite Plan)

### 4. Create  and run this lab's notebook

4.1 Click on the **Assets** tab of your Watson Studio project

4.2 Scroll down to the *Notebooks* section and click on **New Notebook**

4.3 Name the Notebook *Spark ETL Demo Python/Scala* (depending on which version of the notebook you want to run. Select **From URL** and copy the following URL into the **Notebook URL** field for the Python version

```https://raw.githubusercontent.com/djccarew/sparketldemo/master/SparkETLDemoPython.ipynb```

Use the following URL for the Scala version

```https://raw.githubusercontent.com/djccarew/sparketldemo/master/SparkETLDemoScala.ipynb```

4.4 Select the instance of your Spark service as the runtime and select either Python 3.5 or Scala as the  language depending  on which  version of the  notebook you're importing 

4.5 Click on **Create Notebook**. After a few seconds the notebook should be loaded

### 5. Create the DB2 target database 

5.1 In a separate browser tab go to [](http://bluemix.net). Select the Catalog and create an instance of DB2 (Lite Plan) 

5.2 When the instance is created, click on **Open Console**

5.3 Click on the "hamburger" icon at the top left and select **RUN SQL**

5.4 Enter the following SQL and run it to create the table that the demo will use to store the result of the ETL:

```
create table TXSSByCounty (
    County VARCHAR(64) NOT NULL,
    NumTotal BIGINT NOT NULL,
    NumRetired BIGINT NOT NULL,
    NumDisabled  BIGINT NOT NULL,
    NumWidowerOrParent  BIGINT NOT NULL,
    NumSpouses  BIGINT NOT NULL,
    NumChildren  BIGINT NOT NULL,
    BenTotal  BIGINT NOT NULL,
    BenRetired  BIGINT NOT NULL,
    BenWidowerOrParent  BIGINT NOT NULL,
    NumSeniors  BIGINT NOT NULL
)
```

### 6. Run the notebook

6.1  Go back to tthe  open notebook and run  each code block in turn. Make sure you put your DB2 credentilas in the  final code block so the data will be written to your DB2 instance. Once the notebook has run successfully, use your DB2 console to verify that the data has been loaded sucessfully into your database.