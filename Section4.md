Structured APIs in Apache Spark allow handling structured, semi-structured, and unstructured data.



These APIs include Datasets, DataFrames, and SQL tables/views.

The Structured APIs are fundamental for defining data flows in Spark.



The Structured API is Spark’s high-level interface for working with:



DataFrames



Datasets



SQL



This API helps you work with structured and semi-structured data similar to SQL tables.







The DataSet API does not have support for python.

The Dataset API is a collection of strongly type JVM data objects in scala and classes in java.



Typed objects enforces a DataType.

i.e the code;

float avg\_age=2.5

avg\_age="I love Anime"// will cause an error



UnTyped objects does not enforces a Data Type.

i.e the code;

float avg\_age=2.5

avg\_age="I love Anime"// will not cause an error



The DataFrame is an untyped Api that has support for python and scala







Note:A schema is the blueprint or structural design of a database. It defines how data is organized, including tables, fields, relationships and constraints.



Lets create the environment for the DataFrame API



1.Open cmd

2.cd sparktest

3.mkdir sparkdf

4.cd sparkdf

5.mkdir data

6.mkdir testdata

7.python -m venv venv/      /// this creates a python environment which contains only python libraries



8.venv\\Scripts\\activate

9.pip list

10.pip install wheel

11.pip install pyspark

12.pip install pandas

13.pip install seaborn

14.pip install jupyterlab

14.jupyter lab

// this will launch default lab in the browser









After the Jupyter lab opned in the browser

select Python under the Notebook this will create a JupytER NoteBOOk

now rename the Jupyter Note Book as Pyspark\_DataFrame\_Pratice





lets import the pyspark and other importing





from pyspark.sql import SparkSession

from pyspark.sql import StructType, StructField, StringType, IntegerType

runit



//These are used to define a schema for a DataFrame manually.



Schema = structure of your data (column names + data types)



You use this when:



your file has no header



you don’t want Spark to guess the data types



you want strict control over column types







spark =(SparkSession.builder.appName("PySparkDFPratice").getOrCreate()

run it





Download and copy the contents of "persons.txt" and paste into thr cell



data = \[("James","","Smith","36636","M",3000),

    ("Michael","Rose","","40288","M",4000),

    ("Robert","","Williams","42114","M",4000),

    ("Maria","Anne","Jones","39192","F",4000),

    ("Jen","Mary","Brown","","F",-1)

  ]

run it



type(data)

run it



Now lets create a schema and assign the appropriate data types



schema = StructType(\[

 	StructField("firstname" , StringType(), True),

 	StructField("middlename" , StringType(), True),

 	StructField("lastname" , StringType(), True),

 	StructField("Id" , StringType(), True),

 	StructField("Gender" , StringType(), True),

 	StructField("Salary" , IntegerType(), True)

 	])

run it



A StructType is a collection of StructFields that define the column name, column data type and Boolean value to specify if the fields can be nullable or not





df = spark.createDataFrame(data = data , schema = schema)

df.printSchema()

run it



df.show(truncate=false)

run it







## DataFrame Reader and Writer



The DataFrame Reader is a built in API within the DataFrame that allows you to read various source files such as CSV. JSON, and other Big Data file types such as Parquet, ORC and AVRO



Download the "fire-incident.zip" and place it within the testdata" folder



file\_path = './testdata/fire-incident/fire-incident.csv'

fire\_df = (spark.read.format("csv")

.option("header", true)

.option("inferSchema", True)

.load(file\_path)

run it



fire\_df.select("IncidentNumber", "IncidentDate", "City").show(10)

run it





The "select" statement/function is referred as a projection,

where your project(select) the column that you require and spark will resolver at schema level soon adter the actiin has bee called



fire\_df.printSchema()

runit



fire\_df.columns

runit





till now we seen the how dataframe reader works now lets see how the dataframw writer works



output\_path ='./data/output/fireincidents'

fire\_df.write.format("parquet").mode("overwrite").save(output\_path)

run it



The DataFrame Writer is used to save/write a DataFrame into a file or storage system.

The writer is triggered when you call:

fire\_df.write



Just like read loads data…

write stores data.



Structure of DataFrame Writer

df.write.format("<file-format>").mode("<mode>").option("<key>", "<value>").save("<path>")





format("parquet")



Tells Spark in which format to write the data.



Common formats:



"parquet" (default and best performance)



"csv"



"json"



"orc"



"avro"





mode("overwrite")



Specifies what Spark should do if the output directory already exists.



Modes:



Mode	Meaning

"overwrite"	Replace existing output

"append"	Add new data to directory

"ignore"	Do nothing if path exists

"error" (default)	Throw error if path exists





save(output\_path)



This tells Spark where to write the data:



save(output\_path)





Spark creates:



Output folder



Partition folders (if any)



Data files (Parquet, CSV, etc.)



Example output folder structure:



fireincidents/

    part-0000.parquet

    part-0001.parquet

These are output files generated by Spark when writing a DataFrame.



Each file is:



A chunk of your DataFrame



Written by one Spark task



Stored in Parquet format (since you used .format("parquet"))





## CHALLENGE 1

For this challenge u need a jupyter notebook

see the image to know what is the exact challenge

You are required to prepare data for Sales Analytics.



Create a New Notebook Called: SparkSalesAnalytics



Import the required Spark Libraries that can help you create a schema and assign datatypes



Create A Spark Session



Create A Heading: Data Preparation



Create A Schema based on the following blueprint;





  6. Download salesdata.zip into the data folder, and unzip/extract the contents into the directory path "data/salesdata"



  7. Read the entire contents of the "data/salesdata" as a CSV into a Sales RAW Dataframe



  8.  Print out the first 10 records



  9. Print out the Schema information





## SOLUTION



Lets create new note book called as SparkSalesAnalytics to do that click on file select new select notebook and give name as SparkSalesAnalytics



from pyspark.sql import SparkSession

from pyspark.sql import StructType, StructField, StringType

runit

 

spark = (SparkSession.builder.appName("SalesAnalytics").getOrCreate())

run it



schema = StructType(\[

 	StructField("Order Id", StringType(), True),

 	StructField("Product", StringType(), True),

 	StructField("Quantity Ordered", StringType(), True),

 	StructField("Price Each", StringType(), True),

 	StructField("Order Date", StringType(), True),

 	StructField("Purchase Address", StringType(), True),

 	])

 	runit



Download the "fire-incident.zip" and place it within the testdata" folder





sales\_data\_fetch ="./data/salesdata"

sales\_raw\_df =(spark.read.format("csv")

 		.option("header", true)

 		.schema(schema)

 		.load(sales\_data\_fetch)

run it



sales\_raw\_df.show(10)

run it

sales\_raw\_df.printschema()







## Working with Structure Operations

These structured operations will help us shape our data frames exactly the way we need them to be,and in the real world



Note:

Spark i not designed to work on a single node computer, you are bound to experience weird errors





### Why Spark Gives Block Manager Errors / File Not Found Errors on Windows



Spark is designed for distributed clusters, not for a single laptop.



When running on your machine:



✔ Spark uses your computer’s local disk to store temporary shuffle files



(used when Spark does joins, sorts, groupBy, wide transformations)



❌ Sometimes Spark does NOT delete these temp files properly



This can cause:



BlockManagerError



FileNotFoundError



Executor lost errors



Partitions disappearing during execution



Why?

Because on a single machine, Spark tries to behave like a cluster but cannot fully manage disk cleanup.





### What Spark does in a real cluster



A real cluster has:



Cluster manager (YARN, Kubernetes, Standalone mode)



Multiple executors



Worker nodes with isolated directories



Automatic cleanup tools



But on Windows:

→ Spark pretends your laptop is a cluster

→ So temp files collect and cause errors



#### Typical Errors You See



1️⃣ Block Manager Error

Spark cannot find the block of data on disk because it got deleted or corrupted.



2️⃣ FileNotFoundException

Spark tries to read a temp file from shuffle folder, but it’s missing.



3️⃣ Executor Lost / Task Failed

Because Spark can't locate partition data.



These are all common on Windows.





#### The Solution for these errors

#### ✔ Restart the Spark session

spark.stop()

This kills:



Executors



Shuffle files



BlockManager temp data



Cached memory



#### ✔ Turn heavy code into "raw text"



Why?



Because Jupyter automatically re-executes code cells on session restart.



If your cell loads a big CSV (like fire-incident.csv) immediately after restart → Spark fails again before cleanup.



So they convert code cell into Raw Text, meaning:



→ Jupyter won't run it

→ Spark won't load the heavy file

→ No shuffle files created during restart



#### ✔ Restart the entire notebook (kernel)



This gives a fresh SparkSession, clearing:



All memory



All temporary shuffle files



All DAG artifacts



## Reading a JSON File



Json stands for JavaScript Object Notation and was designed as a way for applications and systems to exchange data between each other.



Json is a lightweight, text based file format that is very human readable and has the ability to represent data in a nested format, unlike a typical csv file.

Why JSON is “nested” formats?



Because a JSON object can contain:



Strings



Numbers



Arrays



Objects inside objects



This is why JSON is more flexible than CSV.



So let's start by creating two headings, and to do that, I will start with an H2 heading with a double hash symbol



in jupyer notebook type

\##Working with Structured Operation 

run it

\### Reading a JSON File

run it



from pyspark.sql import ArrayType, FloatType, DataType, BooleanType

run it



persond\_schema = StructType(\[

&nbsp;	StructField("id", IntegerType(), True),

 	StructField("firstname", StringType(), True),

 	StructField("lastname", StringType(), True),

 	StructField("fav\_movie", StringType(), True),

 	StructField("Salary", FloatType(), True),

 	StructField("image\_url", StringType(), True),

 	StructField("DOB", Date(), True),

 	StructField("active", BooleanType(), True),

])



Download the person.json file from the resources

now in the jupyter note book on the folders side

create a new folder called persons

now navigate the downloaded person.json inside the persons folder



json\_files\_path ='./data/persons/persons.json'

person\_df=(spark.read.json(json\_files\_path,person\_schema,multiLine="True")

run it

persons\_df.printSchema()

runit

person\_df.show(10)

run it

person\_df.show(10, truncate= False)

run it





#### Columns and Expressions



Spark has a number of methods or perhaps functions to support a wide variety of operations on data frame columns.



So let's explore columns and expressions together.





Now in the same jupyter notebook 

type 

\##Columns and Expressions

and save it as markdown



from pyspark.sql import col, expr

run it



person\_df.select(col("firstname"), col("lastname"), col("DOB")).show(5)

run it



person\_df.select(exp("firstname"), exp("lastname"), exp("DOB")).show(5)

run it



now both results will same for col and exp lets see the difference



Note:

pyspark.sql function provides two functions concat() and concat\_ws() to concatenate DataFrame multiple columns into a single columns



from pyspark.sql import concat\_ws

run it

(person\_df.select(concat\_ws(' ',col("firstname"),col("lastname").alias("full\_name"),

col("salary"),

(col("salary")\*0.10 + col("salary")).alias("salary\_increases"))).show(10)

run it



from pyspark.sql import concat\_ws

run it

(person\_df.select(concat\_ws(' ',col("firstname"),col("lastname").alias("full\_name"),

col("salary"),

exp("salary \*0.10 + salary").alias("salary\_increases"))).show(10)

run it



again we will get same result for both col and exp



The difference between the col and exp is

that you can assign a string expression



within the expression function to perform explicit calculations.



While you cannot do the same, whether the col function.





#### Filter and Where condition

lets learn to filter data usinf the "FILTER, WHERE" functions.FYI these are built in DataFrame API Functionss

Such as "SELECT, ORDER BY, GROUP BY, FILTER, WHERE"





filter() and where() — BOTH DO THE SAME THING



In Spark:



df.filter(condition)

df.where(condition)





are identical.



Both take a Boolean expression and return rows where the condition is TRUE.



Example:



df.filter(df.salary <= 3000)

df.where(df.salary <= 3000)





Both return the same rows.



🔥 2️⃣ Selecting rows with filter



Example from the lesson:



person\_df.filter(persons\_df.salary <= 3000).show(10)

run it



What this means:



For each row, Spark checks salary <= 3000



If true → keep row



If false → drop row



Output shows rows that match the condition.

person\_df.filter(persons\_df.salary <= 3000).show(10)

run it





Note:

Even if you don’t use select(), Spark automatically selects all columns by default when using filter() or where().



Combining Conditions



Spark uses Python logical operators:



✔ AND → \&

✔ OR → |

✔ NOT → ~





Example of AND

Filter rows where:



Salary <= 3000



AND active == true



person\_df.filter((col("salary") <= 3000) \& (col("active") == True)).show(10)

run it



Example of OR



Birth year is either 2000 or 1989.



First, you need:



from pyspark.sql.functions import year

run it

person\_df.filter((year(col("DOB")) == 2000) |

&nbsp;(year(col("DOB")) == 1989)).show()

run it



from pyspark.sql.functions import \*

run it



person\_df.filter(year("DOB") == 2000 | (year("DOB") == 1989)).show()

run it



from pyspark.sql.functions import array\_contains

run it



person\_df.where(array\_contains(person\_df.fav\_movies,"Land of the Lost")).show()

run it





### Distinct Drop Duplicates Order By



1\. distinct() → Removes Duplicate ROWS Completely



distinct() returns unique rows based on all selected columns.



Example from transcript:



from pyspark.sql.function import count,desc

run it



person\_df.select("active").show(10)

run it





person\_df.select("active").distinct().show()

runn it



(person\_df.select(col("firstname"), year(col("DOB")).alias("year"),col("active")).orderBy("year", "firstname")).show(10)

run it



dropped\_df = (person\_df.select(col("firstname") year(col)("DOB")).alias("year"),col("active")).dropDuplicates(\["year", "active"])).orderBy("year","firstname")

run it

dropped\_df.show()

run it





(person\_df.select(col("firstname"), year(col("DOB")).alias("year"),col("active")).orderBy("year", ascending=False)).show(10)

run it



#### Rows and Union



1\. What is a Row() in Spark?



Spark’s Row object represents one record in a DataFrame.





from pyspark.sql import Row

run it



person\_row = Row(

&nbsp;   101, 

&nbsp;   "Robert", 

&nbsp;   "Owens",

&nbsp;   \["Men in Black 3", "Home Alone"],

&nbsp;   4364.0,

&nbsp;   "http://someimage.com",

&nbsp;   "1964-08-18",

&nbsp;   True

)

runit



**Create a List of Rows**



**A DataFrame cannot be created from a single Row.**

**So we create a list of Rows.**



**persons\_rows\_list = \[**

    **Row(102, "Kenny", "Poppin", \["Men in black III","Home Alone"], 4500.64,** "http://someimage.com","1964-08-18",True**),**

    **Row(103, "Sara", "Diven","Men in black III","Home Alone"], 4500.64,** "http://someimage.com","1964-08-18",True**)**

**]**

**run it**



**Then add the first Row:**



**persons\_rows\_list.append(person\_row)**

**runit**



**person\_row\[1]**

**runn it**





**new\_persons\_df = spark.createDataFrame(**

    **persons\_rows\_list,**

    **\["id", "first\_name", "last\_name", "favorite\_movies", "salary", "image\_url", "date\_of\_birth", "active"]**

**)**

run it



all\_persons\_df = persons\_df.union(new\_persons\_df)

all\_person\_df.sort(desc("id")).show(10)

run it



#### Adding, Renaming and Dropping Columns



1\. Adding New Columns — withColumn()



You can use withColumn() to:



Create a new column

Modify an existing column



In the existing jupyter notebook



from pyspark.sql.function import round

run it



augmented\_persons\_df1 = persons\_df.withColumn(

&nbsp;   "salary\_increase", exp("salary\*0.10 + salary"))

run it



augmented\_persons\_df1.columns

run it



Adding More Columns + Renaming Columns



augmented\_persons\_df2 = (augmented\_persons\_df1

&nbsp;   .withColumn("birth\_year", year(col("date\_of\_birth"))) 

&nbsp;   .withColumnRenamed("favorite\_movies", "movies") 

&nbsp;  .withColumn("salary\_x10", round(col("salary\_increase"),2))

&nbsp;   .drop("salary\_increase"))

run it



augmented\_persons\_df2.show(10)







#### Working with Missing or Bad Data

you can copy the code from bad movies text attached to this particular lesson and then paste it within the next cell.



1\. Creating a DataFrame with Missing Values

bad\_movies\_list = \[

&nbsp;   (None, "Avatar", 2009),

&nbsp;   ("Titanic", None, 1997),

&nbsp;   (None, None, None),

&nbsp;   ("Jurassic Park", "Dinosaur Movie", 1993)

]

run it



bad\_movies\_list

run it



bad\_movies\_columns = \["name", "movie\_title", "produced\_year"]

run it





bad\_movies\_df = spark.createDataFrame(bad\_movies\_list, bad\_movies\_columns)

run it





Some rows contain None (null).





2\. Dropping Rows with Null Values — na.drop()



Spark provides a DataFrame.NA API to clean missing data:



✔ drop() removes rows that contain null based on your rules.

2.1 Drop ANY row that contains a null



bad\_movies\_df.na.drop().show()

run it

OR

bad\_movies\_df.na.drop("any").show()

run it



2.2 Drop rows ONLY if ALL columns are null

bad\_movies\_df.na.drop("all")





Meaning:



Only drop the row if every column is null.







3\. Filtering on Null Columns



Sometimes you don’t want to drop rows.

You want to filter them based on null or not null.



3.1 Keep rows where a column is NOT null

bad\_movies\_df.filter(col("name").isNotNull()).show()





This removes rows where name = null.



3.2 Keep rows where a column IS null

bad\_movies\_df.filter(col("name").isNull()).show()





This keeps only rows missing the name.





#### Working with User Defined Functions



You cannot perform data transformations on the data frame using the Python code.



In fact, you cannot even call an ordinary python function.



And the only way you can manipulate the data frame is using Spark as User defined functions.



However, there are instances where you cannot find the appropriate function that you need and you would rather create your own function.



Let's see how you can create your own spark function, 





from pyspark.sql.functions import udf

run it



students\_list =\[("Joe",85),("Jane",85),("Mary",85),("Nani",85)]



student\_columns=\["name","score"]

run it



Students\_df = spark.createDataFrame(student\_list,schema=student\_columns)

run it

Students\_df.show()

run it





1\. Creating a Simple Python Function



First, you created a normal Python function:



def letter\_grade(score: int):

&nbsp;   if score > 100:

&nbsp;       grade = "Cheating"

&nbsp;   elif score >= 90:

&nbsp;       grade = "A"

&nbsp;   elif score >= 80:

&nbsp;       grade = "B"

&nbsp;   elif score >= 70:

&nbsp;       grade = "C"

&nbsp;   else:

&nbsp;       grade = "F"

&nbsp;   return grade

run it



print(letter\_grade(75)) 

run it



letter\_grade\_udf = udf(letter\_grade)

run it



students\_df.select("name","score" ,letter\_grade\_udf(col("score")).alias("grade")).show()

run it





#### Aggregations





What Are Aggregations?



Aggregation = summarizing data.



Examples:



Total sales per year



Number of orders per month



Highest salary per department



Average quantity sold per product



Spark gives built-in aggregation functions such as:



sum()



avg()



max()



min()



count()



countDistinct()



first(), last()



These operate on groups of data created by groupBy.



🔥 Why Aggregations Need Grouping?



Because Spark must know how to summarize.



Example:



Year	Price

2020	200000

2020	110000

2001	65000



If you want total car stock value per year, Spark must:



👉 Group rows by Year

👉 Apply aggregation on the price column



🔧 Example from the instructor

Dataset:

Year	Car	Price

2020	BMW	200000

2020	Jeep	110000

2001	Audi	65000





1️⃣ SUM Aggregation



"Stock value per year"



df.groupBy("Year").sum("Price").show()





Output:



+-----+-----------+

|Year | sum(Price)|

+-----+-----------+

|2020 | 310000    |

|2001 | 65000     |

+-----+-----------+



2️⃣ MAX Aggregation



"Highest car price per year"



df.groupBy("Year").max("Price").show()





Output:



+-----+-----------+

|Year | max(Price)|

+-----+-----------+

|2020 | 200000    |

|2001 | 65000     |

+-----+-----------+











