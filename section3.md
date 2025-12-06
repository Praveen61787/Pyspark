# RDD

Refer the image



Resilient Distributed Datasets (RDDs) are a fundamental data structure in Apache Spark. They represent a fault-tolerant collection of elements that can be operated on in parallel across a cluster. RDDs are immutable, meaning once created, they cannot be changed. Instead, transformations on RDDs produce new RDDs.

RDD is an immutable and resilient distributed collection of elements of your data, this is partitioned across nodes in your cluster.



Creating RDDs



There are two primary ways to create RDDs:



Parallelizing an existing collection: This involves using the parallelize method on an existing collection in the driver program.



data = \[1, 2, 3, 4, 5]

distData = sc.parallelize(data)



Referencing a dataset in an external storage system: This can be done using methods like textFile to read data from sources like HDFS, S3, or local file systems.



distFile = sc.textFile("data.txt")



RDD Operations



Transformations

Actions





Why learn Rdds?

1.You want low level transformation and actions to control your datasets

2.You want to handle unstructured data that cannot be handled by structured API'S such as Dataframe and DataSets.

3.You want to optimize your spark application using a low level API





## Unpack Rdds

The difference between the RDDS and DataFrame is that the RDDS let you manipulate RAW Java objects, and with Dataframes you manipulate Spark Types





1.Open cmd type cd.. and create new directory

2.mkdir testrdds

3.cd testredds.

4.jupyter notebook

5.After the jupyter notebook opened click on new select python





from pyspark.sql import SparkSession

spark = (SparkSession.builder.appName("TestingRDDS").getOrCreate()) and run it





Rdd transformations are lazy operations this means none of transformers are executed until actions are executed.





words\_list = "Spark makes life easier,Spark is one of the library".split(" ")

run it

type(words\_list)

run it

print(words\_list)

run it



//creation of rdd

words\_rdd = spark.sparkContext.parallelize(words\_list)

//To store the list in another variable

words\_data = words\_rdd.collect()

run it



for word in words\_data:

 	print(word)

run it

Save the notebook as Testing Rdd



## Transformations



Spark has many transformations



### 1.distinct()

distinct() transformation will allow us to remove the duplicates from the RDD

before using distinct() lets use count() to count no of words in the list



words\_rdd.count()

run it





now

words\_rdd.distinct().count()

run it



Now check whether only duplicates are removed or not

words\_data = words\_rdd.collect()

for word in words\_dta:

 	print(word)

run it

when u run it u can see that the duplicate words has not removed at all.



Because the transformations always result new rdd not physically manipulate the current RDD making the RDD immutable



now if we want to see the list without duplicates store that list in another variable



words\_rdd\_unique = words\_rdd.distinct()

run it

for words in words\_rdd\_unique:

 	print(words)

run it



### 2.filter()

 to understand the filter() lets define a function

def wordStartWith(word, letter):

 	return word.startswith(letter)

run it



words\_rdd.filter(lambda word: wordStartWith(word,"S")).collect()

run it



Note:

When filtering records you are most likely to use anonymous function which are referred as Lambda Function





### 3.Map()

In PySpark, the map() function is used on an RDD (Resilient Distributed Dataset) to apply a transformation to each element of the RDD.

It returns a new RDD, just like Python's map, but distributed.



Syntax

new\_rdd = old\_rdd.map(lambda x: <expression>)





Map transformation is used to apply any complex operations like adding a column or perhaps updating another column or just to transform data





The output of the map transformations would always have of the same number of records as input



num\_list = \[\*range(1,21)]

print(num\_list)

run it



nums\_list = \[\*range(1, 21)] creates a list of numbers from 1 to 20 by unpacking the range() into a list.



nums\_rdd = spark.sparkContext.parallelize(nums\_list)

run it



nums\_squared\_rdd = nums\_rdd.map(lambda n:(n,n\*n))

run it

for ele in nums\_squared\_rdd.collect():

 	print(ele)





example 2:

Lets create an RDD to store the transformed data

words\_trd\_rdd = words\_rdd.map(lambda : word (word , word\[0], wordStartWith(word,"s")))

run it



for element in words\_trd\_rdd:

 	print(element)

run it





### 4.flatmap()

flatmap is just simple extension of the map function



flatMap() in PySpark is like map(), but with flattening.

It allows each input element to be mapped to 0, 1, or many output elements.



Simple Definition

map → 1 input → 1 output

flatMap → 1 input → multiple outputs (then flat/merged)





words\_rdd.flatMap(lambda word: list(word).take(10))





### 5.SortByKey()

sortByKey() is a PySpark RDD transformation used to sort Key–Value RDDs by their keys.

What does sortByKey() do?



If you have an RDD like:



\[(key, value), (key, value), ...]





sortByKey() will sort the RDD based only on the key, not the value.



Syntax

sorted\_rdd = rdd.sortByKey(ascending=True)



ascending=True → sorts in increasing order



ascending=False → sorts in decreasing order



sortByKey() transformation needs key value pair for that lets use a list of tuples





countries\_list=\[("India",91),("USA",4),("RSA",23),("KIWI",5)]

countries\_rdd = spark.sparkContext.parallelize(countries\_list)

run it



srtd\_countries\_list = countries\_rdd.sortByKey().collect()

run it



for country in srtd\_countries\_list:

 	print(country)

run it

now we will the output in ascending order based on the country name

what if you want to arrange based on the rank of the country



srtd\_countries\_list = countries\_rdd.map(lambda c:( c\[1], c\[0]).sortByKey(False).collect()

run it



for country in srtd\_countries\_list:

 	print(country)

run it









## Actions

In PySpark, Actions are operations that trigger execution of the RDD/DataFrame pipeline and return a result (or write output).



⚠️ Transformations are lazy → they do nothing until an Action is called.



What is an Action?



An Action:



Forces Spark to execute all previous transformations



Returns a value to the driver OR writes data to storage





As we already know without action execution we cant execute the transformations.

action functions are like count() collect()....etc



### 1.reduce()

reduce() is a PySpark Action that combines all elements of an RDD into one single value using a function you provide.

In simple words we will aggregate the values in to a single value



num\_list=\[1,5,3,4]

run it



result = spark.sparkContext.parallelize(num\_list).reduce(lambda x,y:x+y)

print(result)

run it





To simplify the above code lets define a function to add two values



def sumList(x,y):

 	print(x,y)

 	return x+y

run it



result = spark.sparkContext.parallelize(num\_list).reduce(lambda x,y:sumList(x,y)

print(result)

run it





Example 2:



We can us the reduced function o find the longest word in a set of words



def wordLengthReducer(leftword,rightword):

 	if len(leftword) > len(rightword):

 		return leftword

 	else:

 		return rightword

run it



words\_rdd.reduce(wordLengthReducer)

run it



To get the first word in the word\_rdd

words\_rdd.first()

run it



Example 3

Lets find the max and min values in the list



spark.sparkContext.parallelize(range(1,21)).max()

run it



spark.sparkContext.parallelize(range(1,21)).min()





min() and max() are the action functions



## Challenge 1



now open new jupyter notebook



now to add thr header select markdown and type

\#Convert fahrenheit to degrees



from pysprak.sql import SparkSession

spark = (SparkSession.builder.appName("RDDChallenge").getOrCreate())



tempList = \[59,57.2,53.6,55.4,51.8,53.6,55.4]

tempRdd = spark.sparkContext.parallelize(tempList)

tempRdd.collect()

run it



def FahrenheitToCentigrade(temperature)

&nbsp;	 centigrade = (temperature -32 )\*5 /9

&nbsp;	 return centigrade

run it



FahrenheitToCentigrade(57)

run it



tempFahRDD = tempRdd.map(FahrenheitToCentigrade)

tempFahRDD.collect()

run it





filterTempRDD = tempFahRDD.filter(lambda x:x>=13)

filterTempRDD.collect()







## Challenge 2



Before dng this challenge download the resource from the Udemy 

use the same notebook



create new heading 

\#XYZ Research



data2001List = \['RIN1', 'RIN2', 'RIN3', 'RIN4', 'RIN5', 'RIN6', 'RIN7']



data2002List = \['RIN3', 'RIN4', 'RIN7', 'RIN8', 'RIN9']



data2003List = \['RIN4', 'RIN8', 'RIN10', 'RIN11', 'RIN12']

run it



data2001RDD = spark.sparkContext.parallelize(data2001List)

data2002RDD = spark.sparkContext.parallelize(data2002List)

data2003RDD = spark.sparkContext.parallelize(data2003List)

&nbsp;run it



unionof20012002RDD = data200RDD.union(data2002RDD)

unionof20012002RDD.collect()

run it



allResearchRDD = unionof20012002RDD.union(data2003RDD)

allResearchRDD.collect()

run it



allResearchRDD = unionof20012002RDD.union(data2003RDD).distinct()

allResearchRDD.collect()

run it 



allResearchRDD.count()

run it





//second question





firstyearcompletionRDD = data2001RDD.subtract(data2002RDD)

firstyearcompletionRDD.collect()

run it



uniontwoyearsrdd = data2001RDD.union(data2002RDD)

uniontwoyearsrdd.collect()

run it





uniontwoyearsrdd.subtract(data2003RDD).distinct().collect()



