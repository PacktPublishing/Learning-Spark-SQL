# Learning Spark SQL

This is the code repository for [Learning Spark SQL](https://www.packtpub.com/big-data-and-business-intelligence/learning-spark-sql?utm_source=github&utm_medium=repository&utm_campaign=9781785888359), published by [Packt](https://www.packtpub.com/?utm_source=github). It contains all the supporting project files necessary to work through the book from start to finish.

## About the Book

In the past year, Apache Spark has been increasingly adopted for the development of distributed applications. Spark SQL APIs provide an optimized interface that helps developers build such applications quickly and easily. However, designing web-scale production applications using Spark SQL APIs can be a complex task. Hence, understanding the design and implementation best practices before you start your project will help you avoid these problems.

This book gives an insight into the engineering practices used to design and build real-world, Spark-based applications. The book's hands-on examples will give you the required confidence to work on any future projects you encounter in Spark SQL.

It starts by familiarizing you with data exploration and data munging tasks using Spark SQL and Scala. Extensive code examples will help you understand the methods used to implement typical use-cases for various types of applications. You will get a walkthrough of the key concepts and terms that are common to streaming, machine learning, and graph applications. You will also learn how such systems are architected and deployed for a successful delivery of your project. Finally, you will move on to performance tuning, where you will learn practical tips and tricks to resolve performance issues.

## Instructions and Navigation

All of the code is organized into folders. Each folder starts with a number followed by the application name. For example, Chapter02.



The code will look like the following:
```
scala> val inDiaDataDF = spark.read.option("header", true).csv("file:///Users/aurobindosarkar/Downloads/dataset_diabetes/diabetic_data.csv").cache()
```



## Related Products

* [Getting Started with NoSQL](https://www.packtpub.com/big-data-and-business-intelligence/getting-started-nosql?utm_source=github&utm_medium=repository&utm_campaign=9781849694988)

* [Learning PySpark](https://www.packtpub.com/big-data-and-business-intelligence/learning-pyspark?utm_source=github&utm_medium=repository&utm_campaign=9781786463708)

* [PostgreSQL Development Essentials](https://www.packtpub.com/big-data-and-business-intelligence/postgresql-development-essentials?utm_source=github&utm_medium=repository&utm_campaign=9781783989003)

### Suggestions and Feedback
[Click here](https://docs.google.com/forms/d/e/1FAIpQLSe5qwunkGf6PUvzPirPDtuy1Du5Rlzew23UBp2S-P3wB-GcwQ/viewform) if you have any feedback or suggestions.

