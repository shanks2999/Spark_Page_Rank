# README #

# Spark Page Rank #


**Description:**  
This is a PageRank implementation using Spark Computation model for parallel processing of the [publically available DBLP dataset](https://dblp.uni-trier.de). XML parsing developed in Scala and using Spark RDD's which setup locally and tested on Hortonworks VM as well as deployed on AWS EMR.   

**Overview**
Each entry in the dataset describes a publication, which contains the list of authors, the title, and the publication venue and a few other attributes. The file is approximately 2.5Gb - not big by today's standards, but large enough for this homework assignment. Each entry is independent from the other one in that it can be processed without synchronizing with processing some other entries.

Consider the following entry in the dataset.
```xml
<inproceedings mdate="2017-05-24" key="conf/icst/GrechanikHB13">
<author>Mark Grechanik</author>
<author>B. M. Mainul Hossain</author>
<author>Ugo Buy</author>
<title>Testing Database-Centric Applications for Causes of Database Deadlocks.</title>
<pages>174-183</pages>
<year>2013</year>
<booktitle>ICST</booktitle>
<ee>https://doi.org/10.1109/ICST.2013.19</ee>
<ee>http://doi.ieeecomputersociety.org/10.1109/ICST.2013.19</ee>
<crossref>conf/icst/2013</crossref>
<url>db/conf/icst/icst2013.html#GrechanikHB13</url>
</inproceedings>
```

This entry lists a paper at the IEEE International Conference on Software Testing, Verification and Validation (ICST) published in 2013 whose authors are my former Ph.D. student at UIC, now tenured Associate Professor at the University of Dhaka, Dr. B.M. Mainul Hussain whose advisor Mark Grechanik is a co-author on this paper. The third co-author is Prof.Ugo Buy, a faculty member at our CS department. The presence of two authors, Mark Grechanik and Ugo Buy in a single publication like this one establishes a connection between these faculty members. Depending on the importance of these nodes, ICST may have the highest pagerank value followed by... well, it is up to Spark to find out!



**Project Structure:** 

- **input**: Contains the test.xml which is used in testing the code
    
- **src**: 

    - It contains  main class "PageRank.scala" which is the implementation using Spark
    - "XMLInputFormat.java". A helper class which is used to extract tags from the large xml in chunks.
    - "Test.scala". A scala code to test the basic functionality of the Code.
    
    
**Explain/Notes:**

 - Compiled Jar is placed in the Jar folder in root.
 - You can create it manually using " sbt clean compile package "
 - You can check tests with " sbt clean compile test " 
  
  
**Execution:**

 - The project can be executed either via IntelliJ or SBT locally and deployed on VM using the Jars Provided in Jars folder.
 - Parameters expected are the input and output folder
 - To run locally uncomment line 139 in 'PageRank' class which sets the system property for Spark to run locally.
 
 
 **Steps to deploy onto AWS EMR**
 
 - Youtube Link:  https://youtu.be/e_NwYyJw97g
 

That's It ! :) 
