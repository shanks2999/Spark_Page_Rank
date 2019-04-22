# README #

# Cloud CS441 - Homework 5 #


**Description:**  
This is a PageRank implementation using Spark Computation model for parallel processing. XML parsing developed in Scala and using Spark RDD's which setup locally and tested on Hortonworks VM as well as deployed on AWS EMR.   


**Project Structure:** 

- **input**: Contains the test.xml which is used in testing the code
    
- **src**: 

    - It contains 1 main class called PageRank which is the implementation
    - 1 XMLInputFormat helper class which is used to extract tags from the large xml in chunks
    - 1 test file which basic functionality of the Code.
    
    
**Explain/Notes:**

 - Compiled Jar is placed in the Jar folder in root.
 - You can create it using " sbt clean compile package "
 - You can check tests with " sbt clean compile test " 
  
**Execution:**

 - The project can be executed either via IntelliJ or SBT locally and deployed on VM using the Jars Provided in Jars folder.
 - Parameters expected are the input and output folder
 - To run locally uncomment line 139 in 'PageRank' class which sets the system property for Spark to run locally.
 
 
 **Bonus:**
 
 - Youtube Link:  https://youtu.be/e_NwYyJw97g
 

 PS: Most setup was already reused from 2nd homework. Only hurdle was implementing Scala's Map/Reduce/Groupby implementation. 
 
That's It ! :) 