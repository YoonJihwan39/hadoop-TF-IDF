# Hadoop-TF-IDF

This program computes TF-IDF.

This program is based on WordCount example code.

The formula for calculating TF-IDF is as follows:

TF(t,d) = log (f(t,d) + 1)

IDF(t,D)=log⁡ (|D|/(1+|{d ∈D∶t ∈d}|))

For more information about expressions, please see: https://en.wikipedia.org/wiki/Tf%E2%80%93idf


How to use

Compile the java file as follows:

$ bin/hadoop com.sun.tools.javac.Main TF-IDF.java
$ jar cf TFIDF.jar TF-IDF*.class

Run the program as follows:

$ bin/hadoop jar TFIDF.jar TFIDF input output

