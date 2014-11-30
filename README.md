Avro-MapReduce
==============

Reference : http://java.dzone.com/articles/mapreduce-avro-data-files#viewSource

----------------------------------
Settings.xml to include Hadoop Class paths inside Maven

----------------------------------Data Preparation------------------------------

1.student.avsc

{
  "type" : "record",
  "name" : "student_marks",
  "namespace" : "com.rishav.avro",
  "fields" : [ {
  "name" : "student_id",
  "type" : "int"
  }, {
  "name" : "subject_id",
  "type" : "int"
  }, {
  "name" : "marks",
  "type" : "int"
  } ]
}

2. Student.json
{"student_id":1,"subject_id":63,"marks":19}
{"student_id":2,"subject_id":64,"marks":74}
{"student_id":3,"subject_id":10,"marks":94}
{"student_id":4,"subject_id":79,"marks":27}
{"student_id":1,"subject_id":52,"marks":95}
{"student_id":2,"subject_id":34,"marks":16}
{"student_id":3,"subject_id":81,"marks":17}
{"student_id":4,"subject_id":60,"marks":52}
{"student_id":1,"subject_id":11,"marks":66}
{"student_id":2,"subject_id":84,"marks":39}
{"student_id":3,"subject_id":24,"marks":39}
{"student_id":4,"subject_id":16,"marks":0}
{"student_id":1,"subject_id":65,"marks":75}
{"student_id":2,"subject_id":5,"marks":52}
{"student_id":3,"subject_id":86,"marks":50}
{"student_id":4,"subject_id":55,"marks":42}
{"student_id":1,"subject_id":30,"marks":21}


--------Data Preparation from Avro Tool ------------------------------
AvroTools folder contain jar and procedure to generate AvroFile from Json file

Command to Generate Avro from Json

C:\Asmath\HadoopLuno\AvroTool>java -jar avro-tools-1.7.7.jar fromjson student.js
on --schema-file student.avsc > student.avro

Download avro-tools-1.7.7.jar from http://mirror.nexcess.net/apache/avro/stable/java/

Dont get confused with format in JSON.. You need to give fieldname and value for JSON like subject_id:1

--------------------------------TOOLS ----------------------------------------

I have included AvroTools folder in project path so it can directly used in subsequent projects



---------------------------------- Explanation --------------------------------

In the program the input key to mapper is AvroKey<student_marks> and the input value is null. 
The output key of map method is student_id and output value is an IntPair having marks and 1.

We have a combiner also which aggregates partial sums for each student_id.

Finally reducer takes student_id and partial sums and counts and uses them to calculate average for each student_id. The reducer writes the output in Avro format.
For Avro job setup we have added these properties:

WritableComparable interface is just a subinterface of the Writable and java.lang.Comparable interfaces. For implementing a WritableComparable we must have compareTo method apart from readFields and write methods, as shown below:
public interface WritableComparable extends Writable, Comparable
{
    void readFields(DataInput in);
    void write(DataOutput out);
    int compareTo(WritableComparable o)
}
Comparison of types is crucial for MapReduce, where there is a sorting phase during which keys are compared with one another.

The code for IntPair class which is used in In-mapper Combiner Program to Calculate Average post is given below

---------------------------------- Coding -------------------------------------


1. POM.xml

Include jars for avro-mapred and paranamer in POM.xml 
Also Include jar for avro and cloudera libraries in POM.xml

2. Generate class from Student.avsc by running maven goal Generate-Sources. Class file is generated. Ignore the error with the generate-sources tag in pom.xml

3. Once the class is generated, create custom writable class

4. Snappy conversion --- requires maven plugin 
		   * 
		   * <dependency>
      <groupId>org.xerial.snappy</groupId>
      <artifactId>snappy-java</artifactId>
    </dependency>
    <dependency>
      <groupId>org.apache.commons</groupId>
      <artifactId>commons-compress</artifactId>
    </dependency>


-----------------------------------Execution-----------------------------------
rum mvn install 
**** maven goal install is important step to generate java classes from the java source files

From Eclipse: Add Argements as Input/student.avro Output to public static void main

From Hadoop Cluster:

export LIBJARS=avro-1.7.5.jar,avro-mapred-1.7.5-hadoop1.jar,paranamer-2.6.jar
export HADOOP_CLASSPATH=avro-1.7.5.jar:avro-mapred-1.7.5-hadoop1.jar:paranamer-2.6.jar
hadoop jar avro_mr.jar com.rishav.avro.mapreduce.AvroAverageDriver -libjars ${LIBJARS} student.avro output




##################################### Issues while Executing ###################################################

IncompatibleClassChangeError AvroKeyInputFormat.class

Solution :http://stackoverflow.com/questions/24756078/avro-mapreduce-job-failing-java-lang-incompatibleclasschangeerror

Simple solution - avro need to be compiled against hadoop 2 so added qulifier   <classifier>hadoop2</classifier>

<dependency>
    <groupId>org.apache.avro</groupId>
    <artifactId>avro-mapred</artifactId>
    <version>1.7.4</version>
    <classifier>hadoop2</classifier>
</dependency>

############################################





 
