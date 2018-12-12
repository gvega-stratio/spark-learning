Follow this steps to run the script:
mvn clean package
spark-submit --class com.spark.example.Examples target/rdd-examples-1.0-SNAPSHOT.jar

($SPARK_HOME and $JAVA_HOME should be set, and $SPARK_HOME/bin on $PATH)
