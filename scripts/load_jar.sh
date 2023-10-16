# Move to the project directory
cd ../K-means-mapreduce/

# Build and package the Java application
mvn clean package

# Copy the JAR file to the remote Hadoop server
scp target/K-means-mapreduce-1.0-SNAPSHOT.jar hadoop@10.1.1.53:~
