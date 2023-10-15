#!/bin/bash

# Check for the correct number of arguments
if [ "$#" -ne 3 ]; then
    echo "Usage: <n> <d> <k>"
    exit 1
fi

# Assign command line arguments to variables
n="$1"
d="$2"
k="$3"

# Run the Hadoop MapReduce job with the provided values
ssh hadoop@10.1.1.53 "/opt/hadoop/bin/hadoop jar K-means-mapreduce-1.0-SNAPSHOT.jar it.unipi.dii.hadoop.Application datasets/n_${n}_d_${d}_k_${k}.txt output"

# Retrieve the output from Hadoop
ssh hadoop@10.1.1.53 "/opt/hadoop/bin/hadoop fs -get /user/hadoop/output/part-r-00000 output"

# Copy the output to the local data directory with the test label
scp hadoop@10.1.1.53:~/output ../scripts/output/n_${n}_d_${d}_k_${k}/part-r-00000
# remove the output file from the namenode
ssh hadoop@10.1.1.53 "rm output"

#copy the log file to the results local directory
scp hadoop@10.1.1.53:~/k-means-log.txt ../scripts/output/n_${n}_d_${d}_k_${k}/k-means-log.txt
#remove the log file from the namenode
ssh hadoop@10.1.1.53 "rm k-means-log.txt"
