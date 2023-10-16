# Retrieve the output from Hadoop
ssh hadoop@10.1.1.53 "/opt/hadoop/bin/hadoop fs -get /user/hadoop/output/ output"

# Copy the output to the local data directory with the test label
scp -r hadoop@10.1.1.53:~/output ../scripts/output/test/
# remove the output file from the namenode
ssh hadoop@10.1.1.53 "rm -r output/"
