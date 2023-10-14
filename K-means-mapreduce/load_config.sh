#load the config.xml in the hdfs
scp config.xml hadoop@10.1.1.53:~

#remove the old configuration file from the hdfs
ssh hadoop@10.1.1.53 "/opt/hadoop/bin/hadoop fs -rm config.xml"

#put the new configuration file in the hdfs
ssh hadoop@10.1.1.53 "/opt/hadoop/bin/hadoop fs -put config.xml config.xml"
