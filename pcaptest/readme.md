usage:

./spark-submit   
--class pcaptest2      
--master yarn   
--deploy-mode cluster   
--executor-memory 20G  
--driver-memory 10G  
--num-executors 2 
--executor-cores 30  
/home/baijb/guoPai/CLItest_jar/pcap_scala2.jar \ 
"'hdfs:///user/hive/warehouse/src/line1_1119.seq'" 
"/home/baijb/guoPai/file_cluster.pcap" 
"SELECT pcapByte FROM src where src regexp '180.158.148.194' or dst regexp '180.158.148.194'" 
1   
