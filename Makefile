.ONESHELL:
dir = /Users/fferegrino/Documents/GitHub/MrPageRank/wiki
input_file = /Users/fferegrino/Downloads/enwiki-20080103-sample.txt
output_folder = /Users/fferegrino/Downloads/enwiki
hadoop_path = /Users/fferegrino/hadoop-3.0.0
run: clean package
	export HADOOP_CLASSPATH=$(dir)/target/uog-bigdata-0.0.1-SNAPSHOT.jar
	$(hadoop_path)/bin/hadoop mapreduce.WikiPageRank $(input_file) $(output_folder)
package:
	mvn -f $(dir)/pom.xml package
clean:
	rm -rf $(output_folder)
	mvn -f $(dir)/pom.xml clean