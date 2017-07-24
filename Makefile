#Change the hadoop.root to you Hadoop Root folder
# Add input files to the input folder

hadoop.root=/Users/Naomi/Downloads/hadoop-2.7.3

# Compiles code and builds jar.
jar:
	(cd PageRank && ./gradlew build)

# Runs standalone
alone:	jar
	rm -rf output*
	${hadoop.root}/bin/hadoop jar PageRank/build/libs/PageRank-1.0-SNAPSHOT.jar input/ output/output
	
# Removes local output directory and jars 
clean:
	rm -rf output*
	
	

