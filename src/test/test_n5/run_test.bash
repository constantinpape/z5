cd n5_java
mvn package
java -Xmx3g -cp target/n5_java-1.0-SNAPSHOT.jar zarr_pp.n5.App 
cd ..
#./test_n5
