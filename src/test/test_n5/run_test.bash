# build the java files with maven
cd n5_java
mvn package

# generate the test data for z5
java -Xmx3g -cp target/n5_java-1.0-SNAPSHOT.jar zarr_pp.n5.App 
cd ..

# check the generated data and generate testdata for n5
./test_n5

# check the z5 data
cd n5_java
java -Xmx3g -cp target/n5_java-1.0-SNAPSHOT.jar zarr_pp.n5.App2

# clean up
cd ..
rm -r n5_test_data n5_test_data_out
