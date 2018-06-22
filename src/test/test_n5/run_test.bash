# build the java files with maven
cd n5_java
mvn clean package

# generate the test data for z5
echo "Run Java data generation test!"
java -Xmx6g -cp target/n5_java-2.0.3-SNAPSHOT.jar zarr_pp.n5.App 
cd ..

# check the generated data and generate testdata for n5
echo "Run N5 c++ test!"
./test_n5

# check the z5 data
cd n5_java
echo "Run Java readout test!"
java -Xmx3g -cp target/n5_java-2.0.3-SNAPSHOT.jar zarr_pp.n5.App2

# clean up
cd ..
rm -r n5_test_data n5_test_data_out
