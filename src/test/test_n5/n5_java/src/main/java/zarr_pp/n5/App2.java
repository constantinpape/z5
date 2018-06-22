package zarr_pp.n5;

// import org.janelia.saalfeldlab.n5.N5Reader;
// import org.janelia.saalfeldlab.n5.DataType;
// import org.janelia.saalfeldlab.n5.CompressionType;
// 
// import org.janelia.saalfeldlab.n5.DataBlock;
// import org.janelia.saalfeldlab.n5.ByteArrayDataBlock;
// import org.janelia.saalfeldlab.n5.ShortArrayDataBlock;
// import org.janelia.saalfeldlab.n5.IntArrayDataBlock;
// import org.janelia.saalfeldlab.n5.LongArrayDataBlock;
// import org.janelia.saalfeldlab.n5.FloatArrayDataBlock;
// import org.janelia.saalfeldlab.n5.DoubleArrayDataBlock;
// 
// import org.janelia.saalfeldlab.n5.DatasetAttributes;
// import org.janelia.saalfeldlab.n5.N5;

import java.io.IOException;

// public class App2 {
//     
//     static private int[] blockSize = new int[]{10, 10, 10};
// 	static private String testDir = "../n5_test_data_out";
//     
//     static private String byteSetRaw = "array_byte_raw.n5";
//     static private String shortSetRaw = "array_short_raw.n5";
//     static private String intSetRaw = "array_int_raw.n5";
//     static private String longSetRaw = "array_long_raw.n5";
//     static private String floatSetRaw = "array_float_raw.n5";
//     static private String doubleSetRaw = "array_double_raw.n5";
//     
//     static private String byteSetGzip = "array_byte_gzip.n5";
//     static private String shortSetGzip = "array_short_gzip.n5";
//     static private String intSetGzip = "array_int_gzip.n5";
//     static private String longSetGzip = "array_long_gzip.n5";
//     static private String floatSetGzip = "array_float_gzip.n5";
//     static private String doubleSetGzip = "array_double_gzip.n5";
// 
//     static private N5Reader n5;
// 
// 
//     public static void testReadBlockRaw() throws IOException {
//         
//         // open the n5 reader
//         n5 = N5.openFSReader(testDir);
//         
//         // open the byte array
//         final DatasetAttributes attrsByte = n5.getDatasetAttributes(byteSetRaw);
//         
//         // open the short array
//         final DatasetAttributes attrsShort = n5.getDatasetAttributes(shortSetRaw);
//         
//         // open the int array
//         final DatasetAttributes attrsInt = n5.getDatasetAttributes(intSetRaw);
//         
//         // open the long array
//         final DatasetAttributes attrsLong = n5.getDatasetAttributes(longSetRaw);
//         
//         // open the float array
//         final DatasetAttributes attrsFloat = n5.getDatasetAttributes(floatSetRaw);
//         
//         // open the double array
//         final DatasetAttributes attrsDouble = n5.getDatasetAttributes(doubleSetRaw);
//         
//         // read and test all the blocks
//         for(int x = 0; x < blockSize[0]; x++) {
//             for(int y = 0; y < blockSize[1]; y++) {
//                 for(int z = 0; z < blockSize[2]; z++) {
//                     
//                     // test the byte block
//                     final DataBlock<?> loadedByte = n5.readBlock(byteSetRaw, attrsByte, new long[]{x, y, z});
//                     final byte[] byteData = (byte[]) loadedByte.getData();
//                     for(int i = 0; i < byteData.length; i++) {
//                         assert byteData[i] == 42;
//                     }
//                     
//                     // test the short block
//                     final DataBlock<?> loadedShort = n5.readBlock(shortSetRaw, attrsShort, new long[]{x, y, z});
//                     final short[] shortData = (short[]) loadedShort.getData();
//                     for(int i = 0; i < shortData.length; i++) {
//                         assert shortData[i] == 42;
//                     }
//                     
//                     // test the int block
//                     final DataBlock<?> loadedInt = n5.readBlock(intSetRaw, attrsInt, new long[]{x, y, z});
//                     final int[] intData = (int[]) loadedInt.getData();
//                     for(int i = 0; i < intData.length; i++) {
//                         assert intData[i] == 42;
//                     }
//                     
//                     // test the long block
//                     final DataBlock<?> loadedLong = n5.readBlock(longSetRaw, attrsLong, new long[]{x, y, z});
//                     final long[] longData = (long[]) loadedLong.getData();
//                     for(int i = 0; i < longData.length; i++) {
//                         assert longData[i] == 42;
//                     }
//                     
//                     // test the float block
//                     final DataBlock<?> loadedFloat = n5.readBlock(floatSetRaw, attrsFloat, new long[]{x, y, z});
//                     final float[] floatData = (float[]) loadedFloat.getData();
//                     for(int i = 0; i < floatData.length; i++) {
//                         assert floatData[i] == 42.;
//                     }
//                     
//                     // test the doubl block
//                     final DataBlock<?> loadedDouble = n5.readBlock(doubleSetRaw, attrsDouble, new long[]{x, y, z});
//                     final double[] doubleData = (double[]) loadedDouble.getData();
//                     for(int i = 0; i < doubleData.length; i++) {
//                         assert doubleData[i] == 42.;
//                     }
//                 }
//             }
//         }
//         System.out.println("N5: All Raw Tests passed");
//     }
// 
// 
//     public static void testReadBlockGzip() throws IOException {
//         
//         // open the n5 reader
//         n5 = N5.openFSReader(testDir);
//         
//         // open the byte array
//         final DatasetAttributes attrsByte = n5.getDatasetAttributes(byteSetGzip);
//         
//         // open the short array
//         final DatasetAttributes attrsShort = n5.getDatasetAttributes(shortSetGzip);
//         
//         // open the int array
//         final DatasetAttributes attrsInt = n5.getDatasetAttributes(intSetGzip);
//         
//         // open the long array
//         final DatasetAttributes attrsLong = n5.getDatasetAttributes(longSetGzip);
//         
//         // open the float array
//         final DatasetAttributes attrsFloat = n5.getDatasetAttributes(floatSetGzip);
//         
//         // open the double array
//         final DatasetAttributes attrsDouble = n5.getDatasetAttributes(doubleSetGzip);
//         
//         // read and test all the blocks
//         for(int x = 0; x < blockSize[0]; x++) {
//             for(int y = 0; y < blockSize[1]; y++) {
//                 for(int z = 0; z < blockSize[2]; z++) {
//                     
//                     // test the byte block
//                     final DataBlock<?> loadedByte = n5.readBlock(byteSetGzip, attrsByte, new long[]{x, y, z});
//                     final byte[] byteData = (byte[]) loadedByte.getData();
//                     for(int i = 0; i < byteData.length; i++) {
//                         assert byteData[i] == 42;
//                     }
//                     
//                     // test the short block
//                     final DataBlock<?> loadedShort = n5.readBlock(shortSetGzip, attrsShort, new long[]{x, y, z});
//                     final short[] shortData = (short[]) loadedShort.getData();
//                     for(int i = 0; i < shortData.length; i++) {
//                         assert shortData[i] == 42;
//                     }
//                     
//                     // FIXME int, long and double tests fail with an EOF exception !
//                     // test the int block
//                     final DataBlock<?> loadedInt = n5.readBlock(intSetGzip, attrsInt, new long[]{x, y, z});
//                     final int[] intData = (int[]) loadedInt.getData();
//                     for(int i = 0; i < intData.length; i++) {
//                         assert intData[i] == 42;
//                     }
//                     
//                     // test the long block
//                     final DataBlock<?> loadedLong = n5.readBlock(longSetGzip, attrsLong, new long[]{x, y, z});
//                     final long[] longData = (long[]) loadedLong.getData();
//                     for(int i = 0; i < longData.length; i++) {
//                         assert longData[i] == 42;
//                     }
//                     
//                     // test the float block
//                     final DataBlock<?> loadedFloat = n5.readBlock(floatSetGzip, attrsFloat, new long[]{x, y, z});
//                     final float[] floatData = (float[]) loadedFloat.getData();
//                     for(int i = 0; i < floatData.length; i++) {
//                         assert floatData[i] == 42.;
//                     }
//                     
//                     // test the double block
//                     final DataBlock<?> loadedDouble = n5.readBlock(doubleSetGzip, attrsDouble, new long[]{x, y, z});
//                     final double[] doubleData = (double[]) loadedDouble.getData();
//                     for(int i = 0; i < doubleData.length; i++) {
//                         assert doubleData[i] == 42.;
//                     }
//                 }
//             }
//         }
//         System.out.println("N5: All Gzip Tests passed");
//     }
// 
// 
//     public static void main(String args[]) throws IOException {
//         testReadBlockRaw();
//         testReadBlockGzip();
//     }
// 
// 
// }
