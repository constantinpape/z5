package zarr_pp.n5;

import org.janelia.saalfeldlab.n5.*;

import java.io.IOException;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.Random;

public class App {

    // TODO irregular shapes
    static private long[] dimensions = new long[]{111, 121, 113};
    static private int[] blockSize = new int[]{17, 25, 14};
	static private String testFile = "../n5_test_data.n5";

    static byte[] byteBlock;
    static short[] shortBlock;
    static int[] intBlock;
    static long[] longBlock;
    static float[] floatBlock;
    static double[] doubleBlock;
	
    protected static Compression[] getCompressions() {

		return new Compression[] {
				new RawCompression(),
				new Bzip2Compression(),
				new GzipCompression(),
				new Lz4Compression(),
				new XzCompression()
			};
	}

    static private N5Writer n5;
    
    public static void makeTestData() throws IOException {
		
		n5 = openFSWriter(testFile);
        byteBlock = new byte[blockSize[0] * blockSize[1] * blockSize[2]];
        shortBlock = new short[blockSize[0] * blockSize[1] * blockSize[2]];
        intBlock = new int[blockSize[0] * blockSize[1] * blockSize[2]];
        longBlock = new long[blockSize[0] * blockSize[1] * blockSize[2]];
        floatBlock = new float[blockSize[0] * blockSize[1] * blockSize[2]];
        doubleBlock = new double[blockSize[0] * blockSize[1] * blockSize[2]];

        // random test data
        final Random rnd = new Random();
        for(int i = 0; i < doubleBlock.length; i++) {
            byteBlock[i] = 42;
            shortBlock[i] = 42;
            intBlock[i] = 42;
            longBlock[i] = 42;
            floatBlock[i] = 42;
            doubleBlock[i] = 42;
        }
        

		for (final Compression compression : getCompressions()) {
            final String compressionName = compression.getType();
            // byte set
            final String byteName = "data_byte_" + compressionName;
            n5.createDataset(byteName , dimensions, blockSize, DataType.INT8, compression);
            final DatasetAttributes attrsByte = n5.getDatasetAttributes(byteName);
            // short set
            final String shortName = "data_short_" + compressionName;
            n5.createDataset(shortName, dimensions, blockSize, DataType.INT16, compression);
            final DatasetAttributes attrsShort = n5.getDatasetAttributes(shortName);
            // int set
            final String intName = "data_int_" + compressionName;
            n5.createDataset(intName, dimensions, blockSize, DataType.INT32, compression);
            final DatasetAttributes attrsInt = n5.getDatasetAttributes(intName);
            // long set
            final String longName = "data_long_" + compressionName;
            n5.createDataset(longName, dimensions, blockSize, DataType.INT64, compression);
            final DatasetAttributes attrsLong = n5.getDatasetAttributes(longName);
            // float set
            final String floatName = "data_float_" + compressionName;
            n5.createDataset(floatName, dimensions, blockSize, DataType.FLOAT32, compression);
            final DatasetAttributes attrsFloat = n5.getDatasetAttributes(floatName);
            // double set
            final String doubleName = "data_double_" + compressionName;
            n5.createDataset(doubleName, dimensions, blockSize, DataType.FLOAT64, compression);
            final DatasetAttributes attrsDouble = n5.getDatasetAttributes(doubleName);

            for(int x = 0; x < blockSize[0]; x++) {
                for(int y = 0; y < blockSize[1]; y++) {
                    for(int z = 0; z < blockSize[2]; z++) {
                        
                        // write byte
                        final ByteArrayDataBlock byteDataBlock = new ByteArrayDataBlock(
                            blockSize, new long[]{x, y, z}, byteBlock
                        );
                        n5.writeBlock(byteName, attrsByte, byteDataBlock);
                        
                        // write short
                        final ShortArrayDataBlock shortDataBlock = new ShortArrayDataBlock(
                            blockSize, new long[]{x, y, z}, shortBlock
                        );
                        n5.writeBlock(shortName, attrsShort, shortDataBlock);
                        
                        // write int
                        final IntArrayDataBlock intDataBlock = new IntArrayDataBlock(
                            blockSize, new long[]{x, y, z}, intBlock
                        );
                        n5.writeBlock(intName, attrsInt, intDataBlock);
                
                        // write long
                        final LongArrayDataBlock longDataBlock = new LongArrayDataBlock(
                            blockSize, new long[]{x, y, z}, longBlock
                        );
                        n5.writeBlock(longName, attrsLong, longDataBlock);
                        
                        // write float
                        final FloatArrayDataBlock floatDataBlock = new FloatArrayDataBlock(
                            blockSize, new long[]{x, y, z}, floatBlock
                        );
                        n5.writeBlock(floatName, attrsFloat, floatDataBlock);
                        
                        // write double
                        final DoubleArrayDataBlock doubleDataBlock = new DoubleArrayDataBlock(
                            blockSize, new long[]{x, y, z}, doubleBlock
                        );
                        n5.writeBlock(doubleName, attrsDouble, doubleDataBlock);
                    }
                }
            }
        }
    }
    

    public static void makeEmptyData() throws IOException {
		n5 = openFSWriter(testFile);
        n5.createDataset("data_empty", dimensions, blockSize, DataType.INT32, GzipCompression());
    }

    
    public static void main(String args[]) throws IOException {
        makeEmptyData();
        makeTestData();
    }

}
