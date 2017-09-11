//package test.test_n5;
import org.janelia.saalfeldlab.n5;

public class MakeTestData {

    // TODO irregular shapes
    static private long[] dimensions = new long[]{100, 100, 100};
    static private long[] blockSize = new long[]{10, 10, 10};
    static private String datasetName = "/array.n5";
    static private String emptyDatasetName = "/array_fv.n5";

    static private N5Writer n5;

    
    public static void makeTestData() {
        double[] dataBlock = new double[blockSize[0] * blockSize[1] * blockSize[2]];
        for(int i = 0; i < dataBlock.length; i++) {
            dataBlock[i] = 42.;
        }
        
        n5.createDataset(datasetName, dimensions, blockSize, DataType.FLOAT64, CompressionType.GZIP);
        final DatasetAttributes attributes = n5.getDatasetAttributes(datasetName);
        

        for(int x = 0; x < blockSize[0]; x++) {
            for(int y = 0; y < blockSize[1]; y++) {
                for(int z = 0; z < blockSize[2]; z++) {
                    final DoubleArrayDataBlock dataBlock = new DoubleArrayDataBlock(blockSize, new long[]{x, y, z}, dataBlock);
                    n5.writeBlock(datasetName, attributes, dataBlock);
                }
            }
        }
    }


    public static void makeEmptyData() {
        n5.createDataset(datasetName, dimensions, blockSize, DataType.FLOAT64, CompressionType.GZIP);
        final DatasetAttributes attributes = n5.getDatasetAttributes(emptyDatasetName);
    }

    
    public static void main(String args[]) {
        makeEmptyData();
        makeTestData();
    }

}
