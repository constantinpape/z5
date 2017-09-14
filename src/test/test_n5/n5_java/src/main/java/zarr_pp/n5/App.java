package zarr_pp.n5;

import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.CompressionType;
import org.janelia.saalfeldlab.n5.DoubleArrayDataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5;

import java.io.IOException;

public class App {

    // TODO irregular shapes
    static private long[] dimensions = new long[]{100, 100, 100};
    static private int[] blockSize = new int[]{10, 10, 10};
	static private String testDir = "..";
    static private String datasetName = "array.n5";
    static private String emptyDatasetName = "array_fv.n5";

    static private N5Writer n5;
    
    public static void makeTestData() throws IOException {
		
		n5 = N5.openFSWriter(testDir);

        double[] doubleBlock = new double[blockSize[0] * blockSize[1] * blockSize[2]];
        for(int i = 0; i < doubleBlock.length; i++) {
            doubleBlock[i] = 42.;
        }
        
        n5.createDataset(datasetName, dimensions, blockSize, DataType.FLOAT64, CompressionType.GZIP);
        final DatasetAttributes attributes = n5.getDatasetAttributes(datasetName);

        for(int x = 0; x < blockSize[0]; x++) {
            for(int y = 0; y < blockSize[1]; y++) {
                for(int z = 0; z < blockSize[2]; z++) {
                    final DoubleArrayDataBlock dataBlock = new DoubleArrayDataBlock(blockSize, new long[]{x, y, z}, doubleBlock);
                    n5.writeBlock(datasetName, attributes, dataBlock);
                }
            }
        }
    }


    public static void makeEmptyData() throws IOException {
		n5 = N5.openFSWriter(testDir);
        n5.createDataset(emptyDatasetName, dimensions, blockSize, DataType.FLOAT64, CompressionType.GZIP);
    }

    
    public static void main(String args[]) throws IOException {
		// FIXME this throws a null-pointer
        makeEmptyData();
        makeTestData();
    }

}
