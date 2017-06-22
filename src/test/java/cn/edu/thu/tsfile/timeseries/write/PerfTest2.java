package cn.edu.thu.tsfile.timeseries.write;

import cn.edu.thu.tsfile.common.conf.TSFileConfig;
import cn.edu.thu.tsfile.common.conf.TSFileDescriptor;
import cn.edu.thu.tsfile.common.constant.JsonFormatConstant;
import cn.edu.thu.tsfile.common.utils.RandomAccessOutputStream;
import cn.edu.thu.tsfile.common.utils.TSRandomAccessFileWriter;
import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.thu.tsfile.timeseries.utils.FileUtils;
import cn.edu.thu.tsfile.timeseries.utils.FileUtils.Unit;
import cn.edu.thu.tsfile.timeseries.utils.RecordUtils;
import cn.edu.thu.tsfile.timeseries.write.GenerateBigDataCSV.DataShape;
import cn.edu.thu.tsfile.timeseries.write.exception.WriteProcessException;
import cn.edu.thu.tsfile.timeseries.write.io.TSFileIOWriter;
import cn.edu.thu.tsfile.timeseries.write.record.TSRecord;
import cn.edu.thu.tsfile.timeseries.write.schema.FileSchema;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;
import java.util.Scanner;

/**
 * This is used for performance test, no asserting. User could change {@code ROW_COUNT} for larger
 * data test.
 *
 * @author kangrong
 */
public class PerfTest2 {
    private static final Logger LOG = LoggerFactory.getLogger(PerfTest2.class);
    public static final int ROW_COUNT = 800000;
    public static InternalRecordWriter<TSRecord> innerWriter;
    static public String inputDataFile;
    static public String outputDataFile;
    static public String errorOutputDataFile;
    static public JSONObject jsonSchema;
    static public Random r = new Random();

    @Before
    public void prepare() throws IOException {
        inputDataFile = "src/test/resources/perTestInputData";
        outputDataFile = "src/test/resources/lalaland.ksn";
        errorOutputDataFile = "src/test/resources/perTestErrorOutputData.ksn";
//        generateSampleInputDataFile();
        setParameter();
    }

    //0:直线；1：随机；2：方波；
    public static int[] pageCandidates = {128 * 1024 / 4, 256 * 1024 / 4, 384 * 1024 / 4, 768 * 1024 / 4};
    public static int[] blockCandidates = {32 * 1048576 / 4, 64 * 1048576 / 4, 128 * 1048576 / 4, 192 * 1048576 / 4};
    public static int[] sensorCountCandidates = {1, 2, 3, 6};
//    public static TS[] sensorCountCandidates = {1, 2, 3, 6};

    static private DataShape dataShape = DataShape.BROKEN_LINE;
    static private TSDataType dataType = TSDataType.INT32;
    static private TSEncoding encoding = TSEncoding.TS_2DIFF;

    static private int pageSize = pageCandidates[0];
    static private int blockSize = blockCandidates[0];
    static private int sensorNumber = sensorCountCandidates[2];


    private void setParameter() {
        TSFileConfig conf = TSFileDescriptor.getInstance().getConfig();
        conf.pageSize = pageSize;
        conf.rowGroupSize = blockSize;
        conf.timeSeriesEncoder = encoding.toString();
        jsonSchema = generateTestData(encoding.toString(), dataType, sensorNumber);
    }


    //    @After
    public void after() {
        File file = new File(inputDataFile);
        if (file.exists())
            file.delete();
        file = new File(outputDataFile);
        if (file.exists())
            file.delete();
        file = new File(errorOutputDataFile);
        if (file.exists())
            file.delete();
    }

    @Test
    public void writeTest() throws IOException, InterruptedException, WriteProcessException {
        for (int i = 0; i < 4; i++) {
            for (int j = 2; j < 4; j++) {
                while (true) {
                    pageSize = pageCandidates[i];
                    sensorNumber = sensorCountCandidates[j] * 100;
                    jsonSchema = generateTestData(encoding.toString(), dataType, sensorNumber);
                    System.out.println("====================================");
                    LOG.info("set pageSize:{}, sensorNumber:{}， block: {}M", pageSize, sensorNumber, blockSize / 1024 / 1024);
                    write();
                    System.out.println("====================================");
                    LOG.info("write finish, pageSize:{}, sensorNumber:{}, block: {}M", pageSize, sensorNumber, blockSize / 1024 / 1024);
                    LOG.info("sleep 2 second, then gc, then sleep 2 second");
                    Thread.sleep(1000);
//                    System.gc();
//                    Thread.sleep(1000);
                }
            }
        }
    }

    static public void write() throws IOException, InterruptedException, WriteProcessException {
        File file = new File(outputDataFile);
        File errorFile = new File(errorOutputDataFile);
        if (file.exists())
            file.delete();
        if (errorFile.exists())
            errorFile.delete();

        //LOG.info(jsonSchema.toString());
        FileSchema schema = new FileSchema(jsonSchema);
        WriteSupport<TSRecord> writeSupport = new TSRecordWriteSupport();
        TSRandomAccessFileWriter outputStream = new RandomAccessOutputStream(file);
        TSFileIOWriter tsfileWriter = new TSFileIOWriter(schema, outputStream);

        // TSFileDescriptor.conf.rowGroupSize = 2000;
        // TSFileDescriptor.conf.pageSize = 100;
        innerWriter = new TSRecordWriter(TSFileDescriptor.getInstance().getConfig(), tsfileWriter, writeSupport, schema);

        // write
        try {
            writeToFile(schema);
        } catch (WriteProcessException e) {
            e.printStackTrace();
        }
        LOG.info("write to file successfully!!");
    }

    static private Scanner getDataFile(String path) {
        File file = new File(path);
        try {
            Scanner in = new Scanner(file);
            return in;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return null;
        }
    }

    static public void writeToFile(FileSchema schema) throws InterruptedException, IOException, WriteProcessException {
        Scanner in = getDataFile(inputDataFile);
        long lineCount = 0;
        long startTime = System.currentTimeMillis();
        long endTime = System.currentTimeMillis();
        assert in != null;
//        String dev = "d1,";
//        String sen = ",s1,";
//        while (lineCount < ROW_COUNT) {
        while (true) {
            if (lineCount % 100000 == 0) {
//                if (lineCount % 100000 == 0) {
                endTime = System.currentTimeMillis();
                // time:{}",lineCount,innerWriter.calculateMemSizeForEachGroup(),endTime);
                LOG.info("write line:{},use time:{}s", lineCount, (endTime - startTime) / 1000);
//                }
            }
//            String str = dev + lineCount + sen;
            String str = GenerateBigDataCSV.getNextRecordToFile(dataShape, dataType, sensorNumber, 0, lineCount);
            TSRecord record = RecordUtils.parseSimpleTupleRecord(str, schema);
//            record.time = lineCount;
            innerWriter.write(record);
            lineCount++;
        }
    }

    private static JSONObject generateTestData(String encoding, TSDataType dataType, int sensorNumber) {
        JSONArray measureGroup1 = new JSONArray();
        for (int i = 0; i < sensorNumber; i++) {
            JSONObject s1 = new JSONObject();
            s1.put(JsonFormatConstant.MEASUREMENT_UID, "s" + i);
            s1.put(JsonFormatConstant.DATA_TYPE, dataType.toString());
            s1.put(JsonFormatConstant.MEASUREMENT_ENCODING,
                    encoding);
            measureGroup1.put(s1);
        }
        JSONObject jsonSchema = new JSONObject();
        jsonSchema.put(JsonFormatConstant.DELTA_TYPE, "test_type");
        jsonSchema.put(JsonFormatConstant.JSON_SCHEMA, measureGroup1);
        return jsonSchema;
    }
}
