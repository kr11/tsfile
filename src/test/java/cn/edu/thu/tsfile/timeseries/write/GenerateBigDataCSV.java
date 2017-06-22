package cn.edu.thu.tsfile.timeseries.write;

import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.timeseries.utils.StringContainer;
import cn.edu.thu.tsfile.timeseries.write.record.TSRecord;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class GenerateBigDataCSV {
    private static final Logger LOG = LoggerFactory.getLogger(GenerateBigDataCSV.class);
    public static final int ROW_COUNT = 2000;
    private static String defaultDeviceType = "sample";
    static public String inputDataFile;
    static public String outputDataFile;
    static public String errorOutputDataFile;
    static public JSONObject jsonSchema;
    // private String[] deviceList;
    // To be configure
    private static int deviceCount = 3;
    private static int sensorCount = 4;
    // s0:broken line
    // s1:line
    // s2:square wave with frequency noise
    // s3:long for sin with glitch
    // s4:log
    private static int[][] brokenLineConfigs = {{1000, 1, 100}, {200000, 5, 200000},
            {10000, 2, 50000}};
    private static long[][] lineConfigs = {{1l << 32, 1}, {1l << 22, 4}, {10000, 2}};
    private static float[] squareAmplitude = {12.5f, 1273.143f, 1823767.4f};
    private static float[] squareBaseLine = {25f, 2273.143f, 2823767.4f};
    private static int[] squareLength = {300, 5000, 20000};
    private static double[][] sinAbnormalConfigs = {{0.28, 20}, {0.3, 100}, {0.35, 50}};
    // y = A*sin(wt), sinConfigs:w,A
    private static double[][] sinConfigs = {{0.05, 2000}, {0.03, 1000}, {0.001, 200}};
    private static int[][] maxMinVertical = {{5000, 4000}, {1000, 800}, {100, 70}};
    private static int[][] maxMinHorizontal = {{5, 2}, {20, 10}, {50, 30}};
    private static double[] glitchProbability = {0.008, 0.01, 0.005};

    // y = A*log(wt), logConfigs:w,A
    // private static double[][] logConfigs = { {0.5, 2}, {0.3, 10}, {2, 5}};
    private static int[][] nullList = {{1, 1, 1, 0, 0}, {1, 0, 0, 1, 0}, {0, 1, 1, 0, 1}};
    private static TSRecord[] recordList;
    private static TSRecord[] nullRecordList;
    private static String deviceType = "root.laptop";

    public static void prepare(String outputFile) {
        // inputDataFile = "src/main/resources/perTestInputData";
        outputDataFile = outputFile;
    }
    static public enum DataShape{
        BROKEN_LINE, RANDOM, SQUARE, SIN, LINE
    }

    private static float freqWave[] = {0, 0, 0};
    public static String getNextRecordToFile(DataShape shape, TSDataType dataType, int sensorNumber, long timestamp,
                                              long index) throws IOException {
        StringContainer sc = new StringContainer(",");
        int i = 0;
        String value;
        switch (shape) {
            case BROKEN_LINE:
                // s0:broken line, int
                if ((index % brokenLineConfigs[i][2]) == 0)
                    brokenLineConfigs[i][1] = -brokenLineConfigs[i][1];
                brokenLineConfigs[i][0] += brokenLineConfigs[i][1];
                if (brokenLineConfigs[i][0] < 0) {
                    brokenLineConfigs[i][0] = -brokenLineConfigs[i][0];
                    brokenLineConfigs[i][1] = -brokenLineConfigs[i][1];
                }
                value = String.valueOf(brokenLineConfigs[i][0]);
                break;
            case RANDOM:
                switch (dataType){
                    case INT32:
                        value = String.valueOf(r.nextInt());
                        break;
                    case INT64:
                        value = String.valueOf(r.nextLong());
                        break;
                    case FLOAT:
                        value = String.valueOf(r.nextFloat());
                        break;
                    case DOUBLE:
                        value = String.valueOf(r.nextDouble());
                        break;
                    default:
                        throw new UnsupportedOperationException();
                }
                break;
            case SQUARE:
                if ((index % squareLength[i]) == 0) {
                    squareAmplitude[i] = -squareAmplitude[i];
                    if (hasWrittenFreq[i] == 0) {
                        if ((double) index == squareLength[i]) {
                            System.out.println("d" + i + ":time:" + index + ",sin sin");
                            hasWrittenFreq[i] = 1;
                        }
                    } else if (hasWrittenFreq[i] == 1) {
                        hasWrittenFreq[i] = 2;
                    }
                }
                freqWave[i] =
                        (hasWrittenFreq[i] == 1) ? (float) (squareAmplitude[i] / 2 * Math
                                .sin(sinAbnormalConfigs[i][0] * 2 * Math.PI * index)) : 0;
                value = String.valueOf(freqWave[i] + squareBaseLine[i] + squareAmplitude[i]);
                break;
            case SIN:
                value = String.valueOf(generateSinGlitch(timestamp + index, i));
                break;
            case LINE:
                lineConfigs[i][0] += lineConfigs[i][1];
                if (lineConfigs[i][0] < 0)
                    lineConfigs[i][0] = 0;
                value = String.valueOf(lineConfigs[i][0]);
                break;
            default:
                throw new UnsupportedOperationException();
        }
        sc.addTail("d1", value);
        for (int j = 0; j < sensorNumber; j++) {
            sc.addTail("s" + j, value);
        }
        return sc.toString();
    }

    private static Random r = new Random();
    private static int[] width = {-1, -1, -1};
    private static int[] mid = {0, 0, 0};
    private static long[] upPeek = {0, 0, 0};
    private static long[] downPeek = {0, 0, 0};
    private static long[] base = {0, 0, 0};
    private static long[] startAbTime = {0, 0, 0};

    private static long generateSinGlitch(long t, int i) {
        if (r.nextDouble() < glitchProbability[i] && width[i] == -1) {
            startAbTime[i] = t;
            base[i] =
                    (long) (maxMinVertical[i][0] + sinConfigs[i][1] + sinConfigs[i][1]
                            * Math.sin(sinConfigs[i][0] * t));
            width[i] =
                    r.nextInt(maxMinHorizontal[i][0] - maxMinHorizontal[i][1])
                            + maxMinHorizontal[i][1];

            if (width[i] < 2)
                width[i] = 2;
            mid[i] = r.nextInt(width[i] - 1) + 1;
            // mid = 10;
            upPeek[i] =
                    maxMinVertical[i][1] + r.nextInt(maxMinVertical[i][0] - maxMinVertical[i][1]);
            downPeek[i] =
                    maxMinVertical[i][1] + r.nextInt(maxMinVertical[i][0] - maxMinVertical[i][1]);
            // System.out.println("maocimaoci\r\n");
            // System.out.println(t + "," + base);
            return base[i];
        } else {
            if (width[i] != -1) {
                long value;
                // up
                if (t - startAbTime[i] <= mid[i]) {
                    value = (long) (base[i] + ((double) t - startAbTime[i]) / mid[i] * upPeek[i]);
                } else {
                    value =
                            (long) (base[i] + upPeek[i] - ((double) t - mid[i] - startAbTime[i])
                                    / (width[i] - mid[i]) * downPeek[i]);
                }
                if (t - startAbTime[i] == width[i])
                    width[i] = -1;
                // down
                return value;
                // System.out.println(t + "," + value);
                // if (width == -1)
                // System.out.println("maocimaoci end\r\n");
            } else {
                return (long) (maxMinVertical[i][0] + sinConfigs[i][1] + sinConfigs[i][1]
                        * Math.sin(sinConfigs[i][0] * t));
                // System.out.println(t
                // + ","
                // + (long) (sinConfigs[0][1] + sinConfigs[0][1]
                // * Math.sin(sinConfigs[0][0] * (t))));
            }
        }
    }


    private static Set<String> sensorSet = new HashSet<String>();
    private static long lineCount;
    private static double writeFreqFraction[] = {0, 0, 0};
    // 0:not write->1:writing->2:written
    private static int hasWrittenFreq[] = {0, 0, 0};

}
