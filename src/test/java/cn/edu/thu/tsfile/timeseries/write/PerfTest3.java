package cn.edu.thu.tsfile.timeseries.write;

import org.junit.Test;

import java.io.*;

/**
 * -ea -Xmx4m -Xms2m, 会爆gc，虽然8 * 200000B = 1.5MB
 *
 * @author kangrong
 */
public class PerfTest3 {
    @Test
    public void writeToFile() throws InterruptedException, IOException {
        long lineCount = 0;
//        RandomAccessOutputStream outfile = new RandomAccessOutputStream(new File("testVisualVM"), "rw");
        OutputStream outfile = new OutputStream() {
            RandomAccessFile r = new RandomAccessFile(new File("testVisualVM"), "rw");

            @Override
            public void write(int b) throws IOException {
                r.write(b);
            }

            @Override
            public void write(byte[] b) throws IOException {
                r.write(b);
            }
        };
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
//        byte[] bs = BytesUtils.longToBytes(1);
        byte[] bs = {0, 0, 0, 0, 0, 0, 0, 1};
        int limit = 8 * 200000;
        while (true) {
            if (lineCount % 10000 == 0) {
                if (lineCount % 100000 == 0) {
                    long endTime = System.currentTimeMillis();
                    // logger.info("write line:{},inner space consumer:{},use
                    // time:{}",lineCount,innerWriter.calculateMemSizeForEachGroup(),endTime);
//                    LOG.info("write line:{},use time:{}s", lineCount, (endTime - startTime) / 1000);
                }
            }
            stream.write(bs);
            if (stream.size() > limit) {
                stream.writeTo(outfile);
                stream = new ByteArrayOutputStream();
                System.out.println("write, clear");
                Thread.sleep(1000);
            }
            lineCount++;
        }
    }

}
