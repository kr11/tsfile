package cn.edu.thu.tsfile.timeseries.write.page;

import cn.edu.thu.tsfile.common.utils.bytesinput.ListByteArrayOutputStream;
import cn.edu.thu.tsfile.compress.Compressor;
import cn.edu.thu.tsfile.file.metadata.statistics.Statistics;
import cn.edu.thu.tsfile.file.utils.ReadWriteThriftFormatUtils;
import cn.edu.thu.tsfile.timeseries.write.desc.MeasurementDescriptor;
import cn.edu.thu.tsfile.timeseries.write.exception.PageException;
import cn.edu.thu.tsfile.timeseries.write.io.TSFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * a implementation of {@linkplain IPageWriter IPageWriter}
 *
 * @author kangrong
 * @see IPageWriter IPageWriter
 */
public class PageWriterImpl implements IPageWriter {
    private static Logger LOG = LoggerFactory.getLogger(PageWriterImpl.class);

    private ListByteArrayOutputStream buf;
    private final Compressor compressor;
    private final MeasurementDescriptor desc;

    private long totalValueCount;
    private long maxTimestamp;
    private long minTimestamp = -1;

    public PageWriterImpl(MeasurementDescriptor desc) {
        this.desc = desc;
        this.compressor = desc.getCompressor();
        this.buf = new ListByteArrayOutputStream();
    }

    @Override
    public void writePage(ListByteArrayOutputStream bytesInput, int valueCount, Statistics<?> statistics,
                          long maxTimestamp, long minTimestamp) throws PageException {
        // compress the input data
        if (this.minTimestamp == -1)
            this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
        long uncompressedSize = bytesInput.size();
        if (uncompressedSize > Integer.MAX_VALUE) {
            resetTimeStamp();
            throw new PageException("write too much bytes: " + uncompressedSize);
        }
        ListByteArrayOutputStream compressedBytes = compressor.compress(bytesInput);
        long compressedSize = compressedBytes.size();
        if (compressedSize > Integer.MAX_VALUE) {
            resetTimeStamp();
            throw new PageException("write too much bytes: " + compressedSize);
        }
        ByteArrayOutputStream tempOutputStream = new ByteArrayOutputStream(
                estimateMaxPageHeaderSize() + bytesInput.size());
        // write the page header to IOWriter
        try {
            ReadWriteThriftFormatUtils.writeDataPageHeader((int) uncompressedSize,
                    (int) compressedSize, valueCount, statistics, valueCount,
                    desc.getEncodingType(), tempOutputStream, maxTimestamp, minTimestamp);
        } catch (IOException e) {
            resetTimeStamp();
            throw new PageException(
                    "meet IO Exception in writeDataPageHeader,ignore this page,error message:"
                            + e.getMessage());
        }
        this.totalValueCount += valueCount;
//        LOG.info("write page, buf size:{}, buf capacity:{}", buf.size());
        try {
            bytesInput.writeAllTo(tempOutputStream);
        } catch (IOException e) {
            /*In our method, this line is to flush bytesInput to buf, both of them is in class of ListByteArrayOutputStream,
            and contain several ByteArrayOutputStream.
            In general, it won't throw exception. The IOException is just for interface requirement of OutputStream.
             */
            throw new PageException(
                    "meet IO Exception in buffer append,but we cannot understand it:"
                            + e.getMessage());
        }
        buf.append(tempOutputStream);
        LOG.debug("page {}:write page from seriesWriter, valueCount:{}, stats:{},size:{}", desc,
                valueCount, statistics, estimateMaxPageMemSize());
    }

    private void resetTimeStamp() {
        if (totalValueCount == 0)
            minTimestamp = -1;
    }

    @Override
    public void writeToFileWriter(TSFileIOWriter writer, Statistics<?> statistics)
            throws IOException {
        writer.startSeries(desc, compressor.getCodecName(), desc.getType(), statistics,
                maxTimestamp, minTimestamp);
        long totalByteSize = writer.getPos();
        writer.writeBytesToStream(buf);
        LOG.debug("write series to file finished:{}", desc);
        long size = writer.getPos() - totalByteSize;
        writer.endSeries(size, totalValueCount);
        LOG.debug(
                "page {}:write page to fileWriter,type:{},maxTime:{},minTime:{},nowPos:{},stats:{}",
                desc.getMeasurementId(), desc.getType(), maxTimestamp, minTimestamp,
                writer.getPos(), statistics);
    }

    @Override
    public void reset() {
        minTimestamp = -1;
        buf.reset();
        totalValueCount = 0;
        LOG.info("buf size:" + buf.size());
    }

    @Override
    public long estimateMaxPageMemSize() {
        // return size of buffer + page max size;
        return buf.size() + estimateMaxPageHeaderSize();
    }

    private int estimateMaxPageHeaderSize() {
        int digestSize = (totalValueCount == 0) ? 0 : desc.getTypeLength() * 2;
        return TSFileIOWriter.metadataConverter.caculatePageHeaderSize(digestSize);
    }
}
