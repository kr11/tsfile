//package cn.edu.thu.tsfile.common.utils;
//
//import java.io.IOException;
//import java.io.OutputStream;
//import java.util.ArrayList;
//
///**
// * Writes a given byte or byte array to a byte buffer {@code currentBuf}.
// * If the size of parameter bytes exceed the rest capacity of {@code currentBuf},
// * current buffer will be put into {@code bytesList} and a new byte buffer is allocated.
// * Note that {@code LinkedByteArrayOutputStream} is non thread-safe.
// * Created by kangrong on 17/6/12.
// */
//public class LinkedByteArrayOutputStream extends OutputStream {
//    /**
//     * The current buffer array where data is inputted and stored.
//     */
//    private byte[] currentBuf;
//    private ArrayList<byte[]> bytesList;
//
//    /**
//     * The number of total bytes in the buffer and list.
//     */
//    private int count = 0;
//    private int total = 0;
//    private int increment;
//
//    /**
//     * Creates a new linked byte array output stream.
//     * <p>
//     * with a buffer capacity of
//     * the specified size, in bytes.
//     *
//     * @param initialSize the initial buffer size.
//     * @throws IllegalArgumentException if initialSize and increaseRate <= 0.
//     */
//    public LinkedByteArrayOutputStream(int initialSize, double increaseRate) {
//        if (initialSize <= 0) {
//            throw new IllegalArgumentException("Negative initial size: " + initialSize);
//        }
//        if (increaseRate <= 0) {
//            throw new IllegalArgumentException("Negative increasing size: " + increaseRate);
//        }
//        currentBuf = new byte[initialSize];
//        increment = (int) increaseRate * initialSize;
//        bytesList = new ArrayList<>();
//    }
//
//    /**
//     * puts {@code currentBuf} into {@code bytesList} and allocate a new buffer.
//     */
//    private void allocateNewBuffer() {
//        bytesList.add(currentBuf);
//        total += currentBuf.length;
//        currentBuf = new byte[increment];
//    }
//
//    /**
//     * Writes a byte.
//     *
//     * @param b the byte to be written.
//     */
//    public void write(int b) {
//        currentBuf[count++] = (byte) b;
//        if (count == currentBuf.length) {
//            allocateNewBuffer();
//            count = 0;
//        }
//    }
//
//    /**
//     * Writes <code>len</code> bytes from the specified byte array starting from offset <code>off</code>.
//     * If the byte array to be written exceeds the remaining capacity of {@code currentBuf}, allocates a new buffer.
//     *
//     * @param b   the src byte array.
//     * @param off the start offset.
//     * @param len the size of bytes to be written.
//     */
//    public void write(byte b[], int off, int len) {
//        if ((off < 0) || (off > b.length) || (len < 0) || ((off + len) - b.length > 0)) {
//            throw new IndexOutOfBoundsException();
//        }
//        if (len + count < currentBuf.length) {
//            System.arraycopy(b, off, currentBuf, count, len);
//            count += len;
//        } else {
//            int lastLen = currentBuf.length - count;
//            System.arraycopy(b, off, currentBuf, count, lastLen);
//            allocateNewBuffer();
//            count = len - lastLen;
//            System.arraycopy(b, off + lastLen, currentBuf, 0, count);
//        }
//    }
//
//    /**
//     * Flushes the complete contents of this linked byte array output stream to
//     * the specified output stream argument.
//     *
//     * @param out the output stream to be flushed.
//     * @throws IOException if an I/O error occurs.
//     */
//    public void writeAllTo(OutputStream out) throws IOException {
//        for (byte[] bytes : bytesList) {
//            out.write(bytes, 0, bytes.length);
//        }
//        out.write(currentBuf, 0, count);
//    }
//
//    /**
//     * Resets the <code>count</code> field to zero and clears {@code bytesList}.
//     * {@code currentBuf} will be reused although its length may be {@code increaseRate * initialSize}
//     * but not {@code initialSize}.
//     */
//    public void reset() {
//        count = 0;
//        bytesList.clear();
//    }
//
//    /**
//     * Creates a newly allocated byte array.
//     * Notes that, this class prefers to be used as a buffer to store output data temporarily and flush out to
//     * another {@code OutputStream} using method {@code writeTo}.
//     *
//     * @return the current contents of this linked byte array output stream.
//     * @see #writeTo(OutputStream)
//     */
//    @Deprecated
//    public byte[] toByteArray() {
//        throw new UnsupportedOperationException();
//    }
//
//    /**
//     * Returns the current size of the buffer.
//     */
//    public synchronized int size() {
//        return total + count;
//    }
//
//    public void close() throws IOException {
//        reset();
//    }
//}
