package org.jolokia.util;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.*;

/**
 * Created by gnufied on 2/7/16.
 */
public class ChunkedWriter extends Writer {

    private OutputStream out;
    private Charset cs;
    private CharsetEncoder encoder;
    private ByteBuffer bb;
    // Leftover first char in a surrogate pair
    private boolean haveLeftoverChar = false;
    private char leftoverChar;
    private CharBuffer lcb = null;

    private static final byte[] CRLF = {'\r', '\n'};
    private static final int CRLF_SIZE = CRLF.length;
    private static final byte[] FOOTER = CRLF;
    private static final int FOOTER_SIZE = CRLF_SIZE;
    private static final byte[] EMPTY_CHUNK_HEADER = getHeader(0);
    private static final int EMPTY_CHUNK_HEADER_SIZE = getHeaderSize(0);

    /* return the size of the header for a particular chunk size */
    private static int getHeaderSize(int size) {
        return (Integer.toHexString(size)).length() + CRLF_SIZE;
    }

    /* return a header for a particular chunk size */
    private static byte[] getHeader(int size){
        try {
            String hexStr =  Integer.toHexString(size);
            byte[] hexBytes = hexStr.getBytes("US-ASCII");
            byte[] header = new byte[getHeaderSize(size)];
            for (int i=0; i<hexBytes.length; i++)
                header[i] = hexBytes[i];
            header[hexBytes.length] = CRLF[0];
            header[hexBytes.length+1] = CRLF[1];
            return header;
        } catch (java.io.UnsupportedEncodingException e) {
            /* This should never happen */
            throw new InternalError(e.getMessage(), e);
        }
    }

    public ChunkedWriter(OutputStream stream, String charset) {
        super(stream);
        this.out = stream;
        if (Charset.isSupported(charset)) {
            this.cs = Charset.forName(charset);
            this.encoder = cs.newEncoder().onMalformedInput(CodingErrorAction.REPLACE)
                    .onUnmappableCharacter(CodingErrorAction.REPLACE);
        } else {
            throw new UnsupportedCharsetException(charset);
        }
        bb = ByteBuffer.allocate(DEFAULT_BYTE_BUFFER_SIZE);
    }

    private static final int DEFAULT_BYTE_BUFFER_SIZE = 4096;

    private volatile boolean isOpen = true;

    private void ensureOpen() throws IOException {
        if (!isOpen)
            throw new IOException("Stream closed");
    }

    private boolean isOpen() { return isOpen; }

    @Override
    public void write(char[] cbuf, int off, int len) throws IOException {
        synchronized (lock) {
            ensureOpen();
            if ((off < 0) || (off > cbuf.length) || (len < 0) ||
                    ((off + len) > cbuf.length) || ((off + len) < 0)) {
                throw new IndexOutOfBoundsException();
            } else if (len == 0) {
                return;
            }
        }
    }

    @Override
    public void flush() throws IOException {
        synchronized (lock) {
            ensureOpen();
            implFlush();
        }
    }

    void implFlushBuffer() throws IOException {
        if (bb.position() > 0)
            writeBytes();
        out.write(getHeader(0));
        out.write(CRLF);
    }

    void implFlush() throws IOException {
        implFlushBuffer();
        if (out != null)
            out.flush();
    }

    @Override
    public void close() throws IOException {

    }

    void implWrite(char cbuf[], int off, int len) throws IOException{
        CharBuffer cb = CharBuffer.wrap(cbuf,off, len);

        if(haveLeftoverChar)
            flushLeftOverChar(cb, false);

        while (cb.hasRemaining()) {
            CoderResult cr = encoder.encode(cb, bb, false);

            if (cr.isUnderflow()) {
                assert (cb.remaining() <= 1) : cb.remaining();

                if(cb.remaining() == 1) {
                    haveLeftoverChar = true;
                    leftoverChar = cb.get();
                }
                break;
            }

            if (cr.isOverflow()) {
                assert bb.position() > 0;
                writeBytes();
                continue;
            }
            cr.throwException();
        }

    }

    private void flushLeftOverChar(CharBuffer cb, boolean endOfInput) throws IOException{
        if (!haveLeftoverChar && !endOfInput)
            return;

        if (lcb == null)
            lcb = CharBuffer.allocate(2);
        else
            lcb.clear();

        if (haveLeftoverChar)
            lcb.put(leftoverChar);

        if ((cb != null) && cb.hasRemaining())
            lcb.put(cb.get());
        lcb.flip();

        while (lcb.hasRemaining() || endOfInput) {
            CoderResult cr = encoder.encode(lcb, bb, endOfInput);

            if(cr.isUnderflow()) {
                if (lcb.hasRemaining()) {
                    leftoverChar = lcb.get();
                    if (cb != null && cb.hasRemaining())
                        flushLeftOverChar(cb,endOfInput);
                    return;
                }
                break;
            }

            if(cr.isOverflow()) {
                assert bb.position() > 0;
                writeBytes();
                continue;
            }
            cr.throwException();
        }
        haveLeftoverChar = false;
    }

    private void writeBytes() throws IOException{
        bb.flip();
        int lim = bb.limit();
        int pos = bb.position();
        assert (pos <= lim);

        int rem = (pos <= lim ? lim - pos : 0);

        if (rem > 0) {
            out.write(getHeader(rem));
            out.write(bb.array(), bb.arrayOffset() + pos, rem);
            out.write(CRLF);
        }
        bb.clear();
    }
}
