package org.jolokia.util;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;

public class ChunkedOutputStream extends OutputStream {

    // ------------------------------------------------------- Static Variables

    /** "\r\n", as bytes. */
    private static final byte CRLF[] = new byte[] {(byte) 13, (byte) 10};

    /** End chunk */
    private static final byte ENDCHUNK[] = CRLF;

    /** 0 */
    private static final byte ZERO[] = new byte[] {(byte) '0'};

    /** 1 */
    private static final byte ONE[] = new byte[] {(byte) '1'};

    // ----------------------------------------------------- Instance Variables

    /** Has this stream been closed? */
    private boolean closed = false;

    /** The underlying output stream to which we will write data */
    private OutputStream stream = null;

    // ----------------------------------------------------------- Constructors

    /**
     * Construct an output stream wrapping the given stream.
     * The stream will not use chunking.
     *
     * @param stream wrapped output stream. Must be non-null.
     */
    public ChunkedOutputStream(OutputStream stream) {
        if (stream == null) {
            throw new NullPointerException("stream parameter is null");
        }
        this.stream = stream;
    }


    // --------------------------------------------------------- Public Methods

    /**
     * Writes a String to the client, without a carriage return
     * line feed (CRLF) character at the end. The platform default encoding is
     * used!
     *
     * @param s the String to send to the client. Must be non-null.
     * @throws IOException if an input or output exception occurred
     */
    public void print(String s) throws IOException {
        if (s == null) {
            s = "null";
        }
        write(s.getBytes());
    }

    /**
     * Writes a carriage return-line feed (CRLF) to the client.
     *
     * @throws IOException   if an input or output exception occurred
     */
    public void println() throws IOException {
        print("\r\n");
    }

    /**
     * Writes a String to the client,
     * followed by a carriage return-line feed (CRLF).
     *
     * @param s         the String to write to the client
     * @exception IOException   if an input or output exception occurred
     */
    public void println(String s) throws IOException {
        print(s);
        println();
    }

    // -------------------------------------------- OutputStream Methods

    /**
     * Write the specified byte to our output stream.
     *
     * @param b The byte to be written
     * @throws IOException if an input/output error occurs
     * @throws IllegalStateException if stream already closed
     */
    public void write (int b) throws IOException, IllegalStateException {
        if (closed) {
            throw new IllegalStateException("Output stream already closed");
        }
        //FIXME: If using chunking, the chunks are ONE byte long!
        stream.write(ONE, 0, ONE.length);
        stream.write(CRLF, 0, CRLF.length);
        stream.write(b);
        stream.write(ENDCHUNK, 0, ENDCHUNK.length);
    }

    /**
     * Write the specified byte array.
     *
     * @param b the byte array to write out
     * @param off the offset within b to start writing from
     * @param len the length of data within b to write
     * @throws IOException when errors occur writing output
     */
    public void write (byte[] b, int off, int len) throws IOException {
        System.out.println("Calling write here");
        if (closed) {
            throw new IllegalStateException("Output stream already closed");
        }
        byte chunkHeader[] = (Integer.toHexString(len) + "\r\n").getBytes();
        stream.write(chunkHeader, 0, chunkHeader.length);
        stream.write(b, off, len);
        stream.write(ENDCHUNK, 0, ENDCHUNK.length);
    }

    /**
     * Close this output stream, causing any buffered data to be flushed and
     * any further output data to throw an IOException. The underlying stream
     * is not closed!
     *
     * @throws IOException if an error occurs closing the stream
     */
    public void writeClosingChunk() throws IOException {

        if (!closed) {
            try {
                // Write the final chunk.
                stream.write(ZERO, 0, ZERO.length);
                stream.write(CRLF, 0, CRLF.length);
                stream.write(ENDCHUNK, 0, ENDCHUNK.length);
            } catch (IOException e) {
                throw e;
            } finally {
                // regardless of what happens, mark the stream as closed.
                // if there are errors closing it, there's not much we can do
                // about it
                closed = true;
            }
        }
    }

    /**
     * Flushes the underlying stream.
     * @throws IOException If an IO problem occurs.
     */
    public void flush() throws IOException {
        stream.flush();
    }

    /**
     * Close this output stream, causing any buffered data to be flushed and
     * any further output data to throw an IOException. The underlying stream
     * is not closed!
     *
     * @throws IOException if an error occurs closing the stream
     */
    public void close() throws IOException {
        writeClosingChunk();
        super.close();
    }


}
