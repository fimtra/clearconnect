/*
 * Copyright (c) 2013 Ramon Servadei
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.fimtra.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.SequenceInputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

/**
 * Utility methods for working with GZIP streams
 * 
 * @author Ramon Servadei
 * @author Paul Mackinlay
 */
public abstract class GZipUtils
{
    /**
     * Version of a {@link ByteArrayInputStream} with unsynchronized read methods
     * 
     * @author Ramon Servadei
     */
    private static final class UnsynchronizedByteArrayInputStream extends InputStream
    {
        final byte buf[];
        int pos;
        int count;

        UnsynchronizedByteArrayInputStream(byte buf[], int offset, int length)
        {
            this.buf = buf;
            this.pos = offset;
            this.count = Math.min(offset + length, buf.length);
        }

        @Override
        public int read() throws IOException
        {
            return (this.pos < this.count) ? (this.buf[this.pos++] & 0xff) : -1;
        }

        @Override
        public int read(byte b[], int off, int len)
        {
            if (this.pos >= this.count)
            {
                return -1;
            }
            if (this.pos + len > this.count)
            {
                len = this.count - this.pos;
            }
            if (len <= 0)
            {
                return 0;
            }
            System.arraycopy(this.buf, this.pos, b, off, len);
            this.pos += len;
            return len;
        }
    }

    /**
     * Version of a {@link ByteArrayOutputStream} with unsynchronized write methods
     * 
     * @author Ramon Servadei
     */
    private static final class UnsynchronizedByteArrayOutputStream extends OutputStream
    {
        byte[] buf;
        int count;

        UnsynchronizedByteArrayOutputStream(int size)
        {
            this.buf = ByteArrayPool.get(size);
        }

        @Override
        public void write(int b)
        {
            int newcount = this.count + 1;
            if (newcount > this.buf.length)
            {
                this.buf = Arrays.copyOf(this.buf, Math.max(this.buf.length << 1, newcount));
            }
            this.buf[this.count] = (byte) b;
            this.count = newcount;
        }

        @Override
        public void write(byte b[], int off, int len)
        {
            int newcount = this.count + len;
            if (newcount > this.buf.length)
            {
                this.buf = Arrays.copyOf(this.buf, Math.max(this.buf.length << 1, newcount));
            }
            System.arraycopy(b, off, this.buf, this.count, len);
            this.count = newcount;
        }
    }

    /**
     * A copy of the {@link GZIPInputStream} that is re-usable. Cannot be closed.
     * 
     * @author Ramon Servadei
     */
    private static final class ReusableGZIPInputStream extends InflaterInputStream
    {
        final CRC32 crc = new CRC32();
        final byte[] tmpbuf = new byte[128];
        boolean eos;

        public ReusableGZIPInputStream(InputStream in)
        {
            super(in, new Inflater(true), 512);
        }

        void reset(InputStream in) throws IOException
        {
            this.inf.reset();
            this.crc.reset();
            this.eos = false;

            this.in = in;
            readHeader(in);
        }

        @Override
        public int read(byte[] buf, int off, int len) throws IOException
        {
            if (this.eos)
            {
                return -1;
            }

            int n = super.read(buf, off, len);
            if (n == -1)
            {
                if (readTrailer())
                {
                    this.eos = true;
                }
                else
                {
                    return this.read(buf, off, len);
                }
            }
            else
            {
                this.crc.update(buf, off, n);
            }
            return n;
        }

        @Override
        public void close() throws IOException
        {
            // NOTE: unclosable
        }

        public final static int GZIP_MAGIC = 0x8b1f;
        private final static int FTEXT = 1; // Extra text
        private final static int FHCRC = 2; // Header CRC
        private final static int FEXTRA = 4; // Extra field
        private final static int FNAME = 8; // File name
        private final static int FCOMMENT = 16; // File comment

        private int readHeader(InputStream this_in) throws IOException
        {
            CheckedInputStream in = new CheckedInputStream(this_in, this.crc);

            this.crc.reset();
            // Check header magic
            if (readUShort(in) != GZIP_MAGIC)
            {
                throw new IOException("Not in GZIP format");
            }
            // Check compression method
            if (readUByte(in) != 8)
            {
                throw new IOException("Unsupported compression method");
            }
            // Read flags
            int flg = readUByte(in);
            // Skip MTIME, XFL, and OS fields
            skipBytes(in, 6);
            int n = 2 + 2 + 6;

            // Skip optional extra field
            if ((flg & FEXTRA) == FEXTRA)
            {
                int m = readUShort(in);
                skipBytes(in, m);
                n += m + 2;
            }
            // Skip optional file name
            if ((flg & FNAME) == FNAME)
            {
                do
                {
                    n++;
                }
                while (readUByte(in) != 0);
            }
            // Skip optional file comment
            if ((flg & FCOMMENT) == FCOMMENT)
            {
                do
                {
                    n++;
                }
                while (readUByte(in) != 0);
            }
            // Check optional header CRC
            if ((flg & FHCRC) == FHCRC)
            {
                int v = (int) this.crc.getValue() & 0xffff;
                if (readUShort(in) != v)
                {
                    throw new IOException("Corrupt GZIP header");
                }
                n += 2;
            }

            this.crc.reset();
            return n;
        }

        private boolean readTrailer() throws IOException
        {
            InputStream in = this.in;
            int n = this.inf.getRemaining();
            if (n > 0)
            {
                in = new SequenceInputStream(new ByteArrayInputStream(this.buf, this.len - n, n), in);
            }
            // Uses left-to-right evaluation order
            if ((readUInt(in) != this.crc.getValue()) ||
            // rfc1952; ISIZE is the input size modulo 2^32
                (readUInt(in) != (this.inf.getBytesWritten() & 0xffffffffL)))
                throw new IOException("Corrupt GZIP trailer");

            // If there are more bytes available in "in" or
            // the leftover in the "inf" is > 26 bytes:
            // this.trailer(8) + next.header.min(10) + next.trailer(8)
            // try concatenated case
            if (this.in.available() > 0 || n > 26)
            {
                int m = 8; // this.trailer
                try
                {
                    m += readHeader(in); // next.header
                }
                catch (IOException ze)
                {
                    return true; // ignore any malformed, do nothing
                }

                this.inf.reset();
                if (n > m)
                    this.inf.setInput(this.buf, this.len - n + m, n - m);
                return false;
            }

            return true;
        }

        private long readUInt(InputStream in) throws IOException
        {
            long s = readUShort(in);
            return ((long) readUShort(in) << 16) | s;
        }

        private int readUShort(InputStream in) throws IOException
        {
            int b = readUByte(in);
            return (readUByte(in) << 8) | b;
        }

        private int readUByte(InputStream in) throws IOException
        {
            int b = in.read();
            if (b == -1)
            {
                throw new EOFException();
            }
            if (b < -1 || b > 255)
            {
                // Report on this.in, not argument in; see read{Header, Trailer}.
                throw new IOException(
                    this.in.getClass().getName() + ".read() returned value out of range -1..255: " + b);
            }
            return b;
        }

        private void skipBytes(InputStream in, int n) throws IOException
        {
            while (n > 0)
            {
                int len = in.read(this.tmpbuf, 0, n < this.tmpbuf.length ? n : this.tmpbuf.length);
                if (len == -1)
                {
                    throw new EOFException();
                }
                n -= len;
            }
        }
    }

    /**
     * A copy of the {@link GZIPOutputStream} that has unsynchronized write method and is re-usable.
     * Cannot be closed.
     * 
     * @author Ramon Servadei
     */
    private static final class ReusableGZIPOutputStream extends DeflaterOutputStream
    {
        private final static int GZIP_MAGIC = 0x8b1f;
        private final static int TRAILER_SIZE = 8;

        final CRC32 crc = new CRC32();

        public ReusableGZIPOutputStream(OutputStream out, Deflater deflater)
        {
            super(out, deflater, 512);
        }

        void reset(OutputStream out) throws IOException
        {
            super.def.reset();
            this.crc.reset();

            super.out = out;
            writeHeader();
            this.crc.reset();
        }

        /**
         * Writes array of bytes to the compressed output stream. This method will block until all
         * the bytes are written.
         * 
         * @param buf
         *            the data to be written
         * @param off
         *            the start offset of the data
         * @param len
         *            the length of the data
         * @exception IOException
         *                If an I/O error has occurred.
         */
        @Override
        public void write(byte[] b, int off, int len) throws IOException
        {
            if (len == 0)
            {
                return;
            }
            if (!this.def.finished())
            {
                this.def.setInput(b, off, len);
                while (!this.def.needsInput())
                {
                    deflate();
                }
            }
            this.crc.update(b, off, len);
        }

        /**
         * Finishes writing compressed data to the output stream without closing the underlying
         * stream. Use this method when applying multiple filters in succession to the same output
         * stream.
         * 
         * @exception IOException
         *                if an I/O error has occurred
         */
        @Override
        public void finish() throws IOException
        {
            if (!this.def.finished())
            {
                this.def.finish();
                while (!this.def.finished())
                {
                    int len = this.def.deflate(this.buf, 0, this.buf.length);
                    if (this.def.finished() && len <= this.buf.length - TRAILER_SIZE)
                    {
                        // last deflater buffer. Fit trailer at the end
                        writeTrailer(this.buf, len);
                        len = len + TRAILER_SIZE;
                        this.out.write(this.buf, 0, len);
                        return;
                    }
                    if (len > 0)
                        this.out.write(this.buf, 0, len);
                }
                // if we can't fit the trailer at the end of the last
                // deflater buffer, we write it separately
                byte[] trailer = new byte[TRAILER_SIZE];
                writeTrailer(trailer, 0);
                this.out.write(trailer);
            }
        }

        private final static byte[] header = { (byte) GZIP_MAGIC, // Magic number (short)
            (byte) (GZIP_MAGIC >> 8), // Magic number (short)
            Deflater.DEFLATED, // Compression method (CM)
            0, // Flags (FLG)
            0, // Modification time MTIME (int)
            0, // Modification time MTIME (int)
            0, // Modification time MTIME (int)
            0, // Modification time MTIME (int)
            0, // Extra flags (XFLG)
            0 // Operating system (OS)
        };

        private void writeHeader() throws IOException
        {
            this.out.write(header);
        }

        private void writeTrailer(byte[] buf, int offset)
        {
            writeInt((int) this.crc.getValue(), buf, offset); // CRC-32 of uncompr. data
            writeInt(this.def.getTotalIn(), buf, offset + 4); // Number of uncompr. bytes
        }

        private static void writeInt(int i, byte[] buf, int offset)
        {
            writeShort(i & 0xffff, buf, offset);
            writeShort((i >> 16) & 0xffff, buf, offset + 2);
        }

        private static void writeShort(int s, byte[] buf, int offset)
        {
            buf[offset] = (byte) (s & 0xff);
            buf[offset + 1] = (byte) ((s >> 8) & 0xff);
        }

        @Override
        public void close() throws IOException
        {
            // NOTE: unclosable
        }
    }

    static final ThreadLocal<ReusableGZIPOutputStream> DEFLATER = new ThreadLocal<ReusableGZIPOutputStream>()
    {
        @Override
        protected ReusableGZIPOutputStream initialValue()
        {
            return new ReusableGZIPOutputStream(new ByteArrayOutputStream(),
                new Deflater(Deflater.DEFAULT_COMPRESSION, true));
        }
    };
    static final ThreadLocal<ReusableGZIPInputStream> INFLATER = new ThreadLocal<ReusableGZIPInputStream>()
    {
        @Override
        protected ReusableGZIPInputStream initialValue()
        {
            return new ReusableGZIPInputStream(new ByteArrayInputStream(new byte[1]));
        }
    };

    /**
     * Compress a byte[] with a {@link ReusableGZIPOutputStream}
     * 
     * @return the compressed byte[], <code>null</code> if there was a problem
     */
    public static final byte[] compress(final byte[] uncompressedData)
    {
        try
        {
            final UnsynchronizedByteArrayOutputStream outStream =
                new UnsynchronizedByteArrayOutputStream(uncompressedData.length);
            final ReusableGZIPOutputStream gZipOut = DEFLATER.get();
            gZipOut.reset(outStream);
            gZipOut.write(uncompressedData);
            gZipOut.finish();
            // NOTE: this stream is never closed

            final int size = outStream.count;
            final byte[] compressedData = new byte[size + 4];
            compressedData[0] = (byte) (uncompressedData.length >> 24);
            compressedData[1] = (byte) (uncompressedData.length >> 16);
            compressedData[2] = (byte) (uncompressedData.length >> 8);
            compressedData[3] = (byte) (uncompressedData.length);
            System.arraycopy(outStream.buf, 0, compressedData, 4, size);
            return compressedData;
        }
        catch (IOException e)
        {
            Log.log(GZipUtils.class, "Could not compress data", e);
            return null;
        }
    }

    /**
     * Unzip the data in the byte[] that was compressed using {@link #compress(byte[])}
     * 
     * @return the uncompressed data, <code>null</code> if there was a problem
     */
    @Deprecated
    public static final byte[] uncompress(byte[] compressedData)
    {
        try
        {
            final int uncompressedSize = ByteBuffer.wrap(compressedData).getInt();
            final byte[] uncompressedData = new byte[uncompressedSize];
            final ByteBuffer buffer = ByteBuffer.wrap(uncompressedData);
            final ReusableGZIPInputStream gZipIn = INFLATER.get();
            gZipIn.reset(new UnsynchronizedByteArrayInputStream(compressedData, 4, compressedData.length - 4));
            int len = 0;
            while ((len = uncompressedData.length - buffer.position()) > 0)
            {
                buffer.position(buffer.position() + gZipIn.read(uncompressedData, buffer.position(), len));
            }
            return uncompressedData;
        }
        catch (IOException e)
        {
            Log.log(GZipUtils.class, "Could not uncompress data", e);
            return null;
        }
    }

    /**
     * Unzip the data in the {@link ByteBuffer} that was compressed using {@link #compress(byte[])}
     * 
     * @see ReusableGZIPInputStream
     * @return the uncompressed data, <code>null</code> if there was a problem
     */
    public static final ByteBuffer uncompress(ByteBuffer compressedData)
    {
        try
        {
            final int uncompressedSize = compressedData.getInt();
            final byte[] uncompressedData = ByteArrayPool.get(uncompressedSize);
            final ByteBuffer buffer = ByteBuffer.wrap(uncompressedData);
            final ReusableGZIPInputStream gZipIn = INFLATER.get();
            gZipIn.reset(new UnsynchronizedByteArrayInputStream(compressedData.array(), compressedData.position(),
                compressedData.limit() - compressedData.position()));
            int len = 0;
            while ((len = uncompressedSize - buffer.position()) > 0)
            {
                buffer.position(buffer.position() + gZipIn.read(uncompressedData, buffer.position(), len));
            }
            buffer.flip();
            return buffer;
        }
        catch (IOException e)
        {
            Log.log(GZipUtils.class, "Could not uncompress data", e);
            return null;
        }
    }

    /**
     * Compresses the data in the inputStream to the outputStream.
     */
    public static void compressInputToOutput(InputStream inputStream, OutputStream outputStream) throws IOException
    {
        final GZIPOutputStream gzipOutputStream = new GZIPOutputStream(outputStream);
        final byte[] buffer = ByteArrayPool.get(1024);
        try
        {
            int length;
            while ((length = inputStream.read(buffer)) != -1)
            {
                gzipOutputStream.write(buffer, 0, length);
            }
        }
        finally
        {
            gzipOutputStream.close();
            ByteArrayPool.offer(buffer);
        }
    }
}