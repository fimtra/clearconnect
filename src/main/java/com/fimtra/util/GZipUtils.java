/*
 * Copyright (c) 2013 Ramon Servadei 
 *  
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Utility methods for working with GZIP streams
 * 
 * @author Ramon Servadei
 */
public abstract class GZipUtils
{
    /**
     * Provides access to the internal byte[]
     * 
     * @author Ramon Servadei
     */
    private static final class ByteArrayOutputStreamExtension extends ByteArrayOutputStream
    {
        ByteArrayOutputStreamExtension(int size)
        {
            super(size);
        }

        byte[] getBuffer()
        {
            return this.buf;
        }

        int getCount()
        {
            return this.count;
        }
    }

    /**
     * Compress a byte[] with a {@link GZIPOutputStream}
     * 
     * @return the compressed byte[], <code>null</code> if there was a problem
     */
    public static final byte[] compress(byte[] uncompressedData)
    {
        try
        {
            final ByteArrayOutputStreamExtension outStream =
                new ByteArrayOutputStreamExtension(uncompressedData.length);
            GZIPOutputStream gZipOut = new GZIPOutputStream(outStream);
            gZipOut.write(uncompressedData);
            gZipOut.close();
            byte[] compressedData = new byte[outStream.getCount() + 4];
            ByteBuffer.wrap(compressedData).putInt(uncompressedData.length);
            System.arraycopy(outStream.getBuffer(), 0, compressedData, 4, outStream.getCount());
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
     * @see GZIPInputStream
     * @return the uncompressed data, <code>null</code> if there was a problem
     */
    public static final byte[] uncompress(byte[] compressedData)
    {
        try
        {
            final int uncompressedSize = ByteBuffer.wrap(compressedData).getInt();
            byte[] uncompressedData = new byte[uncompressedSize];
            ByteBuffer buffer = ByteBuffer.wrap(uncompressedData);
            ByteArrayInputStream inStream = new ByteArrayInputStream(compressedData, 4, compressedData.length - 4);
            byte[] tempBuf = new byte[uncompressedSize > 1024 ? uncompressedSize >> 1 : uncompressedSize];
            GZIPInputStream gZipIn = new GZIPInputStream(inStream);
            try
            {
                int count = 0;
                while ((count = gZipIn.read(tempBuf)) != -1)
                {
                    System.arraycopy(tempBuf, 0, buffer.array(), buffer.position(), count);
                    buffer.position(buffer.position() + count);
                }
                return uncompressedData;
            } finally
            {
               gZipIn.close(); 
            }
        }
        catch (IOException e)
        {
            Log.log(GZipUtils.class, "Could not uncompress data", e);
            return null;
        }
    }
}
