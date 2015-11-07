/*
 * Copyright (c) 2014 Ramon Servadei 
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
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * Utility methods for serialization of objects
 * 
 * @author Ramon Servadei
 */
public abstract class SerializationUtils
{
    private SerializationUtils()
    {
    }

    /**
     * Save an object to a file
     * 
     * @param object
     *            the object to save
     * @param file
     *            the file to store the object bytestream
     * @throws IOException
     */
    public static final void serializeToFile(Serializable object, File file) throws IOException
    {
        final File tmp = new File(file.getParentFile(), file.getName() + ".tmp");
        if (tmp.exists() || tmp.createNewFile())
        {
            final ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(tmp));
            try
            {
                oos.writeObject(object);
                oos.flush();
            }
            finally
            {
                oos.close();
            }
            FileUtils.move(tmp, file);
        }
        else
        {
            throw new IOException("Could not create temporary file: " + tmp);
        }
    }

    /**
     * Resolve an object from a file
     * 
     * @param file
     *            the file containing the serialized bytestream of the object
     * @return the object or <code>null</code> if the file does not exist
     * @throws FileNotFoundException
     * @throws IOException
     * @throws ClassNotFoundException
     */
    @SuppressWarnings("unchecked")
    public static final <T extends Serializable> T resolveFromFile(File file) throws FileNotFoundException, IOException,
        ClassNotFoundException
    {
        if (file.exists())
        {
            final ObjectInputStream objectInputStream = new ObjectInputStream(new FileInputStream(file));
            try
            {
                return (T) objectInputStream.readObject();
            }
            finally
            {
                objectInputStream.close();
            }
        }
        return null;
    }

    /**
     * Serialise the object to a byte[]
     */
    public static final byte[] toByteArray(Serializable object) throws IOException
    {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final ObjectOutputStream oos = new ObjectOutputStream(baos);
        try
        {
            oos.writeObject(object);
            oos.flush();
            return baos.toByteArray();
        }
        finally
        {
            oos.close();
        }
    }

    /**
     * Resolve an object from a byte[]
     */
    @SuppressWarnings("unchecked")
    public static final <T extends Serializable> T fromByteArray(byte[] byteArr) throws IOException, ClassNotFoundException
    {
        final ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(byteArr));
        try
        {
            return (T) ois.readObject();
        }
        finally
        {
            ois.close();
        }
    }
}
