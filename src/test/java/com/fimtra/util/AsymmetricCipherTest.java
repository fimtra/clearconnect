/*
 * Copyright (c) 2016 Ramon Servadei 
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

import static org.junit.Assert.*;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;

import org.junit.Before;
import org.junit.Test;

/**
 * Tests for the {@link AsymmetricCipher}
 * 
 * @author Ramon Servadei
 */
public class AsymmetricCipherTest
{
    @Before
    public void setUp() throws Exception
    {
    }

    @Test
    public void testEncryptDecrypt() throws InvalidKeyException, NoSuchAlgorithmException, NoSuchPaddingException
    {
        final AsymmetricCipher c1 = new AsymmetricCipher();
        final AsymmetricCipher c2 = new AsymmetricCipher();
        c1.setTransformation(AsymmetricCipher.TRANSFORMATION);
        c2.setTransformation(AsymmetricCipher.TRANSFORMATION);
        c1.setEncryptionKey(c2.getPubKey());
        c2.setEncryptionKey(c1.getPubKey());

        final String expected = "hello!";
        final byte[] encrypt = c1.encrypt(expected.getBytes());
        assertEquals(expected, new String(c2.decrypt(encrypt)));
    }

    @Test
    public void testEncryptDecryptOfSymmetricKey() throws InvalidKeyException, NoSuchAlgorithmException,
        NoSuchPaddingException, IOException, ClassNotFoundException
    {
        final AsymmetricCipher c1 = new AsymmetricCipher(AsymmetricCipher.ALGORITHM_RSA, 1024);
        final AsymmetricCipher c2 = new AsymmetricCipher(AsymmetricCipher.ALGORITHM_RSA, 2048);
        c1.setTransformation(AsymmetricCipher.TRANSFORMATION);
        c2.setTransformation(AsymmetricCipher.TRANSFORMATION);
        c1.setEncryptionKey(c2.getPubKey());
        c2.setEncryptionKey(c1.getPubKey());

        // generate a symmetric key to send 
        KeyGenerator generator = KeyGenerator.getInstance("AES");
        generator.init(new SecureRandom());
        generator.init(128);
        final SecretKey generateKey = generator.generateKey();

        final byte[] encrypt = c1.encrypt(SerializationUtils.toByteArray(generateKey));
        assertEquals(generateKey, SerializationUtils.fromByteArray(c2.decrypt(encrypt)));
    }

}
