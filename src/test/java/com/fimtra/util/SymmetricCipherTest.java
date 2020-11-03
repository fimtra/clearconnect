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

import static org.junit.Assert.assertEquals;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;

import org.junit.Before;
import org.junit.Test;

/**
 * Tests for the {@link SymmetricCipher}
 * 
 * @author Ramon Servadei
 */
public class SymmetricCipherTest
{
    @Before
    public void setUp() throws Exception
    {
    }

    @Test
    public void test() throws InvalidKeyException, NoSuchAlgorithmException, NoSuchPaddingException
    {
        final SecretKey key = SymmetricCipher.generate128BitKey(SymmetricCipher.ALGORITHM_AES);
        final SymmetricCipher c1 = new SymmetricCipher(SymmetricCipher.ALGORITHM_AES, key);
        final SymmetricCipher c2 = new SymmetricCipher(SymmetricCipher.ALGORITHM_AES, key);

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 30000; i++)
        {
            sb.append("hello").append(i);
        }
        final String expected = sb.toString();
        final byte[] encrypt = c1.encrypt(expected.getBytes());
        assertEquals(expected, new String(c2.decrypt(encrypt)));
    }

}
