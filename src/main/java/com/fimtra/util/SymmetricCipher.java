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

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;

/**
 * This allows encryption and decryption of data between two "end points" using a symmetrical
 * encryption mechanism.
 * <p>
 * Thread-safe.
 * 
 * @author Ramon Servadei
 */
public final class SymmetricCipher
{
    public static final String ALGORITHM_AES = "AES";

    /**
     * Generate a {@link SecureRandom} 128bit key for the passed in algorithm.
     * 
     * @param algorithm
     *            the key algorithm, e.g. "AES"
     * @return the generated key
     */
    public static SecretKey generate128BitKey(String algorithm) throws NoSuchAlgorithmException
    {
        KeyGenerator generator = KeyGenerator.getInstance(algorithm);
        generator.init(new SecureRandom());
        generator.init(128);
        return generator.generateKey();
    }

    final Cipher decryptCipher;
    final Cipher encryptCipher;

    /**
     * Create an instance using the desired key algorithm (the transformation) and the secret key
     * 
     * @param transformation
     *            see {@link Cipher#getInstance(String)}
     * @param key
     *            the key
     */
    public SymmetricCipher(String transformation, SecretKey key)
        throws NoSuchAlgorithmException, NoSuchPaddingException, InvalidKeyException
    {
        this.encryptCipher = Cipher.getInstance(transformation);
        this.encryptCipher.init(Cipher.ENCRYPT_MODE, key);

        this.decryptCipher = Cipher.getInstance(transformation);
        this.decryptCipher.init(Cipher.DECRYPT_MODE, key);
    }

    /**
     * @param data
     *            the data to encrypt
     * @return the encrypted data
     * @throws RuntimeException
     *             if there is a problem encrypting
     */
    public synchronized byte[] encrypt(byte[] data)
    {
        try
        {
            return this.encryptCipher.doFinal(data);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * @param data
     *            the data to decrypt
     * @return the decripted data
     * @throws RuntimeException
     *             if there is a problem decrypting
     */
    public synchronized byte[] decrypt(byte[] data)
    {
        try
        {
            return this.decryptCipher.doFinal(data);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }
}
