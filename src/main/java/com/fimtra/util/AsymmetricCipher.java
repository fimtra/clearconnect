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
import java.security.Key;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;

/**
 * This allows encryption and decryption of data between two "end points" using asymmetric
 * encryption. Each end point has an {@link AsymmetricCipher} instance. Each cipher decrypts using
 * its own private key and encrypts using the other instance's public key. In this way an encrypted
 * data exchange can be performed using the two instances as ends of a communication channel.
 * <p>
 * 
 * @see https://docs.oracle.com/javase/8/docs/technotes/guides/security/StandardNames.html#Cipher
 * 
 * @author Ramon Servadei
 */
public final class AsymmetricCipher
{
    public static final String ALGORITHM_RSA = "RSA";
    public static final String TRANSFORMATION = "RSA/ECB/OAEPWithSHA-256AndMGF1Padding";
    
    private final Key pubKey;
    
    final int keySize;
    final KeyPair keyPair;
    
    Cipher decryptCipher;
    Cipher encryptCipher;
    

    /**
     * Construct with a random generated 2048-bit RSA key
     */
    public AsymmetricCipher() throws NoSuchAlgorithmException
    {
        this(ALGORITHM_RSA, 2048);
    }

    /**
     * Construct the cipher with the given key algorithm, transformation and key size
     * 
     * @param keyAlgorithm
     *            the algorithm for the key, e.g. "RSA"
     * @param keySize
     *            the bit size for the key
     */
    public AsymmetricCipher(String keyAlgorithm, int keySize)
        throws NoSuchAlgorithmException
    {
        this.keySize = keySize;
        
        final KeyPairGenerator generator = KeyPairGenerator.getInstance(keyAlgorithm);
        generator.initialize(keySize);
        this.keyPair = generator.generateKeyPair();
        this.pubKey = this.keyPair.getPublic();
    }

    public void setTransformation(String transformation) throws NoSuchPaddingException, InvalidKeyException, NoSuchAlgorithmException
    {
        this.encryptCipher = Cipher.getInstance(transformation);
        
        this.decryptCipher = Cipher.getInstance(transformation);
        Key privateKey = this.keyPair.getPrivate();
        this.decryptCipher.init(Cipher.DECRYPT_MODE, privateKey);
    }
    
    /**
     * Set the encryption key to use. This is the public key of another {@link AsymmetricCipher}
     * instance.
     * 
     * @param encryptKey
     * @throws InvalidKeyException
     */
    public void setEncryptionKey(Key encryptKey) throws InvalidKeyException
    {
        this.encryptCipher.init(Cipher.ENCRYPT_MODE, encryptKey);
    }

    /**
     * @return the public key that should be used by another {@link AsymmetricCipher} instance to
     *         setup its encryption key
     * @see #setEncryptionKey(Key)
     */
    public Key getPubKey()
    {
        return this.pubKey;
    }

    /**
     * @param data
     *            the data to encrypt
     * @return the encrypted data
     * @throws RuntimeException
     *             if there is a problem encrypting
     */
    public byte[] encrypt(byte[] data)
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
    public byte[] decrypt(byte[] data)
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

    /**
     * @return the key size
     */
    public int getKeySize()
    {
        return this.keySize;
    }
}
