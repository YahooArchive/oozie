/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License. See accompanying LICENSE file.
 */
package org.apache.hadoop.http.authentication.web;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CookieSignerVerifier {

    private static final Logger LOGGER = LoggerFactory.getLogger(CookieSignerVerifier.class.getName());
    private String keyAlgorithm = "RSA";
    private String signatureAlgorithm = "MD5withRSA";
    private KeyFactory keyFactory = null;
    private PrivateKey privateKey = null;
    private PublicKey publicKey = null;

    public CookieSignerVerifier(Configuration conf) throws Exception {
        keyFactory = KeyFactory.getInstance(keyAlgorithm);

        String publicKeyFile = conf.get("cookie.signer.public.key.file");
        String privateKeyFile = conf.get("cookie.signer.private.key.file");
        String certificateFile = conf.get("cookie.signer.certificate.file");
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Public key file: " + publicKeyFile);
            LOGGER.debug("Private key file: " + privateKeyFile);
            LOGGER.debug("Certificate file: " + certificateFile);
        }
        initializeKeys(publicKeyFile, privateKeyFile, certificateFile);
    }

    private void initializeKeys(String publicKeyFile, String privateKeyFile, String certificateFile)
            throws GeneralSecurityException, IOException {

        if (publicKeyFile == null && certificateFile == null && privateKeyFile == null) {
            LOGGER.info("Creating random public and private keys for cookie signing.");
            KeyPair keyPair = KeyPairGenerator.getInstance(keyAlgorithm).generateKeyPair();
            privateKey = keyPair.getPrivate();
            publicKey = keyPair.getPublic();
        }
        else if ((publicKeyFile != null || certificateFile != null) && privateKeyFile != null) {
            privateKey = getPrivateKey(privateKeyFile);

            if (publicKeyFile != null) {
                publicKey = getPublicKey(publicKeyFile);
            }
            else {
                Certificate cert = getCertificate(certificateFile);
                publicKey = cert.getPublicKey();
            }
        }
        else {
            throw new IllegalArgumentException("Both public and private key should be configured");
        }
    }

    public String getSignature(String data) throws GeneralSecurityException {
        Signature signer = Signature.getInstance(signatureAlgorithm);
        signer.initSign(privateKey);
        signer.update(data.getBytes());
        byte[] signature = signer.sign();
        return Base64.encodeBase64URLSafeString(signature);
    }

    public boolean verifySignature(String data, String signature) throws GeneralSecurityException {
        Signature verifier = Signature.getInstance(signatureAlgorithm);
        verifier.initVerify(publicKey);
        verifier.update(data.getBytes());
        byte[] signatureBytes = Base64.decodeBase64(signature.getBytes());
        return verifier.verify(signatureBytes);
    }

    private PublicKey getPublicKey(String fileName) throws IOException, GeneralSecurityException {
        /*
         * Generate an RSA key :
         *     openssl genrsa -rand -des3 -out rsakey.pem 1024
         * Create private key in .der format
         *     openssl pkcs8 -topk8 -nocrypt -inform PEM -in rsakey.pem -outform DER -out private.der
         * Create public key in .der format
         *     openssl rsa -inform PEM -in rsakey.pem -outform DER -out public.der -pubout
         */
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(fileName);
            byte[] keyBytes = new byte[fis.available()];
            fis.read(keyBytes);
            X509EncodedKeySpec keySpec = new X509EncodedKeySpec(keyBytes);
            return keyFactory.generatePublic(keySpec);
        }
        finally {
            IOUtils.closeStream(fis);
        }
    }

    private Certificate getCertificate(String fileName) throws FileNotFoundException, CertificateException {
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(fileName);
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            X509Certificate cert = (X509Certificate) cf.generateCertificate(fis);
            return cert;
        }
        finally {
            IOUtils.closeStream(fis);
        }
    }

    private PrivateKey getPrivateKey(String fileName) throws IOException, InvalidKeySpecException {
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(fileName);
            byte[] keyBytes = new byte[fis.available()];
            fis.read(keyBytes);
            PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(keyBytes);
            return keyFactory.generatePrivate(keySpec);
        }
        finally {
            IOUtils.closeStream(fis);
        }
    }

}
