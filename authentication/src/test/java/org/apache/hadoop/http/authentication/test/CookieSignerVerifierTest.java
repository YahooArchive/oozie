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
package org.apache.hadoop.http.authentication.test;

import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.authentication.web.CookieSignerVerifier;
import org.testng.Assert;
import org.testng.annotations.Test;

public class CookieSignerVerifierTest {

    private static final String text = "Plain text 24xfse@%#&#$%$=";
    private static final String sameLengthText = "Plain text 23xfse@%#&#$%$=";

    @Test
    public void testCookieSigningWithRandomGeneratedKey() throws Exception {
        Configuration conf = new Configuration(false);
        CookieSignerVerifier cookieSignerVerifier = new CookieSignerVerifier(conf);
        String signature = cookieSignerVerifier.getSignature(text);
        Assert.assertTrue(cookieSignerVerifier.verifySignature(text, signature));
        Assert.assertFalse(cookieSignerVerifier.verifySignature(text + ",", signature));
        Assert.assertFalse(cookieSignerVerifier.verifySignature(sameLengthText + ",", signature));
    }

    @Test
    public void testCookieSigningWithPublicAndPrivateKeyFromFile() throws Exception {
        Configuration conf = new Configuration(false);
        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        URL publicKey = classLoader.getResource("TestKeyPair1_public.der");
        URL privateKey = classLoader.getResource("TestKeyPair1_private.der");
        conf.set("cookie.signer.private.key.file", privateKey.getFile());
        conf.set("cookie.signer.public.key.file", publicKey.getFile());
        CookieSignerVerifier cookieSignerVerifier = new CookieSignerVerifier(conf);
        String signature = cookieSignerVerifier.getSignature(text);
        Assert.assertTrue(cookieSignerVerifier.verifySignature(text, signature));
        Assert.assertFalse(cookieSignerVerifier.verifySignature(text + ",", signature));
        Assert.assertFalse(cookieSignerVerifier.verifySignature(sameLengthText + ",", signature));
    }

    @Test
    public void testCookieSigningWithPublicKeyInCertificate() throws Exception {
        Configuration conf = new Configuration(false);
        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        URL certifcate = classLoader.getResource("TestKeyPair2_cert.der");
        URL privateKey = classLoader.getResource("TestKeyPair2_private.der");
        conf.set("cookie.signer.certificate.file", certifcate.getFile());
        conf.set("cookie.signer.private.key.file", privateKey.getFile());
        CookieSignerVerifier cookieSignerVerifier = new CookieSignerVerifier(conf);
        String signature = cookieSignerVerifier.getSignature(text);
        Assert.assertTrue(cookieSignerVerifier.verifySignature(text, signature));
        Assert.assertFalse(cookieSignerVerifier.verifySignature(text + ",", signature));
        Assert.assertFalse(cookieSignerVerifier.verifySignature(sameLengthText + ",", signature));
    }

}
