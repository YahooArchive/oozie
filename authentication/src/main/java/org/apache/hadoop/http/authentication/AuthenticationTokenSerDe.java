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
package org.apache.hadoop.http.authentication;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.http.authentication.exception.AccessDeniedException;
import org.apache.hadoop.http.authentication.web.CookieSignerVerifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.GeneralSecurityException;
import java.util.concurrent.TimeUnit;

public class AuthenticationTokenSerDe {

    private static final Logger LOGGER = LoggerFactory.getLogger(AuthenticationTokenSerDe.class);
    private static final String DELIMITER = ";";
    private static final String SIGN_KEY_STRING = "sign=";
    public static final long EXPIRATION_IN_MILLIS = TimeUnit.HOURS.toMillis(4);
    public static final long EXPIRATION_IN_SECONDS = TimeUnit.HOURS.toSeconds(4);

    private AuthenticationTokenSerDe() {
    }

    public static String serialize(AuthenticationToken authenticationToken, CookieSignerVerifier cookieSigner) {
        StringBuilder buffer = new StringBuilder();
        buffer.append(authenticationToken.getAuthenticationMethod()).append(DELIMITER);
        buffer.append(authenticationToken.getPrincipal()).append(DELIMITER);
        buffer.append(authenticationToken.getRemoteAddr()).append(DELIMITER);
        buffer.append(System.currentTimeMillis() + EXPIRATION_IN_MILLIS);

        try {
            final String token = buffer.toString();
            String signature = cookieSigner.getSignature(token);
            String serializedToken = Base64.encodeBase64URLSafeString(token.getBytes()) + SIGN_KEY_STRING + signature;
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Created cookie: " + serializedToken);
            }
            return serializedToken;
        }
        catch (GeneralSecurityException e) {
            throw new IllegalStateException("Error while trying to sign the cookie", e);
        }
    }

    public static AuthenticationToken deserialize(String serializedToken, String remoteAddr,
            CookieSignerVerifier cookieVerifier) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Received cookie for deserialize: " + serializedToken);
        }
        int index = serializedToken.lastIndexOf(SIGN_KEY_STRING);
        if (index == -1)
            throw new AccessDeniedException("Authentication cookie received does not contain signature");
        String signature = serializedToken.substring(index + SIGN_KEY_STRING.length());
        String data = serializedToken.substring(0, index);
        String decodedData = new String(Base64.decodeBase64(data));

        try {
            if (!cookieVerifier.verifySignature(decodedData, signature))
                throw new AccessDeniedException("Authentication cookie received failed signature verification");
        }
        catch (GeneralSecurityException e) {
            throw new AccessDeniedException("Error while trying to verify the authentication cookie received", e);
        }

        String[] tokens = decodedData.split(DELIMITER);
        if (tokens.length != 4) {
            throw new AccessDeniedException("Authentication cookie received is invalid");
        }

        CookieBasedAuthenticationToken cookieToken = new CookieBasedAuthenticationToken(tokens[0], tokens[1], tokens[2]);
        checkTokenForExpiry(tokens[3], cookieToken);
        checkTokenForRemoteAddr(cookieToken, remoteAddr);
        return cookieToken;
    }

    private static void checkTokenForExpiry(String expirationTimeInMillisStr, CookieBasedAuthenticationToken cookieToken) {
        final long expirationTimeInMillis = Long.valueOf(expirationTimeInMillisStr);
        final boolean isExpired = expirationTimeInMillis < System.currentTimeMillis();

        if (isExpired) {
            String msg = "Received expired authentication token";
            LOGGER.error(msg + ". Cookie info: " + cookieToken.toString());
            throw new AccessDeniedException(msg);
        }
    }

    private static void checkTokenForRemoteAddr(CookieBasedAuthenticationToken cookieToken, String remoteAddr) {
        if (!remoteAddr.equals(cookieToken.getRemoteAddr())) {
            String msg = "Authentication cookie received was issued to " + cookieToken.getRemoteAddr()
                    + ". But the cookie was received from " + remoteAddr;
            LOGGER.error(msg + ". Cookie info: " + cookieToken.toString());
            throw new AccessDeniedException(msg);
        }
    }

    public static class CookieBasedAuthenticationToken extends AbstractAuthenticationToken {

        private final String initialAuthMethod;

        CookieBasedAuthenticationToken(String initialAuthMethod, String principal, String remoteAddr) {
            super(principal, remoteAddr, null);
            super.setAuthenticated(true);
            this.initialAuthMethod = initialAuthMethod;
        }

        @Override
        public String getAuthenticationMethod() {
            return "Cookie";
        }

        public String getInitialAuthMethod() {
            return initialAuthMethod;
        }

        @Override
        public String toString() {
            return "User = " + principal + ",RemoteIP = " + remoteAddr + ",InitalAuthMethod = " + initialAuthMethod;
        }
    }
}
