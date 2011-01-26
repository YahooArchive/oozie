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
package org.apache.hadoop.http.authentication.client;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.servlet.http.Cookie;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class HttpAuthenticator {

    private static final String HADOOP_HTTP_AUTH = "Hadoop-HTTP-Auth";
    public static final String CONF_FOR_HTTP_RENEW_AUTHENTICATION = "http.renew.authentication";
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpAuthenticator.class.getName());
    protected Cookie authCookie = null;
    private long cookieExpiryTime = 0L;

    private static final ThreadLocal<SimpleDateFormat> cookieExpiresFormat = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("E, dd-MMM-yyyy k:m:s z");
        }
    };

    /**
     * To authenticate a client request for http connection
     *
     * @param conf the configuration for authentication
     * @param connection the connection to authenticate
     * @throws IOException
     */
    public abstract void authenticate(Map<String, String> conf, HttpURLConnection connection) throws IOException;

    /**
     * Read cookie from http response
     *
     * @param connection
     */
    public void setCookieFromResponse(HttpURLConnection connection) {
        Map<String, List<String>> headers = connection.getHeaderFields();
        List<String> cookieHeaders = headers.get("Set-Cookie");
        try {
            if (cookieHeaders != null) {
                for (String cookieString : cookieHeaders) {
                    String[] attributes = cookieString.split(";");
                    String nameValue = attributes[0];
                    int equals = nameValue.indexOf('=');
                    String cookieName = nameValue.substring(0, equals);
                    if (cookieName.equals(HADOOP_HTTP_AUTH)) {
                        String cookieValue = nameValue.substring(equals + 1);

                        for (int i = 1; i < attributes.length; i++) {
                            nameValue = attributes[i].trim();
                            if ((equals = nameValue.indexOf('=')) == -1)
                                continue;
                            String attributeName = nameValue.substring(0, equals);
                            String attributeValue = nameValue.substring(equals + 1);
                            if (attributeName.equalsIgnoreCase("expires")) {
                                long expiryTime = cookieExpiresFormat.get().parse(attributeValue).getTime();
                                if (expiryTime < cookieExpiryTime) {
                                    clearCookie();
                                }
                                else {
                                    authCookie = new Cookie(cookieName, cookieValue);
                                    cookieExpiryTime = expiryTime;
                                }
                                break;
                            }
                        }
                    }
                }
            }
        }
        catch (Exception e) {
            LOGGER.warn("Failed to read cookie from response", e);
        }
    }

    /**
     * Clear the cookie
     */
    public void clearCookie() {
        authCookie = null;
        cookieExpiryTime = 0L;
    }

    /**
     * Set cookie to http request
     *
     * @param connection
     * @return true if cookie is added to http request
     */
    protected boolean setCookieInRequest(HttpURLConnection connection) {
        if (authCookie == null || hasCookieExpired())
            return false;
        connection.addRequestProperty("Cookie", authCookie.getName() + "=" + authCookie.getValue());
        return true;
    }

    /**
     * Check if cookie is expired
     *
     * @return true if the Cookie has expired or 10 mins to expire
     */
    private boolean hasCookieExpired() {
        if ((cookieExpiryTime - System.currentTimeMillis()) <= TimeUnit.MINUTES.toMillis(10)) {
            authCookie = null;
            cookieExpiryTime = 0L;
            return true;
        }
        return false;
    }

    /**
     * Return boolean
     *
     * @param value
     * @return true if String "true" is given
     */
    protected boolean getBooleanValue(String value) {
        return value != null && value.equals("true");
    }
}
