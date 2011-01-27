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
package org.apache.hadoop.http.authentication.server.simple;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.authentication.AuthenticationProvider;
import org.apache.hadoop.http.authentication.AuthenticationToken;
import org.apache.hadoop.http.authentication.client.simple.SimpleAuthenticator;
import org.apache.hadoop.http.authentication.exception.AccessDeniedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provider class for simple authentication.
 */
public class SimpleAuthenticationHeaderProvider implements AuthenticationProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleAuthenticationHeaderProvider.class);

    public SimpleAuthenticationHeaderProvider(Configuration configuration) {
    }

    /**
     * Check if parameter "ugi" exists in http request header. True if it exists.
     *
     * @see org.apache.hadoop.http.authentication.AuthenticationProvider#supports(javax.servlet.http.HttpServletRequest)
     */
    @Override
    public boolean supports(HttpServletRequest httpServletRequest) {
        final String header = httpServletRequest.getHeader(SimpleAuthenticator.REQUEST_PARAMETER_NAME);

        if (header != null) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER
                        .debug("Received UGI parameter for request " + httpServletRequest.getRequestURL() + ": "
                                + header);
            }

            return true;
        }

        return false;
    }

    /**
     * Get parameter "ugi" and initialize <code>SimpleAuthenticationToken</code>.
     *
     * @see org.apache.hadoop.http.authentication.AuthenticationProvider#getAuthenticationToken(javax.servlet.http.HttpServletRequest)
     */
    @Override
    public AuthenticationToken getAuthenticationToken(HttpServletRequest httpServletRequest) throws IOException {
        final String header = httpServletRequest.getHeader(SimpleAuthenticator.REQUEST_PARAMETER_NAME);

        if (header != null) {
            return new SimpleAuthenticationToken(header, httpServletRequest.getRemoteAddr(), null);
        }

        throw new AccessDeniedException("Received UGI in request with no value.");
    }

    /**
     * Authenticate token for simple authentication. It is no-op in <code>SimpleAuthenticationHeaderProvider</code>.
     *
     * @see org.apache.hadoop.http.authentication.AuthenticationProvider#authenticate(org.apache.hadoop.http.authentication.AuthenticationToken)
     */
    @Override
    public AuthenticationToken authenticate(AuthenticationToken authenticationToken) {
        return authenticationToken;
    }
}
