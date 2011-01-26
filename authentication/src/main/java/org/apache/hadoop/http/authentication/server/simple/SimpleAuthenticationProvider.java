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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.authentication.AuthenticationProvider;
import org.apache.hadoop.http.authentication.AuthenticationToken;
import org.apache.hadoop.http.authentication.exception.AccessDeniedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;

public class SimpleAuthenticationProvider implements AuthenticationProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleAuthenticationProvider.class);

    private static final String REQUEST_PARAMETER_NAME = "ugi";

    public SimpleAuthenticationProvider(Configuration configuration) {
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.http.authentication.AuthenticationProvider#supports(javax.servlet.http.HttpServletRequest)
     */
    @Override
    public boolean supports(HttpServletRequest httpServletRequest) {
        final String parameter = httpServletRequest.getParameter(REQUEST_PARAMETER_NAME);

        if (parameter != null) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Received UGI parameter for request " + httpServletRequest.getRequestURL() + ": "
                        + parameter);
            }

            return true;
        }

        return false;
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.http.authentication.AuthenticationProvider#getAuthenticationToken(javax.servlet.http.HttpServletRequest)
     */
    @Override
    public AuthenticationToken getAuthenticationToken(HttpServletRequest httpServletRequest) throws IOException {
        final String parameter = httpServletRequest.getParameter(REQUEST_PARAMETER_NAME);

        if (parameter != null) {
            return new SimpleAuthenticationToken(parameter, httpServletRequest.getRemoteAddr(), null);
        }

        throw new AccessDeniedException("Received UGI in request with no value.");
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.http.authentication.AuthenticationProvider#authenticate(org.apache.hadoop.http.authentication.AuthenticationToken)
     */
    @Override
    public AuthenticationToken authenticate(AuthenticationToken authenticationToken) {
        return authenticationToken;
    }
}
