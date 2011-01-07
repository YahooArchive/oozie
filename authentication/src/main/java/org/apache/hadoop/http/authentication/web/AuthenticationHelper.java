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

import org.apache.hadoop.http.authentication.AuthenticationToken;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;

import java.io.IOException;
import java.security.Principal;

final class AuthenticationHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(AuthenticationHelper.class.getName());

    private AuthenticationHelper() {
    }

    static void setupUGI(ProxyUGICacheManager ugiManager, HttpServletRequest httpServletRequest,
            AuthenticationToken authenticatedToken) throws IOException {
        UserGroupInformation proxyUGI = ugiManager.getUGI(authenticatedToken.getPrincipal(), httpServletRequest);
        httpServletRequest.setAttribute("authorized.ugi", proxyUGI);
        LOGGER.info("Proxying as " + authenticatedToken.getPrincipal());
    }

    static HttpServletRequestWrapper createAuthenticatedRequest(final HttpServletRequest httpServletRequest,
            final AuthenticationToken authenticatedToken) {

        return new HttpServletRequestWrapper(httpServletRequest) {
            @Override
            public String getRemoteUser() {
                return authenticatedToken.getPrincipal();
            }

            @Override
            public Principal getUserPrincipal() {
                return new Principal() {

                    @Override
                    public String getName() {
                        return authenticatedToken.getPrincipal();
                    }
                };
            }
        };
    }

}
