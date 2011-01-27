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

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;

/**
 * Provider interface for server authentication class.
 *
 */
public interface AuthenticationProvider {

    /**
     * Check if this authentication provider supports client request
     *
     * @param httpServletRequest
     * @return true if this authentication provider supports client request
     */
    boolean supports(HttpServletRequest httpServletRequest);

    /**
     * Get authentication token
     *
     * @param httpServletRequest httpServletRequest
     * @return authentication token
     * @throws IOException thrown if error to retrieve token from request
     */
    AuthenticationToken getAuthenticationToken(HttpServletRequest httpServletRequest) throws IOException;

    /**
     * Verify and authenticate the token
     *
     * @param authenticationToken authentication token
     * @return authentication token
     */
    AuthenticationToken authenticate(AuthenticationToken authenticationToken);
}
