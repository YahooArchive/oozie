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

/**
 * The interface for a token which contains the information for a authentication provider to use.
 * <p/>
 * Abstract class {@link} provides basic implementation for this interface.
 *
 */
public interface AuthenticationToken {

    /**
     * Get authentication name
     *
     * @return authentication name
     */
    String getAuthenticationMethod();

    /**
     * True if it is authenticated
     *
     * @return true if authenticated
     */
    boolean isAuthenticated();

    /**
     * Get Principal
     *
     * @return principal
     */
    String getPrincipal();

    /**
     * Get remote address
     *
     * @return remote address
     */
    String getRemoteAddr();

    /**
     * Get token
     *
     * @return token
     */
    String getToken();
}
