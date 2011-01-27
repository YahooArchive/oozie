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
 * Abstract class contains basic implementation for {@link AuthenticationToken}.
 * <p/>
 * To provide user-defined authentication, one token <code>AuthenticationToken</code>
 * instance has to be provided to carry the information that authentication needs.
 */
public abstract class AbstractAuthenticationToken implements AuthenticationToken {

    private boolean authenticated = false;
    protected String principal;
    protected String remoteAddr;
    private final String token;

    protected AbstractAuthenticationToken(String remoteAddr, String token) {
        this(null, remoteAddr, token);
    }

    protected AbstractAuthenticationToken(String principal, String remoteAddr, String token) {
        this.principal = principal;
        this.remoteAddr = remoteAddr;
        this.token = token;
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.http.authentication.AuthenticationToken#isAuthenticated()
     */
    @Override
    public boolean isAuthenticated() {
        return authenticated;
    }

    /**
     * Set true if token is authenticated
     *
     * @param authenticated
     */
    public void setAuthenticated(boolean authenticated) {
        this.authenticated = authenticated;
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.http.authentication.AuthenticationToken#getPrincipal()
     */
    @Override
    public String getPrincipal() {
        return principal;
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.http.authentication.AuthenticationToken#getRemoteAddr()
     */
    @Override
    public String getRemoteAddr() {
        return remoteAddr;
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.http.authentication.AuthenticationToken#getToken()
     */
    public String getToken() {
        return token;
    }

}
