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

    @Override
    public boolean isAuthenticated() {
        return authenticated;
    }

    public void setAuthenticated(boolean authenticated) {
        this.authenticated = authenticated;
    }

    @Override
    public String getPrincipal() {
        return principal;
    }

    @Override
    public String getRemoteAddr() {
        return remoteAddr;
    }

    public String getToken() {
        return token;
    }

}
