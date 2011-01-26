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
package org.apache.oozie.filter;

import java.io.IOException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import org.apache.oozie.servlet.JsonRestServlet;

/**
 * The filter checks for whether Oozie has security turned on during
 * initialization. In case security is turned on, the username is read from request
 * and set as oozie.user.name for later servlets
 */
public class OozieAuthFilter implements Filter {

    /**
     * Initializes the Filter. Reads the username from the request and set it as oozie.user.name
     */
    public void init(FilterConfig config) throws ServletException {
    }

    public void doFilter(ServletRequest req, ServletResponse res, FilterChain chain) throws IOException,
    ServletException {
        HttpServletRequest request = (HttpServletRequest) req;
        String userName = request.getUserPrincipal().getName();
        setUserName(request, userName);
        chain.doFilter(req, res);
    }

    /**
     * Take care of cleanup. NOP for now.
     */
    public void destroy() {
    }

    /**
     * Sets the user name to be used later in the chain or by servlets.
     *
     * @param request the request object
     * @param userId the user name to set
     */
    private void setUserName(HttpServletRequest request, String userId) {
        request.setAttribute(JsonRestServlet.USER_NAME, userId);
    }

}