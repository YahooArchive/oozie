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

import java.io.IOException;

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.authentication.AuthenticationProvider;
import org.apache.hadoop.http.authentication.AuthenticationProviderFactory;
import org.apache.hadoop.http.authentication.AuthenticationToken;
import org.apache.hadoop.http.authentication.AuthenticationTokenSerDe;
import org.apache.hadoop.http.authentication.AuthenticationTokenSerDe.CookieBasedAuthenticationToken;
import org.apache.hadoop.http.authentication.exception.AccessDeniedException;
import org.apache.hadoop.http.authentication.exception.AuthenticationException;
import org.apache.hadoop.http.authentication.exception.UnknownAuthenticationSchemeException;
import org.apache.hadoop.http.exception.HttpExceptionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

public class AuthenticationProcessingFilter implements javax.servlet.Filter {

    public static final String AUTH_CONFIGURATION = "auth.configuration";
    public static final String UGI_CACHE_MANAGER = "ugi.cache.manager";
    public static final String COOKIE_SIGNER_VERIFIER = "cookie.signer.verifier";
    public static final String AUTHENTICATION_TOKEN = "authentication.token";
    private static final Logger LOGGER = LoggerFactory.getLogger(AuthenticationProcessingFilter.class.getName());

    private AuthenticationProviderFactory providerFactory;
    private ProxyUGICacheManager ugiManager;
    private CookieSignerVerifier cookieSignerVerifier;

    public void init(FilterConfig config) throws ServletException {
        Configuration configuration = (Configuration) config.getServletContext().getAttribute(AUTH_CONFIGURATION);
        providerFactory = new AuthenticationProviderFactory(configuration);
        ugiManager = (ProxyUGICacheManager) config.getServletContext().getAttribute(UGI_CACHE_MANAGER);
        cookieSignerVerifier = (CookieSignerVerifier) config.getServletContext().getAttribute(COOKIE_SIGNER_VERIFIER);
    }

    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain)
            throws ServletException, IOException {
        final HttpServletRequest httpServletRequest = (HttpServletRequest) servletRequest;
        final HttpServletResponse httpServletResponse = (HttpServletResponse) servletResponse;
        final String path = httpServletRequest.getPathInfo();
        AuthenticationToken authenticatedToken = null;
        AuthenticationException cookieException = null;
        try {
            try {
                authenticatedToken = getAuthenticationTokenFromRequest(httpServletRequest);
            }
            catch (AuthenticationException e) {
                cookieException = e;
            }
            if (authenticatedToken == null || !authenticatedToken.isAuthenticated()) {
                authenticatedToken = authenticate(httpServletRequest);
                MDC.put("User", authenticatedToken.getPrincipal());
                Cookie cookie = CookieHelper.create(AuthenticationTokenSerDe.serialize(authenticatedToken,
                        cookieSignerVerifier));
                httpServletResponse.addCookie(cookie);
            }
            httpServletRequest.setAttribute(AUTHENTICATION_TOKEN, authenticatedToken);
            AuthenticationHelper.setupUGI(ugiManager, httpServletRequest, authenticatedToken);
            ServletRequest authenticatedRequest = AuthenticationHelper.createAuthenticatedRequest(httpServletRequest,
                    authenticatedToken);

            filterChain.doFilter(authenticatedRequest, servletResponse);

        }
        catch (UnknownAuthenticationSchemeException ignore) {
            httpServletResponse.addHeader("WWW-Authenticate", "Negotiate");
            if (cookieException == null) {
                LOGGER.warn("Request did not have any authentication information. Replying with Negotiate header", ignore);
                HttpExceptionUtil.sendErrorAsXml(httpServletResponse, HttpServletResponse.SC_UNAUTHORIZED,
                        new AccessDeniedException("Authentication is required"), path);
            }
            else {
                handleAuthenticationFailure(httpServletResponse, cookieException, path);
            }
        }
        catch (AuthenticationException e) {
            handleAuthenticationFailure(httpServletResponse, e, path);
        }
        catch (IllegalArgumentException e) {
            sendErrorAsXml(httpServletResponse, e, HttpServletResponse.SC_BAD_REQUEST, path);
        }
        catch (Throwable t) {
            sendErrorAsXml(httpServletResponse, t, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, path);
        }
        finally {
            MDC.remove("User");
            httpServletRequest.removeAttribute(AUTHENTICATION_TOKEN);
            if (authenticatedToken != null) {
                ugiManager.removeRequest(authenticatedToken.getPrincipal(), httpServletRequest);
            }
        }
    }

    private AuthenticationToken getAuthenticationTokenFromRequest(HttpServletRequest httpServletRequest) {
        Cookie authenticatedCookie = CookieHelper.extract(httpServletRequest);
        if (authenticatedCookie != null) {
            CookieBasedAuthenticationToken authenticatedToken = (CookieBasedAuthenticationToken) AuthenticationTokenSerDe
                    .deserialize(authenticatedCookie.getValue(), httpServletRequest.getRemoteAddr(),
                            cookieSignerVerifier);
            MDC.put("User", authenticatedToken.getPrincipal());
            LOGGER.info("Cookie had a valid authentication token. Original authentication method:"
                    + authenticatedToken.getInitialAuthMethod());
            return authenticatedToken;
        }
        return null;
    }

    private AuthenticationToken authenticate(HttpServletRequest httpServletRequest) throws IOException {
        AuthenticationProvider supportedProvider = providerFactory.getAuthenticationProvider(httpServletRequest);
        AuthenticationToken rawToken = supportedProvider.getAuthenticationToken(httpServletRequest);
        AuthenticationToken authenticatedToken = supportedProvider.authenticate(rawToken);

        if (!authenticatedToken.isAuthenticated()) {
            throw new AccessDeniedException("Authentication failed for: " + authenticatedToken.getPrincipal());
        }
        return authenticatedToken;
    }

    private void handleAuthenticationFailure(HttpServletResponse httpServletResponse, Exception e, String path) {
        httpServletResponse.addCookie(CookieHelper.createExpiredCookie());
        sendErrorAsXml(httpServletResponse, e, HttpServletResponse.SC_UNAUTHORIZED, path);
    }

    private void sendErrorAsXml(HttpServletResponse httpServletResponse, Throwable t, int statusCode, String path) {
        LOGGER.error("Error processing Authentication.", t);
        try {
            HttpExceptionUtil.sendErrorAsXml(httpServletResponse, statusCode, t, path);
        }
        catch (IOException e) {
            LOGGER.error("Error sending failure.", e);
        }
    }

    @Override
    public void destroy() {
    }
}
