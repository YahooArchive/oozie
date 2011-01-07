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

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;

import org.apache.hadoop.http.authentication.AuthenticationTokenSerDe;

final class CookieHelper {
    private static final String COOKIE_NAME = "Hadoop-HTTP-Auth";

    private CookieHelper() {
    }

    static Cookie create(String serializedAuthenticatedToken) {
        Cookie cookie = new Cookie(COOKIE_NAME, serializedAuthenticatedToken);
        cookie.setMaxAge((int) AuthenticationTokenSerDe.EXPIRATION_IN_SECONDS);
        cookie.setPath("/");
        return cookie;
    }

    static Cookie createExpiredCookie() {
        Cookie cookie = new Cookie(COOKIE_NAME, "Expired cookie To clear browsers");
        cookie.setMaxAge(0);
        cookie.setPath("/");
        return cookie;
    }

    static Cookie extract(HttpServletRequest httpServletRequest) {
        Cookie authenticatedCookie = null;
        Cookie[] cookies = httpServletRequest.getCookies();
        if (cookies != null) {
            for (Cookie cookie : cookies) {
                if (COOKIE_NAME.equals(cookie.getName())) {
                    authenticatedCookie = cookie;
                    break;
                }
            }
        }

        return authenticatedCookie;
    }
}
