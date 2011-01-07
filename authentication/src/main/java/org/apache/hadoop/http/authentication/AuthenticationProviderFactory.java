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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.authentication.exception.AuthenticationException;
import org.apache.hadoop.http.authentication.exception.UnknownAuthenticationSchemeException;
import org.apache.hadoop.http.authentication.web.util.Assert;

import javax.servlet.http.HttpServletRequest;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

public class AuthenticationProviderFactory {

    private List<AuthenticationProvider> authenticationProviderList = new ArrayList<AuthenticationProvider>();

    public AuthenticationProviderFactory(Configuration configuration) {
        initializeAuthenticationProviders(configuration);
    }

    private void initializeAuthenticationProviders(final Configuration configuration) {
        String authProviderConf = configuration.get("authentication.providers");
        Assert.notNull(authProviderConf,
                "You should configure at least one authentication provider in authentication.providers");
        String[] authenticationProviders = authProviderConf.split("\\s*,\\s*");

        for (String authenticationProviderFQNClassName : authenticationProviders) {
            try {
                Class<?> providerClass = Class.forName(authenticationProviderFQNClassName);
                Constructor<?> constructor = providerClass.getDeclaredConstructor(new Class[] { Configuration.class });

                AuthenticationProvider provider = (AuthenticationProvider) constructor.newInstance(configuration);
                authenticationProviderList.add(provider);
            }
            catch (InstantiationException e) {
                throw new AuthenticationException("Unable to create instance for: "
                        + authenticationProviderFQNClassName, e);
            }
            catch (IllegalAccessException e) {
                throw new AuthenticationException("Unable to create instance for: "
                        + authenticationProviderFQNClassName, e);
            }
            catch (ClassNotFoundException e) {
                throw new AuthenticationException("Unable to create instance for: "
                        + authenticationProviderFQNClassName, e);
            }
            catch (NoSuchMethodException e) {
                throw new AuthenticationException("Unable to create instance for: "
                        + authenticationProviderFQNClassName, e);
            }
            catch (InvocationTargetException e) {
                throw new AuthenticationException("Unable to create instance for: "
                        + authenticationProviderFQNClassName, e);
            }
            catch (Throwable t) {
                throw new AuthenticationException("Unable to create instance for: "
                        + authenticationProviderFQNClassName, t);
            }
        }
    }

    public AuthenticationProvider getAuthenticationProvider(HttpServletRequest httpServletRequest) {
        AuthenticationProvider supportedProvider = null;

        for (AuthenticationProvider authenticationProvider : authenticationProviderList) {
            if (authenticationProvider.supports(httpServletRequest)) {
                supportedProvider = authenticationProvider;
                break;
            }
        }

        if (supportedProvider == null) {
            throw new UnknownAuthenticationSchemeException("None of the configured providers could "
                    + "identify a scheme in Request.");
        }

        return supportedProvider;
    }
}
