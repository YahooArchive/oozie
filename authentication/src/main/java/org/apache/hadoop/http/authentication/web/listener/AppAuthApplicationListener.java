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
package org.apache.hadoop.http.authentication.web.listener;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.authentication.web.AuthenticationProcessingFilter;
import org.apache.hadoop.http.authentication.web.CookieSignerVerifier;
import org.apache.hadoop.http.authentication.web.FileSystemEvictorCallback;
import org.apache.hadoop.http.authentication.web.ProxyUGICacheManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AppAuthApplicationListener implements ServletContextListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(AppAuthApplicationListener.class);

    private static final String START_TIME = "StartTime";

    protected ServletContext servletContext = null;
    protected Configuration authConfiguration = null;
    protected Configuration applicationConfiguration = null;

    protected abstract Configuration initializeApplicationConfiguration();

    protected abstract void initializeUGI();

    @Override
    public void contextInitialized(ServletContextEvent servletContextEvent) {
        servletContext = servletContextEvent.getServletContext();

        Date startDate = new Date();
        LOGGER.info("Server Starting at : " + startDate);
        servletContext.setAttribute(START_TIME, startDate);

        authConfiguration = initializeAuthConfiguration();
        servletContext.setAttribute(AuthenticationProcessingFilter.AUTH_CONFIGURATION, authConfiguration);
        servletContext.setAttribute(AuthenticationProcessingFilter.COOKIE_SIGNER_VERIFIER,
                initializeCookieSignerVerifier(authConfiguration));

        applicationConfiguration = initializeApplicationConfiguration();
        servletContext.setAttribute(AuthenticationProcessingFilter.UGI_CACHE_MANAGER,
                initializeUGICacheManager(applicationConfiguration));
        initializeUGI();
    }

    protected Configuration initializeAuthConfiguration() {
        Configuration configuration = new Configuration(false);
        configuration.addResource("authentication-conf.xml");
        configuration.addResource("authentication-site.xml");
        return configuration;
    }

    protected ProxyUGICacheManager initializeUGICacheManager(Configuration conf) {
        long ugiExpiryTimeInMillis = conf.getLong("ugi.expirytime.in.millis", TimeUnit.MINUTES.toMillis(10));
        long evictionIntervalInMillis = conf.getLong("ugi.evictioninterval.in.millis", TimeUnit.MINUTES.toMillis(5));

        ProxyUGICacheManager cacheManager = new ProxyUGICacheManager(ugiExpiryTimeInMillis, evictionIntervalInMillis,
                new FileSystemEvictorCallback());
        return cacheManager;
    }

    protected CookieSignerVerifier initializeCookieSignerVerifier(Configuration conf) {
        try {
            return new CookieSignerVerifier(conf);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void contextDestroyed(ServletContextEvent servletContextEvent) {
        servletContext.removeAttribute(START_TIME);
        servletContext.removeAttribute(AuthenticationProcessingFilter.COOKIE_SIGNER_VERIFIER);
        servletContext.removeAttribute(AuthenticationProcessingFilter.AUTH_CONFIGURATION);
        servletContext.removeAttribute(AuthenticationProcessingFilter.UGI_CACHE_MANAGER);
    }
}
