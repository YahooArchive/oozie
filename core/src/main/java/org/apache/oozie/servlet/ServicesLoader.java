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
package org.apache.oozie.servlet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.authentication.web.listener.AppAuthApplicationListener;
import org.apache.oozie.service.ServiceException;
import org.apache.oozie.service.Services;

import javax.servlet.ServletContextListener;
import javax.servlet.ServletContextEvent;

/**
 * Webapp context listener that initializes Oozie {@link Services}.
 */
public class ServicesLoader extends AppAuthApplicationListener {
    private static Services services;

    /**
     * Initialize Oozie services.
     *
     * @param event context event.
     */
    public void contextInitialized(ServletContextEvent event) {
        try {
            services = new Services();
            services.init();
            super.contextInitialized(event);
        }
        catch (ServiceException ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Destroy Oozie services.
     *
     * @param event context event.
     */
    public void contextDestroyed(ServletContextEvent event) {
        services.destroy();
    }

    @Override
    protected Configuration initializeAuthConfiguration() {
        Configuration configuration = services.getConf();
        return configuration;
    }

    @Override
    protected Configuration initializeApplicationConfiguration() {
        Configuration configuration = services.getConf();
        return configuration;
    }

    @Override
    protected void initializeUGI() {
        // no-op
    }

}
