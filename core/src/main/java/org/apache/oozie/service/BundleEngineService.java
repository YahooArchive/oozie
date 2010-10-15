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
package org.apache.oozie.service;

import org.apache.oozie.BundleEngine;
import org.apache.oozie.service.Service;
import org.apache.oozie.service.Services;

/**
 * Service that return a coordinator engine for a user.
 */
public class BundleEngineService implements Service {

    /**
     * Initialize the service.
     *
     * @param services services instance.
     */
    public void init(Services services) {
    }

    /**
     * Destroy the service.
     */
    public void destroy() {
    }

    /**
     * Return the public interface of the Coordinator engine service.
     *
     * @return {@link BundleEngineService}.
     */
    public Class<? extends Service> getInterface() {
        return BundleEngineService.class;
    }

    /**
     * Return a Coordinator engine.
     *
     * @param user user for the coordinator engine.
     * @param authToken the authentication token.
     * @return the bundle engine for the specified user.
     */
    public BundleEngine getBundleEngine(String user, String authToken) {
        return new BundleEngine(user, authToken);
    }

    /**
     * Return a Bundle engine for a system user (no user, no group).
     *
     * @return a system Coordinator engine.
     */
    public BundleEngine getSystemCoordinatorEngine() {
        return new BundleEngine();
    }

}
