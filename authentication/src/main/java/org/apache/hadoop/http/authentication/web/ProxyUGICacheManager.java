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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.servlet.http.HttpServletRequest;

import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProxyUGICacheManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProxyUGICacheManager.class.getName());

    private ConcurrentMap<String, CacheEntry> userUgiMap;
    private final ScheduledThreadPoolExecutor evictorDaemon;

    public ProxyUGICacheManager(long ugiExpiryTimeInMillis, long evictionIntervalInMillis, EvictorCallback callback) {
        userUgiMap = new ConcurrentHashMap<String, CacheEntry>();
        evictorDaemon = new ScheduledThreadPoolExecutor(1);
        CacheEvictor evictor = new CacheEvictor(ugiExpiryTimeInMillis, callback);
        if (callback != null) {
            evictorDaemon.scheduleWithFixedDelay(evictor, ugiExpiryTimeInMillis, evictionIntervalInMillis,
                    TimeUnit.MILLISECONDS);
        }
    }

    public UserGroupInformation getUGI(String user, HttpServletRequest request) throws IOException {
        CacheEntry entry = userUgiMap.get(user);
        if (entry == null) {
//            UserGroupInformation ugi = UserGroupInformation.createProxyUser(user, UserGroupInformation.getLoginUser());
            UserGroupInformation ugi = UserGroupInformation.createProxyUser(user, UserGroupInformation.getCurrentUser());
            // Take care of race condition
            CacheEntry oldEntry = userUgiMap.putIfAbsent(user, new CacheEntry(ugi, request));
            if (oldEntry == null) {
                LOGGER.info("Creating new proxy ugi for user " + user);
            }
            else {
                oldEntry.addRequest(request);
            }
            return ugi;
        }
        entry.addRequest(request);
        return entry.getUgi();
    }

    public int getNumberOfRequestsForUser(String user) throws IOException {
        CacheEntry entry = userUgiMap.get(user);
        return entry.getNumRequests();
    }

    public void removeRequest(String user, HttpServletRequest request) {
        CacheEntry entry = userUgiMap.get(user);
        if (entry == null) {
            LOGGER.warn("The Cache Manager should have had the ugi for the user " + user);
            return;
        }
        if (!entry.removeRequest(request)) {
            LOGGER.warn("The Cache Manager should have had the request " + request.getRequestURI() + "?"
                    + request.getQueryString() + " from user " + user);
        }
    }

    public void destroy() {
        evictorDaemon.shutdownNow();
        userUgiMap.clear();
        userUgiMap = null;
    }

    private static class CacheEntry {
        private UserGroupInformation ugi;
        private long lastAccessTime;
        private ConcurrentMap<HttpServletRequest, Boolean> requests = new ConcurrentHashMap<HttpServletRequest, Boolean>();

        public CacheEntry(UserGroupInformation ugi, HttpServletRequest request) {
            this.ugi = ugi;
            addRequest(request);
        }

        public UserGroupInformation getUgi() {
            return ugi;
        }

        public long getLastAccessedTime() {
            return lastAccessTime;
        }

        public void addRequest(HttpServletRequest request) {
            if (request == null)
                throw new IllegalArgumentException("HttpServletRequest cannot be null");
            requests.put(request, Boolean.TRUE);
            lastAccessTime = System.currentTimeMillis();
        }

        public boolean removeRequest(HttpServletRequest request) {
            lastAccessTime = System.currentTimeMillis();
            return requests.remove(request) == null ? false : true;
        }

        public boolean hasRequests() {
            return requests.size() != 0;
        }

        public int getNumRequests() {
            return requests.size();
        }
    }

    private class CacheEvictor implements Runnable {
        private final long ugiExpiryTimeInMillis;
        private final EvictorCallback callback;

        public CacheEvictor(long ugiExpiryTimeInMillis, EvictorCallback callback) {
            this.ugiExpiryTimeInMillis = ugiExpiryTimeInMillis;
            this.callback = callback;
        }

        public void run() {
            try {
                if (userUgiMap.isEmpty())
                    return;

                long currentTime = System.currentTimeMillis();
                LOGGER.info("Checking UGI cache for expired entries");
                for (ConcurrentMap.Entry<String, ProxyUGICacheManager.CacheEntry> mapEntry : userUgiMap.entrySet()) {
                    CacheEntry cacheEntry = mapEntry.getValue();
                    long lastAccessed = cacheEntry.getLastAccessedTime();
                    long lifeTime = currentTime - lastAccessed;
                    if (lifeTime < ugiExpiryTimeInMillis) {
                        continue;
                    }

                    if (!cacheEntry.hasRequests()) {
                        LOGGER.info("UGI for user " + mapEntry.getKey() + " has expired. Evicting");
                        userUgiMap.remove(mapEntry.getKey());
                        callback.callback(cacheEntry.getUgi());
                    }
                    else if (lifeTime > TimeUnit.HOURS.toMillis(3)) {
                        LOGGER.warn("There is a request running for more than 3 hours using ugi: " + mapEntry.getKey());
                    }
                }
            }
            catch (Throwable e) {
                LOGGER.error("Error while evicting UGI cache", e);
            }
        }
    }

}
