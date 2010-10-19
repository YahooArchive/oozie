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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.BundleEngine;
import org.apache.oozie.BundleEngineException;
import org.apache.oozie.BundleJobBean;
import org.apache.oozie.client.BundleJob;
import org.apache.oozie.service.BundleEngineService;
import org.apache.oozie.util.DateUtils;

public class MockBundleEngineService extends BundleEngineService {
    public static final String JOB_ID = "bundle-job-C-";
    public static final String CONFIGURATION = "<configuration></configuration>";
    public static final String GROUP = "group";
    public static final String USER = "user";
    public static final String LOG = "log";

    public static String did = null;
    public static List<BundleJob> bundleJobs;
    public static final int INIT_BUNDLE_COUNT = 4;

    static {
        reset();
    }

    public static void reset() {
        did = null;
        bundleJobs = new ArrayList<BundleJob>();
        for (int i = 0; i < INIT_BUNDLE_COUNT; i++) {
            bundleJobs.add(createDummyBundleJob(i));
        }
    }

    @Override
    public BundleEngine getBundleEngine(String user, String authToken) {
        return new MockBundleEngine(user, authToken);
    }

    private static class MockBundleEngine extends BundleEngine {
        
        public MockBundleEngine() {
        }

        public MockBundleEngine(String user, String authToken) {
            super(user, authToken);
        }

        @Override
        public String submitJob(Configuration conf, boolean startJob) throws BundleEngineException {
            did = "submit";
            int idx = bundleJobs.size();
            bundleJobs.add(createDummyBundleJob(idx, conf));
            return JOB_ID + idx;
        }
    }
        
    private static BundleJob createDummyBundleJob(int idx) {
        BundleJobBean bundleJob = new BundleJobBean();
        bundleJob.setId(JOB_ID + idx);
        bundleJob.setCreatedTime(new Date());
        bundleJob.setLastModifiedTime(new Date());
        bundleJob.setUser(USER);
        bundleJob.setGroup(GROUP);
        bundleJob.setAuthToken("notoken");
        bundleJob.setConf(CONFIGURATION);

        try {
            bundleJob.setEndTime(DateUtils.parseDateUTC("2009-02-03T23:59Z"));
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        return bundleJob;
    }

    private static BundleJob createDummyBundleJob(int idx, Configuration conf) {
        BundleJobBean coordJob = new BundleJobBean();
        coordJob.setId(JOB_ID + idx);
        coordJob.setCreatedTime(new Date());
        coordJob.setLastModifiedTime(new Date());
        coordJob.setUser(USER);
        coordJob.setGroup(GROUP);
        coordJob.setAuthToken("notoken");
        coordJob.setConf(conf.toString());
        try {
            coordJob.setEndTime(DateUtils.parseDateUTC("2009-02-03T23:59Z"));
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        return coordJob;
    }
}