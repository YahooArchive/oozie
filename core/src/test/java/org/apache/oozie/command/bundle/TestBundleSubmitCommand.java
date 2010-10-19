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
package org.apache.oozie.command.bundle;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;
import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.BundleJobBean;
import org.apache.oozie.client.BundleJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.service.Services;
import org.apache.oozie.store.BundleStore;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.test.XTestCase;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XLog;

public class TestBundleSubmitCommand extends XTestCase {
    private Services services;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        services.init();
    }

    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    /**
     * Basic test
     *
     * @throws Exception
     */
    public void testBasicSubmit() throws Exception {
        Configuration conf = new XConfiguration();
        String appPath = getTestCaseDir();
        String appXml = getBundleXml("bundle-submit-job1.xml");

        writeToFile(appXml, appPath);
        conf.set(OozieClient.BUNDLE_APP_PATH, appPath);
        conf.set(OozieClient.USER_NAME, getTestUser());
        conf.set(OozieClient.GROUP_NAME, "other");
        BundleSubmitCommand sc = new BundleSubmitCommand(conf, "UNIT_TESTING");
        String jobId = sc.call();

        assertEquals(jobId.substring(jobId.length() - 2), "-B");

        BundleJobBean job = checkBundleJobs(jobId);
        if (job != null) {
            assertEquals(job.getStatus(), BundleJob.Status.PREP);
        }
    }


    /**
     * test schema error. Negative test case.
     *
     * @throws Exception
     */
    public void testSchemaError() throws Exception {
        Configuration conf = new XConfiguration();
        String appPath = getTestCaseDir();
        String appXml = getBundleXml("bundle-submit-job-error1.xml");

        writeToFile(appXml, appPath);
        conf.set(OozieClient.BUNDLE_APP_PATH, appPath);
        conf.set(OozieClient.USER_NAME, getTestUser());
        conf.set(OozieClient.GROUP_NAME, "other");
        BundleSubmitCommand sc = new BundleSubmitCommand(conf, "UNIT_TESTING");
        try {
            sc.call();
            fail("Exception expected if schema has errors!");
        }
        catch (CommandException e) {
            // should come here for schema errors
        }
    }



    /**
     * Don't include controls in XML.
     *
     * @throws Exception
     */
    public void testSubmitNoControls() throws Exception {
        Configuration conf = new XConfiguration();
        String appPath = getTestCaseDir();
        String appXml = getBundleXml("bundle-submit-job2.xml");

        writeToFile(appXml, appPath);
        conf.set(OozieClient.BUNDLE_APP_PATH, appPath);
        conf.set(OozieClient.USER_NAME, getTestUser());
        conf.set(OozieClient.GROUP_NAME, "other");
        BundleSubmitCommand sc = new BundleSubmitCommand(conf, "UNIT_TESTING");
        String jobId = sc.call();

        assertEquals(jobId.substring(jobId.length() - 2), "-B");

        BundleJobBean job = checkBundleJobs(jobId);
        if (job != null) {
            assertNull(job.getKickoffTime());
        }
    }

    private String getBundleXml(String resourceXmlName) {
        try {
            Reader reader = IOUtils.getResourceAsReader(resourceXmlName, -1);
            String appXml = IOUtils.getReaderAsString(reader, -1);
            return appXml;
        }
        catch (IOException ioe) {
            throw new RuntimeException(XLog.format("Could not get " + resourceXmlName, ioe));
        }
    }

    /**
     * Helper methods
     *
     * @param jobId Bundle job id
     * @throws StoreException
     */
    private BundleJobBean checkBundleJobs(String jobId) throws StoreException {
        BundleStore store = new BundleStore(false);
        try {
            BundleJobBean job = store.getBundleJob(jobId, false);
            return job;
        }
        catch (StoreException se) {
            fail("Job ID " + jobId + " was not stored properly in db");
        }
        return null;
    }

    private void writeToFile(String appXml, String appPath) throws IOException {
        File wf = new File(appPath + "/bundle.xml");
        PrintWriter out = null;
        try {
            out = new PrintWriter(new FileWriter(wf));
            out.println(appXml);
        }
        catch (IOException iex) {
            throw iex;
        }
        finally {
            if (out != null) {
                out.close();
            }
        }
    }
}
