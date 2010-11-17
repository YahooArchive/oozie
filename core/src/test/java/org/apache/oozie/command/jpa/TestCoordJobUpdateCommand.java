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
package org.apache.oozie.command.jpa;

import java.io.IOException;
import java.io.Reader;
import java.util.Date;
import java.util.Properties;
import java.util.regex.Matcher;

import org.apache.hadoop.fs.Path;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.CoordinatorJob.Execution;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.local.LocalOozie;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XFsTestCase;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XmlUtils;

public class TestCoordJobUpdateCommand extends XFsTestCase {

    Services services;

    /* (non-Javadoc)
     * @see org.apache.oozie.test.XFsTestCase#setUp()
     */
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        services.init();
        cleanUpDBTables();
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.test.XFsTestCase#tearDown()
     */
    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    public void testCoordJobUpdateCommand() throws Exception {
        String jobId = "00000-" + new Date().getTime() + "-TestCoordJobUpdateCommand-C";
        insertJob(jobId, CoordinatorJob.Status.PREP);
        _testUpdateCoordJob(jobId);
    }

    private void _testUpdateCoordJob(String jobId) {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        try {
            CoordJobGetCommand coordGetCmd = new CoordJobGetCommand(jobId);
            CoordinatorJobBean job = jpaService.execute(coordGetCmd);
            int newFreq = job.getFrequency() + 1;
            job.setFrequency(newFreq);
            CoordJobUpdateCommand coordUpdateCmd = new CoordJobUpdateCommand(job);
            jpaService.execute(coordUpdateCmd);
            job = jpaService.execute(coordGetCmd);
            assertEquals(newFreq, job.getFrequency());
        }
        catch (Exception ex) {
            ex.printStackTrace();
            fail("Unable to UPDATE a record for COORD Job. jobId =" + jobId);
        }

    }

    private void insertJob(String jobId, CoordinatorJob.Status status) throws Exception {
        Path appPath = new Path(getFsTestCaseDir(), "coord");
        String appXml = getCoordJobXml(appPath);

        CoordinatorJobBean coordJob = new CoordinatorJobBean();
        coordJob.setId(jobId);
        coordJob.setAppName("COORD-TEST");
        coordJob.setAppPath(appPath.toString());
        coordJob.setStatus(status);
        coordJob.setCreatedTime(new Date());
        coordJob.setLastModifiedTime(new Date());
        coordJob.setUser(getTestUser());
        coordJob.setGroup(getTestGroup());
        coordJob.setAuthToken("notoken");

        Properties conf = getCoordConf(appPath);
        String confStr = XmlUtils.writePropToString(conf);

        coordJob.setConf(confStr);
        coordJob.setJobXml(appXml);
        coordJob.setLastActionNumber(0);
        coordJob.setFrequency(1);
        coordJob.setExecution(Execution.FIFO);
        coordJob.setConcurrency(1);
        try {
            coordJob.setStartTime(DateUtils.parseDateUTC("2009-12-15T01:00Z"));
            coordJob.setEndTime(DateUtils.parseDateUTC("2009-12-17T01:00Z"));
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Could not set Date/time");
        }

        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            assertNotNull(jpaService);
            CoordJobInsertCommand coordInsertCmd = new CoordJobInsertCommand(coordJob);
            jpaService.execute(coordInsertCmd);
        }
        catch (CommandException ce) {
            ce.printStackTrace();
            fail("Unable to insert the test job record to table");
            throw ce;
        }
    }

    private String getCoordJobXml(Path appPath) {
        String inputTemplate = appPath + "/coord-input/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";
        inputTemplate = Matcher.quoteReplacement(inputTemplate);
        String outputTemplate = appPath + "/coord-input/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";
        outputTemplate = Matcher.quoteReplacement(outputTemplate);
        try {
            Reader reader = IOUtils.getResourceAsReader("coord-job-get.xml", -1);
            String appXml = IOUtils.getReaderAsString(reader, -1);
            appXml = appXml.replaceAll("#inputTemplate", inputTemplate);
            appXml = appXml.replaceAll("#outputTemplate", outputTemplate);
            return appXml;
        }
        catch (IOException ioe) {
            throw new RuntimeException(XLog.format("Could not get coord-rerun-job.xml", ioe));
        }
    }

    private Properties getCoordConf(Path appPath) {
        Path wfAppPath = new Path(getFsTestCaseDir(), "workflow");
        final OozieClient coordClient = LocalOozie.getCoordClient();
        Properties conf = coordClient.createConfiguration();
        conf.setProperty(OozieClient.COORDINATOR_APP_PATH, appPath.toString());
        conf.setProperty("jobTracker", getJobTrackerUri());
        conf.setProperty("nameNode", getNameNodeUri());
        conf.setProperty("wfAppPath", wfAppPath.toString());
        conf.remove("user.name");
        conf.setProperty("user.name", getTestUser());
        injectKerberosInfo(conf);
        return conf;
    }
}
