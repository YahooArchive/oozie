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
package org.apache.oozie.command.coord;

import java.util.Date;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.CoordinatorJob.Execution;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.StoreService;
import org.apache.oozie.store.CoordinatorStore;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.test.XTestCase;
import org.apache.oozie.util.DateUtils;

public class TestCoordChangeCommand extends XTestCase {
    private Services services;

    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        services.init();
        cleanUpDBTables();
    }

    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    public void testCoordChangeCommand() throws StoreException, CommandException {
        System.out.println("Running Test");
        String jobId = "0000000-" + new Date().getTime() + "-testCoordChangeCommand-C";
        CoordinatorStore store = Services.get().get(StoreService.class).getStore(CoordinatorStore.class);
        try {
            addRecordToJobTable(jobId, store);
        }
        catch (Exception ex) {
            ex.printStackTrace();
            fail("Exception thrown " + ex);
        }
        finally {
            store.closeTrx();
        }
        new CoordChangeCommand(jobId, "endtime=2011-12-01T05:00Z;concurrency=200").call();
        try {
            checkCoordJobs(jobId, DateUtils.parseDateUTC("2011-12-01T05:00Z"), 200, null, false);
        }
        catch (Exception ex) {
            ex.printStackTrace();
            fail("Invalid date" + ex);
        }

        new CoordChangeCommand(jobId, "endtime=2011-12-01T05:00Z;concurrency=200;pausetime=2099-11-01T05:00Z").call();
        try {
            checkCoordJobs(jobId, DateUtils.parseDateUTC("2011-12-01T05:00Z"), 200, DateUtils
                    .parseDateUTC("2099-11-01T05:00Z"), true);
        }
        catch (Exception ex) {
            ex.printStackTrace();
            fail("Invalid date" + ex);
        }

        new CoordChangeCommand(jobId, "endtime=2011-12-01T05:00Z;concurrency=200;pausetime=").call();
        try {
            checkCoordJobs(jobId, DateUtils.parseDateUTC("2011-12-01T05:00Z"), 200, null, true);
        }
        catch (Exception ex) {
            ex.printStackTrace();
            fail("Invalid date" + ex);
        }

        new CoordChangeCommand(jobId, "endtime=2011-12-01T05:00Z;pausetime=;concurrency=200").call();
        try {
            checkCoordJobs(jobId, DateUtils.parseDateUTC("2011-12-01T05:00Z"), 200, null, true);
        }
        catch (Exception ex) {
            ex.printStackTrace();
            fail("Invalid date" + ex);
        }

        new CoordChangeCommand(jobId, "endtime=2012-12-20T05:00Z;concurrency=-200").call();
        try {
            checkCoordJobs(jobId, DateUtils.parseDateUTC("2012-12-20T05:00Z"), -200, null, false);
        }
        catch (Exception ex) {
            ex.printStackTrace();
            fail("Invalid date" + ex);
        }

        new CoordChangeCommand(jobId, "endtime=2012-12-20T05:00Z").call();
        try {
            checkCoordJobs(jobId, DateUtils.parseDateUTC("2012-12-20T05:00Z"), null, null, false);
        }
        catch (Exception ex) {
            ex.printStackTrace();
            fail("Invalid date" + ex);
        }

        new CoordChangeCommand(jobId, "concurrency=-1").call();
        try {
            checkCoordJobs(jobId, null, -1, null, false);
        }
        catch (Exception ex) {
            ex.printStackTrace();
            fail("Invalid date" + ex);
        }

        try {
            new CoordChangeCommand(jobId, "a=1;b=-200").call();
            fail("Should not reach here.");
        }
        catch (CommandException ex) {
            if (ex.getErrorCode() != ErrorCode.E1015) {
                fail("Error code should be E1015.");
            }
        }

        try {
            new CoordChangeCommand(jobId, "endtime=2012-12-20T05:00;concurrency=-200").call();
            fail("Should not reach here.");
        }
        catch (CommandException ex) {
            if (ex.getErrorCode() != ErrorCode.E1015) {
                fail("Error code should be E1015.");
            }
        }

        try {
            new CoordChangeCommand(jobId, "endtime=2012-12-20T05:00Z;concurrency=2ac").call();
            fail("Should not reach here.");
        }
        catch (CommandException ex) {
            if (ex.getErrorCode() != ErrorCode.E1015) {
                fail("Error code should be E1015.");
            }
        }

        try {
            new CoordChangeCommand(jobId, "endtime=1900-12-20T05:00Z").call();
            fail("Should not reach here.");
        }
        catch (CommandException ex) {
            if (ex.getErrorCode() != ErrorCode.E1015) {
                fail("Error code should be E1015.");
            }
        }

        try {
            new CoordChangeCommand(jobId, "pausetime=1900-12-20T05:00Z").call();
            fail("Should not reach here.");
        }
        catch (CommandException ex) {
            if (ex.getErrorCode() != ErrorCode.E1015) {
                fail("Error code should be E1015.");
            }
        }

        try {
            new CoordChangeCommand(jobId, "pausetime=2009-02-01T01:03Z").call();
            fail("Should not reach here.");
        }
        catch (CommandException ex) {
            if (ex.getErrorCode() != ErrorCode.E1015) {
                fail("Error code should be E1015.");
            }
        }

        try {
            new CoordChangeCommand(jobId, "pausetime=null").call();
            fail("Should not reach here.");
        }
        catch (CommandException ex) {
            if (ex.getErrorCode() != ErrorCode.E1015) {
                fail("Error code should be E1015.");
            }
        }
        
        try {
            new CoordChangeCommand(jobId, "pausetime=2009-02-01T01:08Z").call();
            fail("Should not reach here.");
        }
        catch (CommandException ex) {
            if (ex.getErrorCode() != ErrorCode.E1015) {
                fail("Error code should be E1015.");
            }
        }
        
        try {
            new CoordChangeCommand(jobId, "pausetime").call();
            fail("Should not reach here.");
        }
        catch (CommandException ex) {
            if (ex.getErrorCode() != ErrorCode.E1015) {
                fail("Error code should be E1015.");
            }
        }
    }

    private void addRecordToJobTable(String jobId, CoordinatorStore store) throws Exception {
        // CoordinatorStore store = new CoordinatorStore(false);
        CoordinatorJobBean coordJob = new CoordinatorJobBean();
        coordJob.setId(jobId);
        coordJob.setAppName("testApp");
        coordJob.setAppPath("testAppPath");
        coordJob.setStatus(CoordinatorJob.Status.SUCCEEDED);
        coordJob.setCreatedTime(new Date());
        coordJob.setLastModifiedTime(DateUtils.parseDateUTC("2009-01-02T23:59Z"));
        coordJob.setUser("testUser");
        coordJob.setGroup("testGroup");
        coordJob.setAuthToken("notoken");

        String confStr = "<configuration></configuration>";
        coordJob.setConf(confStr);
        String appXml = "<coordinator-app xmlns='uri:oozie:coordinator:0.1' name='NAME' frequency=\"5\" start='2009-02-01T01:00Z' end='2009-02-01T01:09Z' timezone='UTC' freq_timeunit='MINUTE' end_of_duration='NONE'>";
        appXml += "<controls>";
        appXml += "<timeout>10</timeout>";
        appXml += "<concurrency>2</concurrency>";
        appXml += "<execution>LIFO</execution>";
        appXml += "</controls>";
        appXml += "<input-events>";
        appXml += "<data-in name='A' dataset='a'>";
        appXml += "<dataset name='a' frequency='5' initial-instance='2009-02-01T01:00Z' timezone='UTC' freq_timeunit='MINUTE' end_of_duration='NONE'>";
        appXml += "<uri-template>file:///tmp/coord/workflows/${YEAR}/${DAY}</uri-template>";
        appXml += "</dataset>";
        appXml += "<instance>${coord:latest(0)}</instance>";
        appXml += "</data-in>";
        appXml += "</input-events>";
        appXml += "<output-events>";
        appXml += "<data-out name='LOCAL_A' dataset='local_a'>";
        appXml += "<dataset name='local_a' frequency='5' initial-instance='2009-02-01T01:00Z' timezone='UTC' freq_timeunit='MINUTE' end_of_duration='NONE'>";
        appXml += "<uri-template>file:///tmp/coord/workflows/${YEAR}/${DAY}</uri-template>";
        appXml += "</dataset>";
        appXml += "<instance>${coord:current(-1)}</instance>";
        appXml += "</data-out>";
        appXml += "</output-events>";
        appXml += "<action>";
        appXml += "<workflow>";
        appXml += "<app-path>hdfs:///tmp/workflows/</app-path>";
        appXml += "<configuration>";
        appXml += "<property>";
        appXml += "<name>inputA</name>";
        appXml += "<value>${coord:dataIn('A')}</value>";
        appXml += "</property>";
        appXml += "<property>";
        appXml += "<name>inputB</name>";
        appXml += "<value>${coord:dataOut('LOCAL_A')}</value>";
        appXml += "</property>";
        appXml += "</configuration>";
        appXml += "</workflow>";
        appXml += "</action>";
        appXml += "</coordinator-app>";
        coordJob.setJobXml(appXml);
        coordJob.setLastActionNumber(0);
        coordJob.setFrequency(5);
        coordJob.setExecution(Execution.FIFO);
        coordJob.setConcurrency(1);
        try {
            coordJob.setStartTime(DateUtils.parseDateUTC("2009-02-01T01:00Z"));
            coordJob.setEndTime(DateUtils.parseDateUTC("2009-02-01T01:09Z"));
            coordJob.setLastActionTime(DateUtils.parseDateUTC("2009-02-01T01:10Z"));
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Could not set Date/time");
        }

        try {
            store.beginTrx();
            store.insertCoordinatorJob(coordJob);
            store.commitTrx();
        }
        catch (StoreException se) {
            se.printStackTrace();
            store.rollbackTrx();
            fail("Unable to insert the test job record to table");
            throw se;
        }
    }

    private void checkCoordJobs(String jobId, Date endTime, Integer concurrency, Date pauseTime, boolean checkPauseTime) throws StoreException {
        CoordinatorStore store = Services.get().get(StoreService.class).getStore(CoordinatorStore.class);
        try {
            CoordinatorJobBean job = store.getCoordinatorJob(jobId, false);

            if (endTime != null) {
                Date d = job.getEndTime();
                if (d.compareTo(endTime) != 0) {
                    fail("Endtime is not updated properly" + d + " " + endTime);
                }

                CoordinatorJob.Status status = job.getStatus();
                if (status != CoordinatorJob.Status.RUNNING) {
                    fail("Coordinator job's status is not updated properly");
                }
            }

            if (concurrency != null) {
                int c = job.getConcurrency();

                if (c != concurrency) {
                    fail("Concurrency is not updated properly");
                }
            }

            if (checkPauseTime) {
                Date d = job.getPauseTime();
                if (pauseTime == null) {
                    if (d != null) {
                        fail("Pausetime is not updated properly" + d + " " + pauseTime);
                    }
                }
                else {
                    if (d.compareTo(pauseTime) != 0) {
                        fail("Pausetime is not updated properly" + d + " " + pauseTime);
                    }
                }
            }
        }
        catch (StoreException se) {
            se.printStackTrace();
        }
    }
}
