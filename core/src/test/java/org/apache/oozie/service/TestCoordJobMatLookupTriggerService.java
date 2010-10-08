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

import java.util.Date;

import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.CoordinatorJob.Execution;
import org.apache.oozie.service.CoordJobMatLookupTriggerService.CoordJobMatLookupTriggerRunnable;
import org.apache.oozie.store.CoordinatorStore;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.test.XTestCase;
import org.apache.oozie.util.DateUtils;

public class TestCoordJobMatLookupTriggerService extends XTestCase {
    private Services services;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        services.init();
        cleanUpDBTables();
    }

    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    /**
     * Tests functionality of the CoordJobMatLookupTriggerService Runnable
     * command. </p> Insert a coordinator job with PREP. Then, runs the
     * CoordJobMatLookupTriggerService runnable and ensures the job status
     * changes to PREMATER.
     *
     * @throws Exception
     */
    public void testCoordJobMatLookupTriggerService1() throws Exception {
        final String jobId = "0000000-" + new Date().getTime() + "-testCoordRecoveryService-C";
        CoordinatorStore store = Services.get().get(StoreService.class).getStore(CoordinatorStore.class);
        store.beginTrx();
        Date start = DateUtils.parseDateUTC("2009-02-01T01:00Z");
        Date end = DateUtils.parseDateUTC("2009-02-03T23:59Z");
        addRecordToJobTable(jobId, store, start, end);
        store.commitTrx();
        store.closeTrx();

        Thread.sleep(3000);
        Runnable runnable = new CoordJobMatLookupTriggerRunnable(3600);
        runnable.run();
        Thread.sleep(6000);

        CoordinatorStore store2 = Services.get().get(StoreService.class).getStore(CoordinatorStore.class);
        store2.beginTrx();
        CoordinatorJobBean action = store2.getCoordinatorJob(jobId, false);
        store2.commitTrx();
        store2.closeTrx();
        if (!(action.getStatus() == CoordinatorJob.Status.PREMATER)) {
            fail();
        }
    }

    private void addRecordToJobTable(String jobId, CoordinatorStore store, Date start, Date end) throws StoreException {
        CoordinatorJobBean coordJob = new CoordinatorJobBean();
        coordJob.setId(jobId);
        coordJob.setAppName("testApp");
        coordJob.setAppPath("testAppPath");
        coordJob.setStatus(CoordinatorJob.Status.PREP);
        coordJob.setCreatedTime(new Date());
        coordJob.setLastModifiedTime(new Date());
        coordJob.setUser("testUser");
        coordJob.setGroup("testGroup");
        coordJob.setAuthToken("notoken");

        String confStr = "<configuration></configuration>";
        coordJob.setConf(confStr);
        String startDateStr = null, endDateStr = null;
        try {
            startDateStr = DateUtils.formatDateUTC(start);
            endDateStr = DateUtils.formatDateUTC(end);
        }
        catch (Exception ex) {
            ex.printStackTrace();
            fail("Could not format dates");
        }
        String appXml = "<coordinator-app xmlns='uri:oozie:coordinator:0.1' name='NAME' frequency=\"1\" start='" + startDateStr + "' end='" + endDateStr + "'";

        appXml += " timezone='UTC' freq_timeunit='DAY' end_of_duration='NONE'>";
        appXml += "<controls>";
        appXml += "<timeout>10</timeout>";
        appXml += "<concurrency>2</concurrency>";
        appXml += "<execution>LIFO</execution>";
        appXml += "</controls>";
        appXml += "<action>";
        appXml += "<workflow>";
        appXml += "<app-path>hdfs:///tmp/workflows/</app-path>";
        appXml += "</workflow>";
        appXml += "</action>";
        appXml += "</coordinator-app>";

        coordJob.setJobXml(appXml);
        coordJob.setLastActionNumber(0);
        coordJob.setFrequency(1);
        coordJob.setExecution(Execution.FIFO);
        coordJob.setConcurrency(1);

        coordJob.setStartTime(start);
        coordJob.setEndTime(end);

        try {
            store.insertCoordinatorJob(coordJob);
        }
        catch (StoreException se) {
            se.printStackTrace();
            store.rollbackTrx();
            fail("Unable to insert the test job record to table");
            throw se;
        }
    }

    /**
     * Test current mode.
     *
     * @throws Exception
     */
    public void testCoordJobMatLookupTriggerService2() throws Exception {
        Date start = new Date();
        Date end = new Date(start.getTime() + 3600 * 1000);
        final String jobId = "0000000-" + start.getTime() + "-testCoordRecoveryService-C";
        CoordinatorStore store = Services.get().get(StoreService.class).getStore(CoordinatorStore.class);
        store.beginTrx();
        addRecordToJobTable(jobId, store, start, end);
        store.commitTrx();
        store.closeTrx();

        Thread.sleep(3000);
        Runnable runnable = new CoordJobMatLookupTriggerRunnable(3600);
        runnable.run();
        Thread.sleep(6000);

        CoordinatorStore store2 = Services.get().get(StoreService.class).getStore(CoordinatorStore.class);
        store2.beginTrx();
        CoordinatorJobBean action = store2.getCoordinatorJob(jobId, false);
        store2.commitTrx();
        store2.closeTrx();
        if (!(action.getStatus() == CoordinatorJob.Status.PREMATER)) {
            fail();
        }
    }
}