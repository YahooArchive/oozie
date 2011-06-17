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

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.executor.jpa.CoordActionGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobUpdateJPAExecutor;
import org.apache.oozie.service.CallableQueueService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.SchemaService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.StatusTransitService;
import org.apache.oozie.test.XDataTestCase;

public class TestCoordKillXCommand extends XDataTestCase {
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
     * Test : kill job and action (READY) successfully
     *
     * @throws Exception
     */
    public void testCoordKillSuccess1() throws Exception {
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);
        CoordinatorActionBean action = addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.READY, "coord-action-get.xml", 0);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        CoordJobGetJPAExecutor coordJobGetCmd = new CoordJobGetJPAExecutor(job.getId());
        CoordActionGetJPAExecutor coordActionGetCmd = new CoordActionGetJPAExecutor(action.getId());

        job = jpaService.execute(coordJobGetCmd);
        action = jpaService.execute(coordActionGetCmd);
        assertEquals(job.getStatus(), CoordinatorJob.Status.RUNNING);
        assertEquals(action.getStatus(), CoordinatorAction.Status.READY);

        new CoordKillXCommand(job.getId()).call();

        job = jpaService.execute(coordJobGetCmd);
        action = jpaService.execute(coordActionGetCmd);
        assertEquals(job.getStatus(), CoordinatorJob.Status.KILLED);
        assertEquals(action.getStatus(), CoordinatorAction.Status.KILLED);
    }

    /**
     * Test : kill job and action (RUNNING) successfully
     *
     * @throws Exception
     */
    public void testCoordKillSuccess2() throws Exception {
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);
        CoordinatorActionBean action = addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.RUNNING, "coord-action-get.xml", 0);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        CoordJobGetJPAExecutor coordJobGetCmd = new CoordJobGetJPAExecutor(job.getId());
        CoordActionGetJPAExecutor coordActionGetCmd = new CoordActionGetJPAExecutor(action.getId());

        job = jpaService.execute(coordJobGetCmd);
        action = jpaService.execute(coordActionGetCmd);
        assertEquals(job.getStatus(), CoordinatorJob.Status.RUNNING);
        assertEquals(action.getStatus(), CoordinatorAction.Status.RUNNING);

        new CoordKillXCommand(job.getId()).call();

        job = jpaService.execute(coordJobGetCmd);
        action = jpaService.execute(coordActionGetCmd);
        assertEquals(job.getStatus(), CoordinatorJob.Status.KILLED);
        assertEquals(action.getStatus(), CoordinatorAction.Status.KILLED);
    }

    /**
     * Test : kill job successfully but failed to kill an already successful action
     *
     * @throws Exception
     */
    public void testCoordKillFailedOnAction() throws Exception {
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        CoordinatorActionBean action = addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        CoordJobGetJPAExecutor coordJobGetCmd = new CoordJobGetJPAExecutor(job.getId());
        CoordActionGetJPAExecutor coordActionGetCmd = new CoordActionGetJPAExecutor(action.getId());

        job = jpaService.execute(coordJobGetCmd);
        action = jpaService.execute(coordActionGetCmd);
        assertEquals(job.getStatus(), CoordinatorJob.Status.SUCCEEDED);
        assertEquals(action.getStatus(), CoordinatorAction.Status.SUCCEEDED);

        new CoordKillXCommand(job.getId()).call();

        job = jpaService.execute(coordJobGetCmd);
        action = jpaService.execute(coordActionGetCmd);
        assertEquals(job.getStatus(), CoordinatorJob.Status.SUCCEEDED);
        assertEquals(action.getStatus(), CoordinatorAction.Status.SUCCEEDED);
    }

    /**
     * Test : kill SUCCEEDED job successfully when CONF_BACKWARD_SUPPORT_FOR_COORD_STATUS is true and coordinator schema
     * is 0.1
     *
     * @throws Exception
     */
    public void testCoordKillForBackwardSupport() throws Exception {
        Services.get().destroy();
        setSystemProperty(StatusTransitService.CONF_BACKWARD_SUPPORT_FOR_COORD_STATUS, "true");
        new Services().init();

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        CoordinatorActionBean action = addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.RUNNING, "coord-action-get.xml", 0);

        job.setAppNamespace(SchemaService.COORDINATOR_NAMESPACE_URI_1);
        jpaService.execute(new CoordJobUpdateJPAExecutor(job));

        CoordJobGetJPAExecutor coordJobGetCmd = new CoordJobGetJPAExecutor(job.getId());
        CoordActionGetJPAExecutor coordActionGetCmd = new CoordActionGetJPAExecutor(action.getId());

        job = jpaService.execute(coordJobGetCmd);
        action = jpaService.execute(coordActionGetCmd);
        assertEquals(CoordinatorJob.Status.SUCCEEDED, job.getStatus());
        assertEquals(CoordinatorAction.Status.RUNNING, action.getStatus());

        new CoordKillXCommand(job.getId()).call();

        job = jpaService.execute(coordJobGetCmd);
        action = jpaService.execute(coordActionGetCmd);
        assertEquals(CoordinatorJob.Status.KILLED, job.getStatus());
        assertEquals(CoordinatorAction.Status.KILLED, action.getStatus());
    }


    /**
     * Test : kill job failed. Job does not exist.
     *
     * @throws Exception
     */
    public void testCoordKillFailed() throws Exception {
        final String testJobId = "0000001-" + new Date().getTime() + "-testCoordKill-C";

        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        CoordinatorActionBean action = addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.READY, "coord-action-get.xml", 0);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        CoordJobGetJPAExecutor coordJobGetCmd = new CoordJobGetJPAExecutor(job.getId());
        CoordActionGetJPAExecutor coordActionGetCmd = new CoordActionGetJPAExecutor(action.getId());

        job = jpaService.execute(coordJobGetCmd);
        action = jpaService.execute(coordActionGetCmd);
        assertEquals(job.getStatus(), CoordinatorJob.Status.SUCCEEDED);
        assertEquals(action.getStatus(), CoordinatorAction.Status.READY);

        try {
            new CoordKillXCommand(testJobId).call();
            fail("Job doesn't exist. Should fail.");
        } catch (CommandException ce) {
            //Job doesn't exist. Exception is expected.
        }
    }

    public class MyCoordKillXCommand extends CoordKillXCommand {
        long executed = 0;
        int wait;

        public MyCoordKillXCommand(String jobId, int wait) {
            super(jobId);
            this.wait = wait;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("Type:").append(getType());
            sb.append(",Priority:").append(getPriority());
            return sb.toString();
        }

        @Override
        protected Void execute() throws CommandException {
            try {
                Thread.sleep(wait);
            }
            catch (InterruptedException e) {
            }
            executed = System.currentTimeMillis();
            return null;
        }

    }

    public void testCoordKillXCommandUniqueness() throws Exception {

        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);

        final MyCoordKillXCommand callable1 = new MyCoordKillXCommand(job.getId(), 100);
        final MyCoordKillXCommand callable2 = new MyCoordKillXCommand(job.getId(), 100);
        final MyCoordKillXCommand callable3 = new MyCoordKillXCommand(job.getId(), 100);

        List<MyCoordKillXCommand> callables = Arrays.asList(callable1, callable2, callable3);

        CallableQueueService queueservice = services.get(CallableQueueService.class);

        for (MyCoordKillXCommand c : callables) {
            queueservice.queue(c);
        }

        waitFor(200, new Predicate() {
            public boolean evaluate() throws Exception {
                return callable1.executed != 0 && callable2.executed == 0 && callable3.executed == 0;
            }
        });

        assertTrue(callable1.executed != 0);
        assertTrue(callable2.executed == 0);
        assertTrue(callable3.executed == 0);
    }

}