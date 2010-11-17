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

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.jpa.CoordActionGetCommand;
import org.apache.oozie.command.jpa.CoordJobGetCommand;
import org.apache.oozie.local.LocalOozie;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;

public class TestCoordKillXCommand extends XDataTestCase {
    private Services services;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        services.init();
        cleanUpDBTables();
        LocalOozie.start();
    }

    @Override
    protected void tearDown() throws Exception {
        LocalOozie.stop();
        services.destroy();
        super.tearDown();
    }

    /**
     * Test : kill job and action successfully
     *
     * @throws Exception
     */
    public void testCoordKillSuccess() throws Exception {
        final String jobId = "0000000-" + new Date().getTime() + "-testCoordKill-C";
        final int actionNum = 1;
        final String actionId = jobId + "@" + actionNum;

        addRecordToCoordJobTable(jobId, CoordinatorJob.Status.SUCCEEDED);
        addRecordToCoordActionTable(jobId, actionNum, CoordinatorAction.Status.READY, "coord-rerun-action1.xml");

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        CoordJobGetCommand coordJobGetCmd = new CoordJobGetCommand(jobId);
        CoordActionGetCommand coordActionGetCmd = new CoordActionGetCommand(actionId);

        CoordinatorJobBean job = jpaService.execute(coordJobGetCmd);
        CoordinatorActionBean action = jpaService.execute(coordActionGetCmd);
        assertEquals(job.getStatus(), CoordinatorJob.Status.SUCCEEDED);
        assertEquals(action.getStatus(), CoordinatorAction.Status.READY);

        new CoordKillXCommand(jobId).call();

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
        final String jobId = "0000000-" + new Date().getTime() + "-testCoordKill-C";
        final int actionNum = 1;
        final String actionId = jobId + "@" + actionNum;

        addRecordToCoordJobTable(jobId, CoordinatorJob.Status.SUCCEEDED);
        addRecordToCoordActionTable(jobId, actionNum, CoordinatorAction.Status.SUCCEEDED, "coord-rerun-action1.xml");

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        CoordJobGetCommand coordJobGetCmd = new CoordJobGetCommand(jobId);
        CoordActionGetCommand coordActionGetCmd = new CoordActionGetCommand(actionId);

        CoordinatorJobBean job = jpaService.execute(coordJobGetCmd);
        CoordinatorActionBean action = jpaService.execute(coordActionGetCmd);
        assertEquals(job.getStatus(), CoordinatorJob.Status.SUCCEEDED);
        assertEquals(action.getStatus(), CoordinatorAction.Status.SUCCEEDED);

        new CoordKillXCommand(jobId).call();

        job = jpaService.execute(coordJobGetCmd);
        action = jpaService.execute(coordActionGetCmd);
        assertEquals(job.getStatus(), CoordinatorJob.Status.KILLED);
        assertEquals(action.getStatus(), CoordinatorAction.Status.SUCCEEDED);
    }

    /**
     * Test : kill job failed. Job does not exist.
     *
     * @throws Exception
     */
    public void testCoordKillFailed() throws Exception {
        final String jobId = "0000000-" + new Date().getTime() + "-testCoordKill-C";
        final String testJobId = "0000001-" + new Date().getTime() + "-testCoordKill-C";
        final int actionNum = 1;
        final String actionId = jobId + "@" + actionNum;

        addRecordToCoordJobTable(jobId, CoordinatorJob.Status.SUCCEEDED);
        addRecordToCoordActionTable(jobId, actionNum, CoordinatorAction.Status.READY, "coord-rerun-action1.xml");

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        CoordJobGetCommand coordJobGetCmd = new CoordJobGetCommand(jobId);
        CoordActionGetCommand coordActionGetCmd = new CoordActionGetCommand(actionId);

        CoordinatorJobBean job = jpaService.execute(coordJobGetCmd);
        CoordinatorActionBean action = jpaService.execute(coordActionGetCmd);
        assertEquals(job.getStatus(), CoordinatorJob.Status.SUCCEEDED);
        assertEquals(action.getStatus(), CoordinatorAction.Status.READY);

        try {
            new CoordKillXCommand(testJobId).call();
            fail("Job doesn't exist. Should fail.");
        } catch (CommandException ce) {
            if (ce.getErrorCode() != ErrorCode.E0604) {
                fail("Error code should be E0604.");
            }
        }
    }

}