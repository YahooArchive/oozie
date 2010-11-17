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

import java.util.Date;
import java.util.List;

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.local.LocalOozie;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;

public class TestCoordActionsGetForJobCommand extends XDataTestCase {
    Services services;

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

    public void testCoordActionsGetForJob() throws Exception {
        String jobId = "00000-" + new Date().getTime() + "-TestCoordActionsGetForJobCommand-C";
        _insertActions(jobId);
        _testGetActions(jobId);
        _testGetActionForJobInFIFO(jobId);
        _testGetActionForJobInLIFO(jobId);
    }

    private void _insertActions(String jobId) throws Exception {
        addRecordToCoordActionTable(jobId, 1, CoordinatorAction.Status.READY, "coord-action-get.xml");
        addRecordToCoordActionTable(jobId, 2, CoordinatorAction.Status.READY, "coord-action-get.xml");
    }

    private void _testGetActions(String jobId) throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        CoordActionsGetForJobCommand coordActionsGetCmd = new CoordActionsGetForJobCommand(jobId);
        List<CoordinatorActionBean> actions = jpaService.execute(coordActionsGetCmd);
        assertEquals(actions.size(), 2);
    }

    private void _testGetActionForJobInFIFO(String jobId) throws CommandException {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        CoordActionsGetForJobCommand coordActionsGetCmd = new CoordActionsGetForJobCommand(jobId, 20, "FIFO");
        List<CoordinatorActionBean> actions = jpaService.execute(coordActionsGetCmd);
        assertEquals(actions.size(), 2);
    }

    private void _testGetActionForJobInLIFO(String jobId) throws CommandException {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        CoordActionsGetForJobCommand coordActionsGetCmd = new CoordActionsGetForJobCommand(jobId, 20, "LIFO");
        List<CoordinatorActionBean> actions = jpaService.execute(coordActionsGetCmd);
        assertEquals(actions.size(), 2);
    }

}
