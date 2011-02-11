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
package org.apache.oozie.executor.jpa;

import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.local.LocalOozie;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;

public class TestCoordActionsActiveCountJPAExecutor extends XDataTestCase {
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

    public void testCoordActionGet() throws Exception {
        int actionNum = 1;
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING);
        addRecordToCoordActionTable(job.getId(), actionNum++, CoordinatorAction.Status.WAITING, "coord-action-get.xml");
        _testActiveActionsCount(job.getId(), 1);
        addRecordToCoordActionTable(job.getId(), actionNum++, CoordinatorAction.Status.RUNNING, "coord-action-get.xml");

        addRecordToCoordActionTable(job.getId(), actionNum, CoordinatorAction.Status.WAITING, "coord-action-get.xml");
        _testActiveActionsCount(job.getId(), 2);
    }

    private void _testActiveActionsCount(String jobId, int expected) throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        CoordActionsActiveCountJPAExecutor actionActiveCmd = new CoordActionsActiveCountJPAExecutor(jobId);
        int cnt = jpaService.execute(actionActiveCmd);
        assertEquals(cnt, expected);
    }

}