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
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.local.LocalOozie;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;

public class TestCoordJobUpdateJPAExecutor extends XDataTestCase {
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

    public void testCoordJobUpdate() throws Exception {
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);
        _testUpdateJob(job.getId());
    }

    private void _testUpdateJob(String jobId) throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        CoordJobGetJPAExecutor jobGetCmd = new CoordJobGetJPAExecutor(jobId);
        CoordinatorJobBean job1 = jpaService.execute(jobGetCmd);
        job1.setStatus(CoordinatorJob.Status.SUCCEEDED);
        CoordJobUpdateJPAExecutor coordJobUpdateCommand = new CoordJobUpdateJPAExecutor(job1);
        jpaService.execute(coordJobUpdateCommand);

        CoordinatorJobBean job2 = jpaService.execute(jobGetCmd);
        assertEquals(job2.getStatus(), CoordinatorJob.Status.SUCCEEDED);
    }

}
