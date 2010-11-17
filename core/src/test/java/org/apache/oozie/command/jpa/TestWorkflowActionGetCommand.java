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

import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;

public class TestWorkflowActionGetCommand extends XDataTestCase {
    Services services;

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

    public void testWfJobGet() throws Exception {
        String jobId = "00000-" + new Date().getTime() + "-TestWorkflowActionGetCommand-W";
        int actionNum = 1;
        String actionId = jobId + "@" + actionNum;
        addRecordToWfActionTable(jobId, actionNum, WorkflowAction.Status.PREP);
        _testGetAction(actionId);
    }

    private void _testGetAction(String actionId) throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        WorkflowActionGetCommand actionGetCmd = new WorkflowActionGetCommand(actionId);
        WorkflowActionBean ret = jpaService.execute(actionGetCmd);
        assertNotNull(ret);
        assertEquals(ret.getId(), actionId);
    }

}