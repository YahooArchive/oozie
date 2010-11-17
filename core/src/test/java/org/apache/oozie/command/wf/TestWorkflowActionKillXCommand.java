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
package org.apache.oozie.command.wf;

import java.util.Date;

import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.action.hadoop.MapperReducerForTest;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.jpa.WorkflowActionGetCommand;
import org.apache.oozie.command.jpa.WorkflowActionInsertCommand;
import org.apache.oozie.command.jpa.WorkflowJobGetCommand;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.workflow.WorkflowInstance;

public class TestWorkflowActionKillXCommand extends XDataTestCase {
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
     * Test : kill action successfully.
     *
     * @throws Exception
     */
    public void testWfActionKillSuccess() throws Exception {
        final String wfId = "0000000-" + new Date().getTime() + "-testWfKill-W";
        final int actionNum = 1;
        final String actionId = wfId + "@" + actionNum;

        this.addRecordToWfJobTable(wfId, WorkflowJob.Status.KILLED, WorkflowInstance.Status.KILLED);
        this.addRecordToWfActionTable(wfId, actionNum, WorkflowAction.Status.KILLED);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        WorkflowJobGetCommand wfJobGetCmd = new WorkflowJobGetCommand(wfId);
        WorkflowActionGetCommand wfActionGetCmd = new WorkflowActionGetCommand(actionId);

        WorkflowActionBean action = jpaService.execute(wfActionGetCmd);
        assertEquals(action.getStatus(), WorkflowAction.Status.KILLED);
        assertEquals(action.getExternalStatus(), "RUNNING");

        new WorkflowActionKillXCommand(actionId).call();

        action = jpaService.execute(wfActionGetCmd);
        assertEquals(action.getStatus(), WorkflowAction.Status.KILLED);
        assertEquals(action.getExternalStatus(), "KILLED");
    }

    /**
     * Test : kill a non-killed action. Will throw the exception from {@link
     * WorkflowActionKillXCommand.verifyPrecondition()}
     *
     * @throws Exception
     */
    public void testWfActionKillFailed() throws Exception {
        final String wfId = "0000000-" + new Date().getTime() + "-testWfKill-W";
        final int actionNum = 1;
        final String actionId = wfId + "@" + actionNum;

        this.addRecordToWfJobTable(wfId, WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING);
        this.addRecordToWfActionTable(wfId, actionNum, WorkflowAction.Status.RUNNING);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        WorkflowActionGetCommand wfActionGetCmd = new WorkflowActionGetCommand(actionId);

        WorkflowActionBean action = jpaService.execute(wfActionGetCmd);
        assertEquals(action.getStatus(), WorkflowAction.Status.RUNNING);
        assertEquals(action.getExternalStatus(), "RUNNING");

        new WorkflowActionKillXCommand(actionId).call();

        // action is not in KILLED, action status must not change
        action = jpaService.execute(wfActionGetCmd);
        assertEquals(action.getStatus(), WorkflowAction.Status.RUNNING);
        assertEquals(action.getExternalStatus(), "RUNNING");
    }

    @Override
    protected String addRecordToWfActionTable(String wfId, int actionNum, WorkflowAction.Status status) throws Exception {
        WorkflowActionBean action = new WorkflowActionBean();
        String actionId = wfId + "@" + actionNum;
        action.setId(actionId);
        action.setJobId(wfId);
        action.setName("testAction");
        action.setType("map-reduce");
        action.setStatus(status);
        action.setStartTime(new Date());
        action.setEndTime(new Date());
        action.setLastCheckTime(new Date());
        action.setPending();
        action.setExternalId("job_201011110000_00000");
        action.setExternalStatus("RUNNING");

        String actionXml = "<map-reduce>" +
        "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
        "<name-node>" + getNameNodeUri() + "</name-node>" +
        "<configuration>" +
        "<property><name>mapred.mapper.class</name><value>" + MapperReducerForTest.class.getName() +
        "</value></property>" +
        "<property><name>mapred.reducer.class</name><value>" + MapperReducerForTest.class.getName() +
        "</value></property>" +
        "<property><name>mapred.input.dir</name><value>inputDir</value></property>" +
        "<property><name>mapred.output.dir</name><value>outputDir</value></property>" +
        "</configuration>" +
        "</map-reduce>";
        action.setConf(actionXml);

        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            assertNotNull(jpaService);
            WorkflowActionInsertCommand actionInsertCmd = new WorkflowActionInsertCommand(action);
            jpaService.execute(actionInsertCmd);
        }
        catch (CommandException ce) {
            ce.printStackTrace();
            fail("Unable to insert the test wf action record to table");
            throw ce;
        }
        return actionId;
    }

}
