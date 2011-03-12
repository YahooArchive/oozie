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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorEngine;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.DagEngine;
import org.apache.oozie.ForTestingActionExecutor;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.CoordinatorJob.Execution;
import org.apache.oozie.service.RecoveryService.RecoveryRunnable;
import org.apache.oozie.store.CoordinatorStore;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.store.WorkflowStore;
import org.apache.oozie.test.XTestCase;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XConfiguration;

public class TestRecoveryService extends XTestCase {
    private Services services;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        setSystemProperty(SchemaService.WF_CONF_EXT_SCHEMAS, "wf-ext-schema.xsd");
        services = new Services();
        services.init();
        cleanUpDBTables();
        services.get(ActionService.class).register(ForTestingActionExecutor.class);
    }

    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    /**
     * Tests functionality of the Recovery Service Runnable command. </p> Starts an action which behaves like an Async
     * Action (Action and Job state set to Running). Changes the action configuration to run in sync mode and updates
     * the store. Runs the recovery runnable, and ensures the state of the action and job have not changed. </p> Changes
     * the state of the action from RUNNING to PREP and updates the store. Again, runs the recovery runnable and ensures
     * the state changes to OK and the job completes successfully.
     *
     * @throws Exception
     */
    public void testActionRecoveryService() throws Exception {
        Reader reader = IOUtils.getResourceAsReader("wf-ext-schema-valid.xml", -1);
        Writer writer = new FileWriter(getTestCaseDir() + "/workflow.xml");
        createTestCaseSubDir("lib");
        IOUtils.copyCharStream(reader, writer);

        final DagEngine engine = new DagEngine(getTestUser(), "a");
        Configuration conf = new XConfiguration();
        conf.set(OozieClient.APP_PATH, getTestCaseDir() + File.separator + "workflow.xml");
        conf.set(OozieClient.USER_NAME, getTestUser());
        conf.set(OozieClient.GROUP_NAME, getTestGroup());
        injectKerberosInfo(conf);
        conf.set(OozieClient.LOG_TOKEN, "t");

        conf.set("external-status", "ok");
        conf.set("signal-value", "based_on_action_status");
        conf.set("running-mode", "async");

        //TODO CHECK, without this we get JPA concurrency exceptions, ODD
        Thread.sleep(1000);

        final String jobId = engine.submitJob(conf, true);

        //TODO CHECK, without this we get JPA concurrency exceptions, ODD
        Thread.sleep(1000);

        waitFor(5000, new Predicate() {
            public boolean evaluate() throws Exception {
                return (engine.getJob(jobId).getStatus() == WorkflowJob.Status.RUNNING);
            }
        });

        Thread.sleep(1000);
        final WorkflowStore store = Services.get().get(WorkflowStoreService.class).create();
        store.beginTrx();
        List<WorkflowActionBean> actions = store.getActionsForWorkflow(jobId, false);
        WorkflowActionBean action = actions.get(0);
        final String actionId = action.getId();
        assertEquals(WorkflowActionBean.Status.RUNNING, action.getStatus());
        String actionConf = action.getConf();
        String fixedActionConf = actionConf.replaceAll("async", "sync");
        action.setConf(fixedActionConf);
        action.setPending();
        store.updateAction(action);
        store.commitTrx();
        store.closeTrx();

        Runnable recoveryRunnable = new RecoveryRunnable(0, 60, 60);
        recoveryRunnable.run();
        Thread.sleep(3000);

        final WorkflowStore store2 = Services.get().get(WorkflowStoreService.class).create();
        assertEquals(WorkflowJob.Status.RUNNING, engine.getJob(jobId).getStatus());
        store2.beginTrx();
        WorkflowActionBean action2 = store2.getAction(actionId, false);
        assertEquals(WorkflowActionBean.Status.RUNNING, action2.getStatus());
        action2.setStatus(WorkflowActionBean.Status.PREP);
        action2.setPending();
        store2.updateAction(action2);
        store2.commitTrx();
        store2.closeTrx();

        Thread.sleep(1000);
        recoveryRunnable.run();
        Thread.sleep(3000);

        waitFor(10000, new Predicate() {
            public boolean evaluate() throws Exception {
                return (engine.getWorkflowAction(actionId).getStatus() == WorkflowActionBean.Status.OK);
            }
        });

        // getPendingActions works correctly only with MYSQL - following assertsfail with hsql - to be investigated
        // assertEquals(WorkflowJob.Status.SUCCEEDED, engine.getJob(jobId).getStatus());
        final WorkflowStore store3 = Services.get().get(WorkflowStoreService.class).create();
        store3.beginTrx();
        WorkflowActionBean action3 = store3.getAction(actionId, false);
        assertEquals(WorkflowActionBean.Status.OK, action3.getStatus());
        store3.commitTrx();
        store3.closeTrx();
    }

    /**
     * Tests functionality of the Recovery Service Runnable command. </p> Insert a coordinator job with PREPMATER and
     * action with SUBMITTED. Then, runs the recovery runnable and ensures the action status changes to READY.
     *
     * @throws Exception
     */
    public void testCoordActionRecoveryService() throws Exception {
        final String jobId = "0000000-" + new Date().getTime() + "-testCoordRecoveryService-C";
        final int actionNum = 1;
        final String actionId = jobId + "@" + actionNum;
        final CoordinatorEngine ce = new CoordinatorEngine(getTestUser(), "UNIT_TESTING");
        CoordinatorStore store = Services.get().get(StoreService.class).getStore(CoordinatorStore.class);
        store.beginTrx();
        try {
            createTestCaseSubDir("no-op");
            createTestCaseSubDir("no-op/lib");
            createTestCaseSubDir("workflows");
            createTestCaseSubDir("in");
            addRecordToJobTable(jobId, store, getTestCaseDir());
            addRecordToActionTable(jobId, actionNum, actionId, store, getTestCaseDir());
            store.commitTrx();
        }
        finally {
            store.closeTrx();
        }

        Thread.sleep(3000);
        Runnable recoveryRunnable = new RecoveryRunnable(0, 1,1);
        recoveryRunnable.run();

        waitFor(10000, new Predicate() {
            public boolean evaluate() throws Exception {
                CoordinatorActionBean bean = ce.getCoordAction(actionId);
                return (bean.getStatus() == CoordinatorAction.Status.RUNNING || bean.getStatus() == CoordinatorAction.Status.SUCCEEDED);
            }
        });

        CoordinatorStore store2 = Services.get().get(StoreService.class).getStore(CoordinatorStore.class);
        store2.beginTrx();
        CoordinatorActionBean action = store2.getCoordinatorAction(actionId, false);
        if (action.getStatus() == CoordinatorAction.Status.RUNNING
                || action.getStatus() == CoordinatorAction.Status.SUCCEEDED) {

        }
        else {
            fail();
        }
        store2.commitTrx();
        store2.closeTrx();
    }

    private void addRecordToActionTable(String jobId, int actionNum, String actionId, CoordinatorStore store, String baseDir) throws StoreException, IOException {
        CoordinatorActionBean action = new CoordinatorActionBean();
        action.setJobId(jobId);
        action.setId(actionId);
        action.setActionNumber(actionNum);
        action.setNominalTime(new Date());
        action.setLastModifiedTime(new Date());
        action.setStatus(CoordinatorAction.Status.SUBMITTED);
        String appPath = baseDir + "/no-op/workflow.xml";
        String baseURI = baseDir + "/workflows";
        String actionXml = "<coordinator-app xmlns='uri:oozie:coordinator:0.1' xmlns:sla='uri:oozie:sla:0.1' name='NAME' frequency=\"1\" start='2009-02-01T01:00Z' end='2009-02-03T23:59Z' timezone='UTC' freq_timeunit='DAY' end_of_duration='NONE'  instance-number=\"1\" action-nominal-time=\"2009-02-01T01:00Z\">";
        actionXml += "<controls>";
        actionXml += "<timeout>10</timeout>";
        actionXml += "<concurrency>2</concurrency>";
        actionXml += "<execution>LIFO</execution>";
        actionXml += "</controls>";
        actionXml += "<input-events>";
        actionXml += "<data-in name='A' dataset='a'>";
        actionXml += "<dataset name='a' frequency='7' initial-instance='2009-02-01T01:00Z' timezone='UTC' freq_timeunit='DAY' end_of_duration='NONE'>";
        actionXml += "<uri-template>file://" + baseURI + "/workflows/${YEAR}/${DAY}</uri-template>";
        actionXml += "</dataset>";
        actionXml += "<instance>${coord:latest(0)}</instance>";
        actionXml += "</data-in>";
        actionXml += "</input-events>";
        actionXml += "<output-events>";
        actionXml += "<data-out name='LOCAL_A' dataset='local_a'>";
        actionXml += "<dataset name='local_a' frequency='7' initial-instance='2009-02-01T01:00Z' timezone='UTC' freq_timeunit='DAY' end_of_duration='NONE'>";
        actionXml += "<uri-template>file://" + baseURI + "/${YEAR}/${DAY}</uri-template>";
        actionXml += "</dataset>";
        actionXml += "<instance>${coord:current(-1)}</instance>";
        actionXml += "</data-out>";
        actionXml += "</output-events>";
        actionXml += "<action>";
        actionXml += "<workflow>";
        actionXml += "<app-path>file://" + appPath + "</app-path>";
        actionXml += "<configuration>";
        actionXml += "<property>";
        actionXml += "<name>inputA</name>";
        actionXml += "<value>file://" + baseURI + "/US/2009/02/01</value>";
        actionXml += "</property>";
        actionXml += "<property>";
        actionXml += "<name>inputB</name>";
        actionXml += "<value>file://" + baseURI + "/US/2009/02/01</value>";
        actionXml += "</property>";
        actionXml += "</configuration>";
        actionXml += "</workflow>";
        actionXml += "</action>";
        actionXml += "</coordinator-app>";
        action.setActionXml(actionXml);

        String createdConf = "<configuration> ";
        createdConf += "<property> <name>execution_order</name> <value>LIFO</value> </property>";
        createdConf += "<property> <name>user.name</name> <value>" + getTestUser() + "</value> </property>";
        createdConf += "<property> <name>group.name</name> <value>other</value> </property>";
        createdConf += "<property> <name>app-path</name> " + "<value>file://" + appPath + "</value> </property>";
        createdConf += "<property> <name>jobTracker</name> ";
        createdConf += "<value>localhost:9001</value></property>";
        createdConf += "<property> <name>nameNode</name> <value>hdfs://localhost:9000</value></property>";
        createdConf += "<property> <name>queueName</name> <value>default</value></property>";

        createdConf += "</configuration> ";

        XConfiguration conf = new XConfiguration(new StringReader(createdConf));
        injectKerberosInfo(conf);
        createdConf = conf.toXmlString(false);

        action.setCreatedConf(createdConf);
        store.insertCoordinatorAction(action);
        String content = "<workflow-app xmlns='uri:oozie:workflow:0.1'  xmlns:sla='uri:oozie:sla:0.1' name='no-op-wf'>";
        content += "<start to='end' />";
        content += "<end name='end' /></workflow-app>";
        writeToFile(content, baseDir + "/no-op/");
    }

    private void writeToFile(String content, String appPath) throws IOException {
        createDir(appPath);
        File wf = new File(appPath + "/workflow.xml");
        PrintWriter out = null;
        try {
            out = new PrintWriter(new FileWriter(wf));
            out.println(content);
        }
        catch (IOException iex) {
            iex.printStackTrace();
            throw iex;
        }
        finally {
            if (out != null) {
                out.close();
            }
        }

    }

    private void createDir(String dir) {
        Process pr;
        try {
            pr = Runtime.getRuntime().exec("mkdir -p " + dir + "/_SUCCESS");
            pr.waitFor();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void addRecordToJobTable(String jobId, CoordinatorStore store, String baseDir) throws StoreException {
        CoordinatorJobBean coordJob = new CoordinatorJobBean();
        coordJob.setId(jobId);
        coordJob.setAppName("testApp");
        coordJob.setAppPath("testAppPath");
        coordJob.setStatus(CoordinatorJob.Status.PREMATER);
        coordJob.setCreatedTime(new Date());
        coordJob.setLastModifiedTime(new Date());
        coordJob.setUser(getTestUser());
        coordJob.setGroup(getTestGroup());
        coordJob.setAuthToken("notoken");
        coordJob.setTimeZone("UTC");

        String baseURI = baseDir + "/workflows";
        String confStr = "<configuration></configuration>";
        coordJob.setConf(confStr);
        String appXml = "<coordinator-app xmlns='uri:oozie:coordinator:0.1' name='NAME' frequency=\"1\" start='2009-02-01T01:00Z' end='2009-02-03T23:59Z'";
        appXml += " timezone='UTC' freq_timeunit='DAY' end_of_duration='NONE'>";
        appXml += "<controls>";
        appXml += "<timeout>10</timeout>";
        appXml += "<concurrency>2</concurrency>";
        appXml += "<execution>LIFO</execution>";
        appXml += "</controls>";
        appXml += "<input-events>";
        appXml += "<data-in name='A' dataset='a'>";
        appXml += "<dataset name='a' frequency='7' initial-instance='2009-02-01T01:00Z' timezone='UTC' freq_timeunit='DAY' end_of_duration='NONE'>";
        appXml += "<uri-template>file://" + baseURI + "/${YEAR}/${DAY}</uri-template>";
        appXml += "</dataset>";
        appXml += "<instance>${coord:latest(0)}</instance>";
        appXml += "</data-in>";
        appXml += "</input-events>";
        appXml += "<output-events>";
        appXml += "<data-out name='LOCAL_A' dataset='local_a'>";
        appXml += "<dataset name='local_a' frequency='7' initial-instance='2009-02-01T01:00Z' timezone='UTC' freq_timeunit='DAY' end_of_duration='NONE'>";
        appXml += "<uri-template>file://" + baseURI + "/${YEAR}/${DAY}</uri-template>";
        appXml += "</dataset>";
        appXml += "<instance>${coord:current(-1)}</instance>";
        appXml += "</data-out>";
        appXml += "</output-events>";
        appXml += "<action>";
        appXml += "<workflow>";
        appXml += "<app-path>file://" + baseURI + "</app-path>";
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
        coordJob.setFrequency(1);
        coordJob.setExecution(Execution.FIFO);
        coordJob.setConcurrency(1);
        try {
            coordJob.setEndTime(DateUtils.parseDateUTC("2009-02-03T23:59Z"));
            coordJob.setStartTime(DateUtils.parseDateUTC("2009-02-01T23:59Z"));
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Could not set Date/time");
        }

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
}
