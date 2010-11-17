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
package org.apache.oozie.test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.util.Date;
import java.util.Properties;
import java.util.regex.Matcher;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.action.hadoop.MapperReducerForTest;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.CoordinatorJob.Execution;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.jpa.CoordActionInsertCommand;
import org.apache.oozie.command.jpa.CoordJobInsertCommand;
import org.apache.oozie.command.jpa.WorkflowActionInsertCommand;
import org.apache.oozie.command.jpa.WorkflowJobInsertCommand;
import org.apache.oozie.local.LocalOozie;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.WorkflowAppService;
import org.apache.oozie.service.WorkflowStoreService;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XmlUtils;
import org.apache.oozie.workflow.WorkflowApp;
import org.apache.oozie.workflow.WorkflowInstance;
import org.apache.oozie.workflow.WorkflowLib;
import org.apache.oozie.workflow.lite.EndNodeDef;
import org.apache.oozie.workflow.lite.LiteWorkflowApp;
import org.apache.oozie.workflow.lite.LiteWorkflowInstance;
import org.apache.oozie.workflow.lite.StartNodeDef;
import org.jdom.Element;
import org.jdom.JDOMException;

public class XDataTestCase extends XFsTestCase {

    /**
     * Insert coord job for testing.
     *
     * @param jobId
     * @param status
     * @throws StoreException
     * @throws IOException
     * @throws CommandException
     */
    protected void addRecordToCoordJobTable(String jobId, CoordinatorJob.Status status) throws CommandException,
            IOException {
        Path appPath = new Path(getFsTestCaseDir(), "coord");
        String appXml = getCoordJobXml(appPath);

        FileSystem fs = getFileSystem();

        Writer writer = new OutputStreamWriter(fs.create(new Path(appPath + "/coordinator.xml")));
        byte[] bytes = appXml.getBytes("UTF-8");
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        Reader reader2 = new InputStreamReader(bais);
        IOUtils.copyCharStream(reader2, writer);

        CoordinatorJobBean coordJob = new CoordinatorJobBean();
        coordJob.setId(jobId);
        coordJob.setAppName("COORD-TEST");
        coordJob.setAppPath(appPath.toString());
        coordJob.setStatus(status);
        coordJob.setCreatedTime(new Date());
        coordJob.setLastModifiedTime(new Date());
        coordJob.setUser(getTestUser());
        coordJob.setGroup(getTestGroup());
        coordJob.setAuthToken("notoken");

        Properties conf = getCoordConf(appPath);
        String confStr = XmlUtils.writePropToString(conf);

        coordJob.setConf(confStr);
        coordJob.setJobXml(appXml);
        coordJob.setLastActionNumber(0);
        coordJob.setFrequency(1);
        coordJob.setExecution(Execution.FIFO);
        coordJob.setConcurrency(1);
        try {
            coordJob.setStartTime(DateUtils.parseDateUTC("2009-12-15T01:00Z"));
            coordJob.setEndTime(DateUtils.parseDateUTC("2009-12-17T01:00Z"));
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Could not set Date/time");
        }

        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            assertNotNull(jpaService);
            CoordJobInsertCommand coordInsertCmd = new CoordJobInsertCommand(coordJob);
            jpaService.execute(coordInsertCmd);
        }
        catch (CommandException ce) {
            ce.printStackTrace();
            fail("Unable to insert the test coord job record to table");
            throw ce;
        }

    }

    /**
     * Insert coord action for testing.
     *
     * @param jobId
     * @param actionNum
     * @param status
     * @param resourceXmlName
     * @return TODO
     * @throws StoreException
     * @throws IOException
     */
    protected String addRecordToCoordActionTable(String jobId, int actionNum, CoordinatorAction.Status status,
            String resourceXmlName) throws CommandException, IOException {
        String actionId = jobId + "@" + actionNum;
        Path appPath = new Path(getFsTestCaseDir(), "coord");
        String actionXml = getCoordActionXml(appPath, resourceXmlName);
        String actionNomialTime = getActionNomialTime(actionXml);

        CoordinatorActionBean action = new CoordinatorActionBean();
        action.setId(actionId);
        action.setJobId(jobId);
        action.setActionNumber(actionNum);
        try {
            action.setNominalTime(DateUtils.parseDateUTC(actionNomialTime));
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Unable to get action nominal time");
            throw new IOException(e);
        }
        action.setLastModifiedTime(new Date());
        action.setStatus(status);
        action.setActionXml(actionXml);

        Properties conf = getCoordConf(appPath);
        String createdConf = XmlUtils.writePropToString(conf);

        action.setCreatedConf(createdConf);

        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            assertNotNull(jpaService);
            CoordActionInsertCommand coordActionInsertCmd = new CoordActionInsertCommand(action);
            jpaService.execute(coordActionInsertCmd);
        }
        catch (CommandException ce) {
            ce.printStackTrace();
            fail("Unable to insert the test coord action record to table");
            throw ce;
        }
        return actionId;
    }

    /**
     * Insert wf job for testing.
     *
     * @param wfId
     * @throws Exception
     */
    protected void addRecordToWfJobTable(String wfId, WorkflowJob.Status jobStatus,
            WorkflowInstance.Status instanceStatus) throws Exception {
        WorkflowApp app = new LiteWorkflowApp("testApp", "<workflow-app/>", new StartNodeDef("end"))
                .addNode(new EndNodeDef("end"));
        Configuration conf = new Configuration();
        conf.set(OozieClient.APP_PATH, "testPath");
        conf.set(OozieClient.LOG_TOKEN, "testToken");
        conf.set(OozieClient.USER_NAME, getTestUser());
        conf.set(OozieClient.GROUP_NAME, getTestGroup());
        injectKerberosInfo(conf);
        WorkflowJobBean wfBean = createWorkflow(app, conf, "auth", jobStatus, instanceStatus);
        wfBean.setId(wfId);

        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            assertNotNull(jpaService);
            WorkflowJobInsertCommand wfInsertCmd = new WorkflowJobInsertCommand(wfBean);
            jpaService.execute(wfInsertCmd);
        }
        catch (CommandException ce) {
            ce.printStackTrace();
            fail("Unable to insert the test wf job record to table");
            throw ce;
        }
    }

    /**
     * Insert wf action for testing.
     *
     * @param wfId
     * @param actionNum
     * @param status
     * @throws Exception
     */
    protected void addRecordToWfActionTable(String wfId, int actionNum, WorkflowAction.Status status) throws Exception {
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
        action.resetPendingOnly();

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
    }

    /**
     * Read coord job xml from test resources
     *
     * @param appPath
     * @return coord job xml
     */
    protected String getCoordJobXml(Path appPath) {
        String inputTemplate = appPath + "/coord-input/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";
        inputTemplate = Matcher.quoteReplacement(inputTemplate);
        String outputTemplate = appPath + "/coord-input/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";
        outputTemplate = Matcher.quoteReplacement(outputTemplate);
        try {
            Reader reader = IOUtils.getResourceAsReader("coord-rerun-job.xml", -1);
            String appXml = IOUtils.getReaderAsString(reader, -1);
            appXml = appXml.replaceAll("#inputTemplate", inputTemplate);
            appXml = appXml.replaceAll("#outputTemplate", outputTemplate);
            return appXml;
        }
        catch (IOException ioe) {
            throw new RuntimeException(XLog.format("Could not get coord-rerun-job.xml", ioe));
        }
    }

    /**
     * Read coord action xml from test resources
     *
     * @param appPath
     * @param resourceXmlName
     * @return coord action xml
     */
    protected String getCoordActionXml(Path appPath, String resourceXmlName) {
        String inputTemplate = appPath + "/coord-input/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";
        inputTemplate = Matcher.quoteReplacement(inputTemplate);
        String outputTemplate = appPath + "/coord-input/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";
        outputTemplate = Matcher.quoteReplacement(outputTemplate);

        String inputDir = appPath + "/coord-input/2010/07/05/01/00";
        inputDir = Matcher.quoteReplacement(inputDir);
        String outputDir = appPath + "/coord-input/2009/12/14/11/00";
        outputDir = Matcher.quoteReplacement(outputDir);
        try {
            Reader reader = IOUtils.getResourceAsReader(resourceXmlName, -1);
            String appXml = IOUtils.getReaderAsString(reader, -1);
            appXml = appXml.replaceAll("#inputTemplate", inputTemplate);
            appXml = appXml.replaceAll("#outputTemplate", outputTemplate);
            appXml = appXml.replaceAll("#inputDir", inputDir);
            appXml = appXml.replaceAll("#outputDir", outputDir);
            return appXml;
        }
        catch (IOException ioe) {
            throw new RuntimeException(XLog.format("Could not get " + resourceXmlName, ioe));
        }
    }

    protected Properties getCoordConf(Path appPath) throws IOException {
        Path wfAppPath = new Path(getFsTestCaseDir(), "workflow");
        final OozieClient coordClient = LocalOozie.getCoordClient();
        Properties conf = coordClient.createConfiguration();
        conf.setProperty(OozieClient.COORDINATOR_APP_PATH, appPath.toString());
        conf.setProperty("jobTracker", getJobTrackerUri());
        conf.setProperty("nameNode", getNameNodeUri());
        conf.setProperty("wfAppPath", wfAppPath.toString());
        conf.remove("user.name");
        conf.setProperty("user.name", getTestUser());
        injectKerberosInfo(conf);

        String content = "<workflow-app xmlns='uri:oozie:workflow:0.1'  xmlns:sla='uri:oozie:sla:0.1' name='no-op-wf'>";
        content += "<start to='end' />";
        content += "<end name='end' /></workflow-app>";
        writeToFile(content, wfAppPath, "workflow.xml");

        return conf;
    }

    private void writeToFile(String content, Path appPath, String fileName) throws IOException {
        FileSystem fs = getFileSystem();
        Writer writer = new OutputStreamWriter(fs.create(new Path(appPath, fileName), true));
        writer.write(content);
        writer.close();
    }

    private String getActionNomialTime(String actionXml) {
        Element eAction;
        try {
            eAction = XmlUtils.parseXml(actionXml);
        }
        catch (JDOMException je) {
            throw new RuntimeException(XLog.format("Could not parse actionXml :" + actionXml, je));
        }
        String actionNomialTime = eAction.getAttributeValue("action-nominal-time");

        return actionNomialTime;
    }

    protected WorkflowJobBean createWorkflow(WorkflowApp app, Configuration conf, String authToken,
            WorkflowJob.Status jobStatus, WorkflowInstance.Status instanceStatus) throws Exception {
        WorkflowAppService wps = Services.get().get(WorkflowAppService.class);
        Configuration protoActionConf = wps.createProtoActionConf(conf, authToken, true);
        WorkflowLib workflowLib = Services.get().get(WorkflowStoreService.class).getWorkflowLibWithNoDB();
        WorkflowInstance wfInstance = workflowLib.createInstance(app, conf);
        ((LiteWorkflowInstance) wfInstance).setStatus(instanceStatus);
        WorkflowJobBean workflow = new WorkflowJobBean();
        workflow.setId(wfInstance.getId());
        workflow.setAppName(app.getName());
        workflow.setAppPath(conf.get(OozieClient.APP_PATH));
        workflow.setConf(XmlUtils.prettyPrint(conf).toString());
        workflow.setProtoActionConf(XmlUtils.prettyPrint(protoActionConf).toString());
        workflow.setCreatedTime(new Date());
        workflow.setLogToken(conf.get(OozieClient.LOG_TOKEN, ""));
        workflow.setStatus(jobStatus);
        workflow.setRun(0);
        workflow.setUser(conf.get(OozieClient.USER_NAME));
        workflow.setGroup(conf.get(OozieClient.GROUP_NAME));
        workflow.setAuthToken(authToken);
        workflow.setWorkflowInstance(wfInstance);
        return workflow;
    }

}