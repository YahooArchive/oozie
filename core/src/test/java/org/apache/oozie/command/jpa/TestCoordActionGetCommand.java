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

import java.io.IOException;
import java.io.Reader;
import java.util.Date;
import java.util.Properties;
import java.util.regex.Matcher;

import org.apache.hadoop.fs.Path;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.local.LocalOozie;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XFsTestCase;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;
import org.jdom.JDOMException;

public class TestCoordActionGetCommand extends XFsTestCase {
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
        String jobId = "00000-" + new Date().getTime() + "-TestCoordActionGetCommand-C";
        int actionNum = 1;
        String actionId = jobId + "@" + actionNum;
        insertAction(jobId, actionId, actionNum, CoordinatorAction.Status.WAITING);
        _testGetAction(actionId);
    }

    private void _testGetAction(String actionId) throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        CoordActionGetCommand actionGetCmd = new CoordActionGetCommand(actionId);
        CoordinatorActionBean ret = jpaService.execute(actionGetCmd);
        assertNotNull(ret);
        assertEquals(ret.getId(), actionId);
    }

    private void insertAction(String jobId, String actionId, int actionNum, CoordinatorAction.Status status) throws Exception {
        Path appPath = new Path(getFsTestCaseDir(), "coord");
        String actionXml = getCoordActionXml(appPath);
        String actionNomialTime = getActionNomialTime(actionXml);

        CoordinatorActionBean action = new CoordinatorActionBean();
        action.setJobId(jobId);
        action.setId(actionId);
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
            fail("Unable to insert the test action record to table");
            throw ce;
        }
    }

    private String getCoordActionXml(Path appPath) {
        String inputTemplate = appPath + "/coord-input/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";
        inputTemplate = Matcher.quoteReplacement(inputTemplate);
        String outputTemplate = appPath + "/coord-input/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";
        outputTemplate = Matcher.quoteReplacement(outputTemplate);

        String inputDir = appPath + "/coord-input/2010/07/05/01/00";
        inputDir = Matcher.quoteReplacement(inputDir);
        String outputDir = appPath + "/coord-input/2009/12/14/11/00";
        outputDir = Matcher.quoteReplacement(outputDir);
        try {
            Reader reader = IOUtils.getResourceAsReader("coord-action-get.xml", -1);
            String appXml = IOUtils.getReaderAsString(reader, -1);
            appXml = appXml.replaceAll("#inputTemplate", inputTemplate);
            appXml = appXml.replaceAll("#outputTemplate", outputTemplate);
            appXml = appXml.replaceAll("#inputDir", inputDir);
            appXml = appXml.replaceAll("#outputDir", outputDir);
            return appXml;
        }
        catch (IOException ioe) {
            throw new RuntimeException(XLog.format("Could not get coord-action-get.xml", ioe));
        }
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

    private Properties getCoordConf(Path appPath) {
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
        return conf;
    }
}
