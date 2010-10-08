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
package org.apache.oozie;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.service.ActionService;
import org.apache.oozie.service.SchemaService;
import org.apache.oozie.service.WorkflowStoreService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XTestCase;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XConfiguration;

import java.io.File;
import java.io.FileWriter;
import java.io.Reader;
import java.io.Writer;
import java.io.StringReader;
import java.io.OutputStream;
import java.io.FileOutputStream;
import java.util.List;

public class TestDagEngine extends XTestCase {
    private Services services;

    protected void setUp() throws Exception {
        super.setUp();
        setSystemProperty(SchemaService.WF_CONF_EXT_SCHEMAS, "wf-ext-schema.xsd");
        services = new Services();
        cleanUpDB(services.getConf());
        services.init();
        services.get(ActionService.class).register(ForTestingActionExecutor.class);
    }

    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    public void testSubmit() throws Exception {
        Reader reader = IOUtils.getResourceAsReader("wf-ext-schema-valid.xml", -1);
        Writer writer = new FileWriter(getTestCaseDir() + "/workflow.xml");
        IOUtils.copyCharStream(reader, writer);

        OutputStream os = new FileOutputStream(getTestCaseDir() + "/config-default.xml");
        XConfiguration defaultConf = new XConfiguration();
        defaultConf.set("a", "AA");
        defaultConf.set("b", "BB");
        defaultConf.set("e", "${d}${d}");
        defaultConf.writeXml(os);
        os.close();

        final DagEngine engine = new DagEngine(getTestUser(), "a");
        Configuration conf = new XConfiguration();
        conf.set(OozieClient.APP_PATH, getTestCaseDir() + File.separator + "workflow.xml");
        conf.set(OozieClient.USER_NAME, getTestUser());
        conf.set(OozieClient.GROUP_NAME, getTestGroup());
        injectKerberosInfo(conf);
        conf.set(OozieClient.LOG_TOKEN, "t");
        conf.set("signal-value", "OK");
        conf.set("external-status", "ok");
        conf.set("error", "end.error");
        conf.set("b", "B");
        conf.set("c", "C");
        conf.set("d", "${c}${c}");
        conf.set("f", "${e}${e}");

        final String jobId1 = engine.submitJob(conf, true);

        WorkflowJob wf = engine.getJob(jobId1);
        XConfiguration wfConf = new XConfiguration(new StringReader(wf.getConf()));
        assertEquals("AA", wfConf.get("a"));
        assertEquals("B", wfConf.get("b"));
        assertEquals("C", conf.get("c"));
        assertEquals("CC", conf.get("d"));
        assertEquals("CCCC", conf.get("e"));
        assertEquals("CCCCCCCC", conf.get("f"));

        waitFor(5000, new Predicate() {
            public boolean evaluate() throws Exception {
                WorkflowJobBean bean = Services.get().get(WorkflowStoreService.class).create().getWorkflow(jobId1, false);
                return bean.getWorkflowInstance().getStatus().isEndState();
            }
        });
        assertEquals(WorkflowJob.Status.KILLED, engine.getJob(jobId1).getStatus());
    }

    public void testJobDefinition() throws Exception {
        Reader reader = IOUtils.getResourceAsReader("wf-ext-schema-valid.xml", -1);
        Writer writer = new FileWriter(getTestCaseDir() + "/workflow.xml");
        IOUtils.copyCharStream(reader, writer);

        final DagEngine engine = new DagEngine(getTestUser(), "a");
        Configuration conf = new XConfiguration();
        conf.set(OozieClient.APP_PATH, getTestCaseDir() + File.separator + "workflow.xml");
        conf.set(OozieClient.USER_NAME, getTestUser());
        conf.set(OozieClient.GROUP_NAME, getTestGroup());
        injectKerberosInfo(conf);
        conf.set(OozieClient.LOG_TOKEN, "t");
        conf.set("signal-value", "OK");
        conf.set("external-status", "ok");
        conf.set("error", "end.error");

        String jobId1 = engine.submitJob(conf, false);

        String def = engine.getDefinition(jobId1);
        assertNotNull(def);
    }

    public void testGetJobs() throws Exception {
        Reader reader = IOUtils.getResourceAsReader("wf-ext-schema-valid.xml", -1);
        Writer writer = new FileWriter(getTestCaseDir() + "/workflow.xml");
        IOUtils.copyCharStream(reader, writer);

        final DagEngine engine = new DagEngine(getTestUser(), "a");
        Configuration conf = new XConfiguration();
        conf.set(OozieClient.APP_PATH, getTestCaseDir() + File.separator + "workflow.xml");
        conf.set(OozieClient.USER_NAME, getTestUser());
        conf.set(OozieClient.GROUP_NAME, getTestGroup());
        injectKerberosInfo(conf);
        conf.set(OozieClient.LOG_TOKEN, "t");
        conf.set("signal-value", "OK");
        conf.set("external-status", "ok");
        conf.set("error", "end.error");

        final String jobId1 = engine.submitJob(conf, true);
        String jobId2 = engine.submitJob(conf, false);
/*
        WorkflowsInfo wfInfo = engine.getJobs("group=" + getTestGroup(), 1, 1);
        List<WorkflowJobBean> workflows = wfInfo.getWorkflows();
        assertEquals(1, workflows.size());
        assertEquals(getTestGroup(), workflows.get(0).getGroup());
        assertEquals(jobId1, workflows.get(0).getId());

        wfInfo = engine.getJobs("group=" + getTestGroup(), 1, 5);
        workflows = wfInfo.getWorkflows();
        assertEquals(2, workflows.size());
        assertEquals(getTestGroup(), workflows.get(0).getGroup());
        assertEquals(jobId1, workflows.get(0).getId());
        assertEquals(jobId2, workflows.get(1).getId());

        wfInfo = engine.getJobs("user=" + getTestUser(), 1, 1);
        workflows = wfInfo.getWorkflows();
        assertEquals(1, workflows.size());
        assertEquals(getTestUser(), workflows.get(0).getUser());
        assertEquals(jobId1, workflows.get(0).getId());

        wfInfo = engine.getJobs("user=" + getTestUser(), 2, 5);
        workflows = wfInfo.getWorkflows();
        assertEquals(1, workflows.size());
        assertEquals(getTestUser(), workflows.get(0).getUser());
        assertEquals(jobId2, workflows.get(0).getId());

        waitFor(5000, new Predicate() {
            public boolean evaluate() throws Exception {
                WorkflowJobBean bean = Services.get().get(WorkflowStoreService.class).create().getWorkflow(jobId1, false);
                return bean.getWorkflowInstance().getStatus().isEndState();
            }
        });

        wfInfo = engine.getJobs("status=PREP", 1, 5);
        workflows = wfInfo.getWorkflows();
        assertEquals(1, workflows.size());
        assertEquals(jobId2, workflows.get(0).getId());

        wfInfo = engine.getJobs("name=test-wf", 1, 5);
        workflows = wfInfo.getWorkflows();
        assertEquals(2, workflows.size());
        assertEquals(jobId1, workflows.get(0).getId());
        assertEquals(jobId2, workflows.get(1).getId());

        wfInfo = engine.getJobs("name=test-wf;status=PREP", 1, 5);
        workflows = wfInfo.getWorkflows();
        assertEquals(1, workflows.size());
        assertEquals(jobId2, workflows.get(0).getId());
*/
    }
}
