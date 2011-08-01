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

import org.apache.oozie.test.XFsTestCase;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.local.LocalOozie;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowJob;

import java.io.File;
import java.io.Reader;
import java.io.Writer;
import java.io.OutputStreamWriter;
import java.util.Properties;

public class TestWorkflowProgress extends XFsTestCase {

    protected void setUp() throws Exception {
        super.setUp();
        setSystemProperty("oozielastmod.log", "/tmp/oozielastmod.log");
    }

    public void testChainWorkflow() throws Exception {
        FileSystem fs = getFileSystem();
        Path appPath = new Path(getFsTestCaseDir(), "app");
        fs.mkdirs(appPath);
        Writer writer = new OutputStreamWriter(fs.create(new Path(appPath, "workflow.xml")));
        String wfApp = "<workflow-app xmlns='uri:oozie:workflow:0.1' name='test-wf'>" + "    <start to='end'/>"
        + "    <end name='end'/>" + "</workflow-app>";
        writer.write(wfApp);
        writer.close();
        
        try {
            LocalOozie.start();
            final OozieClient wc = LocalOozie.getClient();
            Properties conf = wc.createConfiguration();
            conf.setProperty(OozieClient.APP_PATH, appPath.toString() + File.separator + "workflow.xml");
            conf.setProperty(OozieClient.USER_NAME, getTestUser());
            conf.setProperty(OozieClient.GROUP_NAME, getTestGroup());
            injectKerberosInfo(conf);

            final String jobId = wc.submit(conf);
            WorkflowJob wf = wc.getJobInfo(jobId);
            float p = wf.getProgress();
            assertEquals(p, 0.0f);

            wc.start(jobId);
            wf = wc.getJobInfo(jobId);
            assertEquals(p, 0.0f);
            waitFor(600000, new Predicate() {
                public boolean evaluate() throws Exception {
                    WorkflowJob wf = wc.getJobInfo(jobId);
                    return wf.getStatus() == WorkflowJob.Status.SUCCEEDED;
                }
            });

            wf = wc.getJobInfo(jobId);
            p = wf.getProgress();
            assertEquals(p, 1.0f);
        }
        finally {
            LocalOozie.stop();
        }
    }
}
