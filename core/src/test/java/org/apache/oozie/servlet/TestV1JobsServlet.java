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
package org.apache.oozie.servlet;

import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.DagEngine;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.rest.JsonTags;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.service.AuthorizationService;
import org.apache.oozie.service.DagEngineService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.XConfiguration;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

public class TestV1JobsServlet extends DagServletTestCase {

    static {
        new V1JobsServlet();
    }

    private static final boolean IS_SECURITY_ENABLED = false;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
    }

    public void testSubmit() throws Exception {
        runTest("/v1/jobs", V1JobsServlet.class, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                MockDagEngineService.reset();

                String appPath = getFsTestCaseDir().toString() + "/app";

                FileSystem fs = getFileSystem();
                Path jobXmlPath = new Path(appPath, "workflow.xml");
                fs.create(jobXmlPath);

                int wfCount = MockDagEngineService.workflows.size();
                Configuration jobConf = new XConfiguration();
                jobConf.set(OozieClient.USER_NAME, getTestUser());
                jobConf.set(OozieClient.GROUP_NAME, getTestGroup());
                jobConf.set(OozieClient.APP_PATH, appPath);
                injectKerberosInfo(jobConf);
                Map<String, String> params = new HashMap<String, String>();
                URL url = createURL("", params);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("POST");
                conn.setRequestProperty("content-type", RestConstants.XML_CONTENT_TYPE);
                conn.setDoOutput(true);
                jobConf.writeXml(conn.getOutputStream());
                assertEquals(HttpServletResponse.SC_CREATED, conn.getResponseCode());
                JSONObject obj = (JSONObject) JSONValue.parse(new InputStreamReader(conn.getInputStream()));
                assertEquals(MockDagEngineService.JOB_ID + wfCount, obj.get(JsonTags.JOB_ID));
                assertFalse(MockDagEngineService.started.get(wfCount));
                wfCount++;

                jobConf = new XConfiguration();
                jobConf.set(OozieClient.USER_NAME, getTestUser());
                jobConf.set(OozieClient.APP_PATH, appPath);
                injectKerberosInfo(jobConf);
                params = new HashMap<String, String>();
                params.put(RestConstants.ACTION_PARAM, RestConstants.JOB_ACTION_START);
                url = createURL("", params);
                conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("POST");
                conn.setRequestProperty("content-type", RestConstants.XML_CONTENT_TYPE);
                conn.setDoOutput(true);
                jobConf.writeXml(conn.getOutputStream());
                assertEquals(HttpServletResponse.SC_CREATED, conn.getResponseCode());
                obj = (JSONObject) JSONValue.parse(new InputStreamReader(conn.getInputStream()));
                assertEquals(MockDagEngineService.JOB_ID + wfCount, obj.get(JsonTags.JOB_ID));
                assertTrue(MockDagEngineService.started.get(wfCount));
                Services services = Services.get();
                DagEngine de = services.get(DagEngineService.class).getDagEngine(getTestUser(), "undef");
                StringReader sr = new StringReader(de.getJob(MockDagEngineService.JOB_ID + wfCount).getConf());
                Configuration conf1 = new XConfiguration(sr);
                assertEquals(AuthorizationService.DEFAULT_GROUP, conf1.get(OozieClient.GROUP_NAME));
                return null;
            }
        });
    }
    
    public void testSubmitBundle() throws Exception {
        runTest("/v1/jobs", V1JobsServlet.class, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                MockBundleEngineService.reset();

                String bundleAppPath = getFsTestCaseDir().toString() + "/bundle";
                FileSystem fs = getFileSystem();
                Path jobXmlPath = new Path(bundleAppPath, "bundle.xml");
                fs.create(jobXmlPath);

                int bundleJobCount = MockBundleEngineService.bundleJobs.size();
                Configuration jobConf = new XConfiguration();
                jobConf.set(OozieClient.USER_NAME, getTestUser());
                jobConf.set(OozieClient.GROUP_NAME, getTestGroup());
                jobConf.set(OozieClient.BUNDLE_APP_PATH, bundleAppPath);
                injectKerberosInfo(jobConf);
                Map<String, String> params = new HashMap<String, String>();
                URL url = createURL("", params);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("POST");
                conn.setRequestProperty("content-type", RestConstants.XML_CONTENT_TYPE);
                conn.setDoOutput(true);
                jobConf.writeXml(conn.getOutputStream());
                assertEquals(HttpServletResponse.SC_CREATED, conn.getResponseCode());
                JSONObject obj = (JSONObject) JSONValue.parse(new InputStreamReader(conn.getInputStream()));
                assertEquals(MockBundleEngineService.JOB_ID + bundleJobCount, obj.get(JsonTags.JOB_ID));
                return null;
            }
        });
    }

    public void testJobs() throws Exception {
        runTest("/v1/jobs", V1JobsServlet.class, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                MockDagEngineService.reset();

                int wfCount = MockDagEngineService.workflows.size();
                Map<String, String> params = new HashMap<String, String>();
                params.put(RestConstants.JOBS_FILTER_PARAM, "name=x");
                URL url = createURL("", params);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
                assertTrue(conn.getHeaderField("content-type").startsWith(RestConstants.JSON_CONTENT_TYPE));
                JSONObject json = (JSONObject) JSONValue.parse(new InputStreamReader(conn.getInputStream()));
                JSONArray array = (JSONArray) json.get(JsonTags.WORKFLOWS_JOBS);
                assertEquals(MockDagEngineService.INIT_WF_COUNT, array.size());
                for (int i = 0; i < MockDagEngineService.INIT_WF_COUNT; i++) {
                    assertEquals(MockDagEngineService.JOB_ID + i, ((JSONObject) array.get(i)).get(JsonTags.WORKFLOW_ID));
                    assertNotNull(((JSONObject) array.get(i)).get(JsonTags.WORKFLOW_APP_PATH));
                }

                params = new HashMap<String, String>();
                params.put(RestConstants.JOBS_FILTER_PARAM, "name=x");
                params.put(RestConstants.OFFSET_PARAM, "2");
                params.put(RestConstants.LEN_PARAM, "100");
                url = createURL("", params);
                conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
                assertTrue(conn.getHeaderField("content-type").startsWith(RestConstants.JSON_CONTENT_TYPE));
                json = (JSONObject) JSONValue.parse(new InputStreamReader(conn.getInputStream()));
                array = (JSONArray) json.get(JsonTags.WORKFLOWS_JOBS);

                assertEquals(MockDagEngineService.INIT_WF_COUNT, array.size());
                for (int i = 0; i < MockDagEngineService.INIT_WF_COUNT; i++) {
                    assertEquals(MockDagEngineService.JOB_ID + i, ((JSONObject) array.get(i)).get(JsonTags.WORKFLOW_ID));
                    assertNotNull(((JSONObject) array.get(i)).get(JsonTags.WORKFLOW_APP_PATH));
                }

                params = new HashMap<String, String>();
                params.put(RestConstants.JOBTYPE_PARAM, "wf");
                params.put(RestConstants.JOBS_EXTERNAL_ID_PARAM, "external-valid");
                url = createURL("", params);
                conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
                assertTrue(conn.getHeaderField("content-type").startsWith(RestConstants.JSON_CONTENT_TYPE));
                JSONObject obj = (JSONObject) JSONValue.parse(new InputStreamReader(conn.getInputStream()));
                assertEquals("id-valid", obj.get(JsonTags.JOB_ID));

                params = new HashMap<String, String>();
                params.put(RestConstants.JOBS_EXTERNAL_ID_PARAM, "external-invalid");
                url = createURL("", params);
                conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
                assertTrue(conn.getHeaderField("content-type").startsWith(RestConstants.JSON_CONTENT_TYPE));
                obj = (JSONObject) JSONValue.parse(new InputStreamReader(conn.getInputStream()));
                assertNull(obj.get(JsonTags.JOB_ID));

                return null;
            }
        });
    }
}