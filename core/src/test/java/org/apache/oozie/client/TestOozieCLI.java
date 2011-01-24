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
package org.apache.oozie.client;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringReader;
import java.net.URL;
import java.util.Properties;
import java.util.concurrent.Callable;

import com.cloudera.alfredo.client.AuthenticatedURL;
import com.cloudera.alfredo.client.AuthenticationException;
import com.cloudera.alfredo.client.PseudoAuthenticator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.cli.OozieCLI;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.servlet.DagServletTestCase;
import org.apache.oozie.servlet.JobServlet;
import org.apache.oozie.servlet.JobsServlet;
import org.apache.oozie.servlet.MockCoordinatorEngineService;
import org.apache.oozie.servlet.MockDagEngineService;
import org.apache.oozie.servlet.OozieAuthenticationFilter;
import org.apache.oozie.servlet.V1AdminServlet;
import org.apache.oozie.util.XConfiguration;

//hardcoding options instead using constants on purpose, to detect changes to option names if any and correct docs.
public class TestOozieCLI extends DagServletTestCase {

    static {
        new HeaderTestingVersionServlet();
        new JobServlet();
        new JobsServlet();
        new V1AdminServlet();
    }

    static final boolean IS_SECURITY_ENABLED = false;
    static final String VERSION = "/v" + OozieClient.WS_PROTOCOL_VERSION;
    static final String[] END_POINTS = {"/versions", VERSION + "/jobs", VERSION + "/job/*", VERSION + "/admin/*"};
    static final Class[] SERVLET_CLASSES = {
            HeaderTestingVersionServlet.class, JobsServlet.class, JobServlet.class, V1AdminServlet.class };

    static final String[] FILTER_PATHS = {"/*"};
    static final Class[] FILTER_CLASSES = { OozieAuthenticationFilter.class };

    protected void runTestWithAuthFilter(String[] servletPath, Class[] servletClass,
                                         boolean securityEnabled, Callable<Void> assertions) throws Exception {
        runTest(servletPath, servletClass, FILTER_PATHS, FILTER_CLASSES, securityEnabled, assertions);
    }

    private static class OozieCLI4Test extends OozieCLI {

        public static File getAuthTokenFile() {
            return AUTH_TOKEN_FILE;
        }
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        MockDagEngineService.reset();
        MockCoordinatorEngineService.reset();
        setSystemProperty("oozie.authentication.simple.anonymous.allowed", "false");
        OozieCLI4Test.getAuthTokenFile().delete();
        setSystemProperty("user.name", getTestUser());
        OozieCLI4Test.getAuthTokenFile().delete();
    }

    @Override
    protected void tearDown() throws Exception {
        OozieCLI4Test.getAuthTokenFile().delete();
        super.tearDown();
    }

    private String createConfigFile(String appPath) throws Exception {
        String path = getTestCaseDir() + "/" + getName() + ".xml";
        Configuration conf = new Configuration(false);
        conf.set(OozieClient.USER_NAME, getTestUser());
        conf.set(OozieClient.GROUP_NAME, getTestGroup());
        conf.set(OozieClient.APP_PATH, appPath);
        conf.set(OozieClient.RERUN_SKIP_NODES, "node");
        injectKerberosInfo(conf);
        OutputStream os = new FileOutputStream(path);
        conf.writeXml(os);
        os.close();
        return path;
    }

    private String createPropertiesFile(String appPath) throws Exception {
        String path = getTestCaseDir() + "/" + getName() + ".properties";
        Properties props = new Properties();
        props.setProperty(OozieClient.USER_NAME, getTestUser());
        props.setProperty(OozieClient.GROUP_NAME, getTestGroup());
        props.setProperty(OozieClient.APP_PATH, appPath);
        props.setProperty(OozieClient.RERUN_SKIP_NODES, "node");
        props.setProperty("a", "A");
        injectKerberosInfo(props);
        OutputStream os = new FileOutputStream(path);
        props.store(os, "");
        os.close();
        return path;
    }

    private String createPropertiesFileWithTrailingSpaces(String appPath) throws Exception {
        String path = getTestCaseDir() + "/" + getName() + ".properties";
        Properties props = new Properties();
        props.setProperty(OozieClient.USER_NAME, getTestUser());
        props.setProperty(OozieClient.GROUP_NAME, getTestGroup());
        injectKerberosInfo(props);
        props.setProperty(OozieClient.APP_PATH, appPath);
        //add spaces to string
        props.setProperty(OozieClient.RERUN_SKIP_NODES + " ", " node ");
        OutputStream os = new FileOutputStream(path);
        props.store(os, "");
        os.close();
        return path;
    }

    public void testSubmit() throws Exception {
        runTestWithAuthFilter(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                int wfCount = MockDagEngineService.INIT_WF_COUNT;

                Path appPath = new Path(getFsTestCaseDir(), "app");
                getFileSystem().mkdirs(appPath);
                getFileSystem().create(new Path(appPath, "workflow.xml")).close();

                String[] args = new String[]{"job", "-submit", "-oozie", oozieUrl, "-config",
                                             createConfigFile(appPath.toString())};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals("submit", MockDagEngineService.did);
                assertFalse(MockDagEngineService.started.get(wfCount));
                wfCount++;

                args = new String[]{"job", "-submit", "-oozie", oozieUrl, "-config",
                                    createPropertiesFile(appPath.toString())};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals("submit", MockDagEngineService.did);
                assertFalse(MockDagEngineService.started.get(wfCount));

                MockDagEngineService.reset();
                wfCount = MockDagEngineService.INIT_WF_COUNT;
                args = new String[]{"job", "-submit", "-oozie", oozieUrl, "-config",
                                    createPropertiesFile(appPath.toString()) + "x"};
                assertEquals(-1, new OozieCLI().run(args));
                assertEquals(null, MockDagEngineService.did);
                try {
                    MockDagEngineService.started.get(wfCount);
                    //job was not created, then how did this extra job come after reset? fail!!
                    fail();
                }
                catch (Exception e) {
                    //job was not submitted, so its fine
                }
                return null;
            }
        });
    }

    public void testSubmitWithPropertyArguments() throws Exception {
        runTestWithAuthFilter(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                int wfCount = MockDagEngineService.INIT_WF_COUNT;

                Path appPath = new Path(getFsTestCaseDir(), "app");
                getFileSystem().mkdirs(appPath);
                getFileSystem().create(new Path(appPath, "workflow.xml")).close();

                String[] args = new String[]{"job", "-submit", "-oozie", oozieUrl, "-config",
                                             createConfigFile(appPath.toString()), "-Da=X", "-Db=B"};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals("submit", MockDagEngineService.did);
                assertFalse(MockDagEngineService.started.get(wfCount));

                assertEquals("X", MockDagEngineService.submittedConf.get("a"));
                assertEquals("B", MockDagEngineService.submittedConf.get("b"));
                return null;
            }
        });
    }

    public void testRun() throws Exception {
        runTestWithAuthFilter(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                Path appPath = new Path(getFsTestCaseDir(), "app");
                getFileSystem().mkdirs(appPath);
                getFileSystem().create(new Path(appPath, "workflow.xml")).close();
                String oozieUrl = getContextURL();
                int wfCount = MockDagEngineService.INIT_WF_COUNT;
                String[] args = new String[]{"job", "-run", "-oozie", oozieUrl, "-config",
                                             createConfigFile(appPath.toString())};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals("submit", MockDagEngineService.did);
                assertTrue(MockDagEngineService.started.get(wfCount));

                return null;
            }
        });
    }

    public void testStart() throws Exception {
        runTestWithAuthFilter(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                String[] args = new String[]{"job", "-oozie", oozieUrl, "-start", MockDagEngineService.JOB_ID + "1"};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals(RestConstants.JOB_ACTION_START, MockDagEngineService.did);
                assertTrue(MockDagEngineService.started.get(1));

                args = new String[]{"job", "-oozie", oozieUrl, "-start",
                                    MockDagEngineService.JOB_ID + (MockDagEngineService.workflows.size() + 1)};
                assertEquals(-1, new OozieCLI().run(args));
                return null;
            }
        });
    }

    public void testSuspend() throws Exception {
        runTestWithAuthFilter(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                String[] args = new String[]{"job", "-oozie", oozieUrl, "-suspend", MockDagEngineService.JOB_ID + 1};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals(RestConstants.JOB_ACTION_SUSPEND, MockDagEngineService.did);

                args = new String[]{"job", "-oozie", oozieUrl, "-suspend",
                                    MockDagEngineService.JOB_ID + (MockDagEngineService.workflows.size() + 1)};
                assertEquals(-1, new OozieCLI().run(args));
                return null;
            }
        });
    }

    public void testResume() throws Exception {
        runTestWithAuthFilter(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                String[] args = new String[]{"job", "-oozie", oozieUrl, "-resume", MockDagEngineService.JOB_ID + 1};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals(RestConstants.JOB_ACTION_RESUME, MockDagEngineService.did);

                args = new String[]{"job", "-oozie", oozieUrl, "-resume",
                                    MockDagEngineService.JOB_ID + (MockDagEngineService.workflows.size() + 1)};
                assertEquals(-1, new OozieCLI().run(args));
                return null;
            }
        });
    }

    public void testKill() throws Exception {
        runTestWithAuthFilter(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                String[] args = new String[]{"job", "-oozie", oozieUrl, "-kill", MockDagEngineService.JOB_ID + 1};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals(RestConstants.JOB_ACTION_KILL, MockDagEngineService.did);

                args = new String[]{"job", "-oozie", oozieUrl, "-kill",
                                    MockDagEngineService.JOB_ID + (MockDagEngineService.workflows.size() + 1)};
                assertEquals(-1, new OozieCLI().run(args));
                return null;
            }
        });
    }

    public void testReRun() throws Exception {
        runTestWithAuthFilter(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                Path appPath = new Path(getFsTestCaseDir(), "app");
                getFileSystem().mkdirs(appPath);
                getFileSystem().create(new Path(appPath, "workflow.xml")).close();
                String oozieUrl = getContextURL();
                String[] args = new String[]{"job", "-oozie", oozieUrl, "-config", createConfigFile(appPath.toString()),
                                             "-rerun", MockDagEngineService.JOB_ID + "1"};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals(RestConstants.JOB_ACTION_RERUN, MockDagEngineService.did);
                assertTrue(MockDagEngineService.started.get(1));
                return null;
            }
        });
    }

    /**
     * Test: oozie -rerun coord_job_id -action 1
     *
     * @throws Exception
     */
    public void testCoordReRun1() throws Exception {
        runTestWithAuthFilter(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                Path appPath = new Path(getFsTestCaseDir(), "app");
                getFileSystem().mkdirs(appPath);
                getFileSystem().create(new Path(appPath, "coordinator.xml")).close();
                String oozieUrl = getContextURL();
                String[] args = new String[]{"job", "-oozie", oozieUrl, "-rerun",
                                             MockCoordinatorEngineService.JOB_ID + "1",
                                             "-action", "1"};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals(RestConstants.JOB_COORD_ACTION_RERUN, MockCoordinatorEngineService.did);
                assertTrue(MockCoordinatorEngineService.started.get(1));
                return null;
            }
        });
    }

    /**
     * Test: oozie -rerun coord_job_id -date 2009-12-15T01:00Z::2009-12-16T01:00Z
     *
     * @throws Exception
     */
    public void testCoordReRun2() throws Exception {
        runTestWithAuthFilter(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                Path appPath = new Path(getFsTestCaseDir(), "app");
                getFileSystem().mkdirs(appPath);
                getFileSystem().create(new Path(appPath, "coordinator.xml")).close();
                String oozieUrl = getContextURL();
                String[] args = new String[]{"job", "-oozie", oozieUrl, "-rerun",
                                             MockCoordinatorEngineService.JOB_ID + "1",
                                             "-date", "2009-12-15T01:00Z::2009-12-16T01:00Z"};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals(RestConstants.JOB_COORD_ACTION_RERUN, MockCoordinatorEngineService.did);
                assertTrue(MockCoordinatorEngineService.started.get(1));
                return null;
            }
        });
    }

    /**
     * Negative Test: oozie -rerun coord_job_id -date 2009-12-15T01:00Z -action 1
     *
     * @throws Exception
     */
    public void testCoordReRunNeg1() throws Exception {
        runTestWithAuthFilter(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                Path appPath = new Path(getFsTestCaseDir(), "app");
                getFileSystem().mkdirs(appPath);
                getFileSystem().create(new Path(appPath, "coordinator.xml")).close();
                String oozieUrl = getContextURL();
                String[] args = new String[]{"job", "-oozie", oozieUrl, "-rerun",
                                             MockCoordinatorEngineService.JOB_ID + "1",
                                             "-date", "2009-12-15T01:00Z", "-action", "1"};
                assertEquals(-1, new OozieCLI().run(args));
                assertNull(MockCoordinatorEngineService.did);
                assertFalse(MockCoordinatorEngineService.started.get(1));
                return null;
            }
        });
    }

    /**
     * Negative Test: oozie -rerun coord_job_id
     *
     * @throws Exception
     */
    public void testCoordReRunNeg2() throws Exception {
        runTestWithAuthFilter(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                Path appPath = new Path(getFsTestCaseDir(), "app");
                getFileSystem().mkdirs(appPath);
                getFileSystem().create(new Path(appPath, "coordinator.xml")).close();
                String oozieUrl = getContextURL();
                String[] args = new String[]{"job", "-oozie", oozieUrl, "-rerun",
                                             MockCoordinatorEngineService.JOB_ID + "1"};
                assertEquals(-1, new OozieCLI().run(args));
                assertNull(MockCoordinatorEngineService.did);
                assertFalse(MockCoordinatorEngineService.started.get(1));
                return null;
            }
        });
    }

    public void testJobStatus() throws Exception {
        runTestWithAuthFilter(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                MockDagEngineService.reset();
                String[] args = new String[]{"job", "-oozie", oozieUrl, "-info", MockDagEngineService.JOB_ID + 0};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals(RestConstants.JOB_SHOW_INFO, MockDagEngineService.did);

                args = new String[]{"job", "-localtime", "-oozie", oozieUrl, "-info", MockDagEngineService.JOB_ID + 1};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals(RestConstants.JOB_SHOW_INFO, MockDagEngineService.did);

                args = new String[]{"job", "-oozie", oozieUrl, "-info", MockDagEngineService.JOB_ID + 2};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals(RestConstants.JOB_SHOW_INFO, MockDagEngineService.did);

                args = new String[]{"job", "-oozie", oozieUrl, "-info",
                                    MockDagEngineService.JOB_ID + (MockDagEngineService.workflows.size() + 1)};
                assertEquals(-1, new OozieCLI().run(args));
                return null;
            }
        });
    }

    public void testJobsStatus() throws Exception {
        runTestWithAuthFilter(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                String[] args = new String[]{"jobs", "-len", "3", "-offset", "2", "-oozie", oozieUrl, "-filter",
                                             "name=x"};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals(RestConstants.JOBS_FILTER_PARAM, MockDagEngineService.did);

                args = new String[]{"jobs", "-localtime", "-len", "3", "-offset", "2", "-oozie", oozieUrl, "-filter",
                                    "name=x"};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals(RestConstants.JOBS_FILTER_PARAM, MockDagEngineService.did);
                args = new String[]{"jobs", "-jobtype", "coord", "-filter", "status=FAILED", "-oozie", oozieUrl};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals(RestConstants.JOBS_FILTER_PARAM, MockDagEngineService.did);
                return null;
            }
        });
    }

    public void testHeaderPropagation() throws Exception {
        runTestWithAuthFilter(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                HeaderTestingVersionServlet.OOZIE_HEADERS.clear();
                setSystemProperty(OozieCLI.WS_HEADER_PREFIX + "header", "test");

                String oozieUrl = getContextURL();
                String[] args = new String[]{"job", "-oozie", oozieUrl, "-start", MockDagEngineService.JOB_ID + 1};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals(RestConstants.JOB_ACTION_START, MockDagEngineService.did);
                assertTrue(HeaderTestingVersionServlet.OOZIE_HEADERS.containsKey("header"));
                assertTrue(HeaderTestingVersionServlet.OOZIE_HEADERS.containsValue("test"));

                return null;
            }
        });
    }

    public void testOozieStatus() throws Exception {
        runTestWithAuthFilter(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                HeaderTestingVersionServlet.OOZIE_HEADERS.clear();

                String oozieUrl = getContextURL();
                String[] args = new String[]{"admin", "-status", "-oozie", oozieUrl};
                assertEquals(0, new OozieCLI().run(args));

                args = new String[]{"admin", "-oozie", oozieUrl, "-systemmode", "NORMAL"};
                assertEquals(0, new OozieCLI().run(args));
                return null;
            }
        });
    }

    public void testServerBuildVersion() throws Exception {
        runTestWithAuthFilter(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                HeaderTestingVersionServlet.OOZIE_HEADERS.clear();

                String oozieUrl = getContextURL();
                String[] args = new String[]{"admin", "-version", "-oozie", oozieUrl};
                assertEquals(0, new OozieCLI().run(args));

                return null;
            }
        });
    }

    public void testClientBuildVersion() throws Exception {
        String[] args = new String[]{"version"};
        assertEquals(0, new OozieCLI().run(args));
    }

    public void testJobInfo() throws Exception {
        runTestWithAuthFilter(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                MockDagEngineService.reset();
                String[] args = new String[]{"job", "-oozie", oozieUrl, "-info", MockDagEngineService.JOB_ID + 0};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals(RestConstants.JOB_SHOW_INFO, MockDagEngineService.did);

                args = new String[]{"job", "-oozie", oozieUrl, "-info", MockDagEngineService.JOB_ID + 1, "-len", "3",
                                    "-offset", "1"};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals(RestConstants.JOB_SHOW_INFO, MockDagEngineService.did);

                args = new String[]{"job", "-oozie", oozieUrl, "-info", MockDagEngineService.JOB_ID + 2, "-len", "2"};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals(RestConstants.JOB_SHOW_INFO, MockDagEngineService.did);

                args = new String[]{"job", "-oozie", oozieUrl, "-info", MockDagEngineService.JOB_ID + 3, "-offset",
                                    "3"};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals(RestConstants.JOB_SHOW_INFO, MockDagEngineService.did);

                return null;
            }
        });
    }

    public void testJobLog() throws Exception {
        runTestWithAuthFilter(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                MockDagEngineService.reset();
                String[] args = new String[]{"job", "-oozie", oozieUrl, "-log", MockDagEngineService.JOB_ID + 0};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals(RestConstants.JOB_SHOW_LOG, MockDagEngineService.did);


                return null;
            }
        });
    }

    public void testJobDefinition() throws Exception {
        runTestWithAuthFilter(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                MockDagEngineService.reset();
                String[] args = new String[]{"job", "-oozie", oozieUrl, "-definition", MockDagEngineService.JOB_ID + 0};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals(RestConstants.JOB_SHOW_DEFINITION, MockDagEngineService.did);


                return null;
            }
        });
    }

    public void testPropertiesWithTrailingSpaces() throws Exception {
        runTestWithAuthFilter(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                MockDagEngineService.reset();
                String oozieUrl = getContextURL();

                Path appPath = new Path(getFsTestCaseDir(), "app");
                getFileSystem().mkdirs(appPath);
                getFileSystem().create(new Path(appPath, "workflow.xml")).close();

                String[] args = new String[]{"job", "-submit", "-oozie", oozieUrl, "-config",
                                             createPropertiesFileWithTrailingSpaces(appPath.toString())};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals("submit", MockDagEngineService.did);
                String confStr = MockDagEngineService.workflows.get(MockDagEngineService.INIT_WF_COUNT).getConf();
                XConfiguration conf = new XConfiguration(new StringReader(confStr));
                assertNotNull(conf.get(OozieClient.RERUN_SKIP_NODES));
                assertEquals("node", conf.get(OozieClient.RERUN_SKIP_NODES));
                return null;
            }
        });
    }

    public void testAdminQueueDump() throws Exception {
        runTestWithAuthFilter(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                HeaderTestingVersionServlet.OOZIE_HEADERS.clear();

                String oozieUrl = getContextURL();
                String[] args = new String[]{"admin", "-queuedump", "-oozie", oozieUrl};
                assertEquals(0, new OozieCLI().run(args));

                return null;
            }
        });
    }

    public void testSkipAuth() throws Exception {
        setSystemProperty("skip.auth", "true");
        runTestWithAuthFilter(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                String[] args = new String[]{"admin", "-status", "-oozie", oozieUrl};
                assertNotSame(0, new OozieCLI().run(args));
                return null;
            }
        });
    }

    public static class Authenticator4Test extends PseudoAuthenticator {

        private static boolean USED = false;

        @Override
        public void authenticate(URL url, AuthenticatedURL.Token token) throws IOException, AuthenticationException {
            USED = true;
            super.authenticate(url, token);
        }
    }

    public void testCustomAuthenticator() throws Exception {
        setSystemProperty("authenticator.class", Authenticator4Test.class.getName());
        Authenticator4Test.USED = false;
        runTestWithAuthFilter(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                String[] args = new String[]{"admin", "-status", "-oozie", oozieUrl};
                assertEquals(0, new OozieCLI().run(args));
                return null;
            }
        });
        assertTrue(Authenticator4Test.USED);
    }


    public void testSkipAuthFile() throws Exception {
        runTestWithAuthFilter(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                assertFalse(OozieCLI4Test.getAuthTokenFile().exists());
                String oozieUrl = getContextURL();
                String[] args = new String[]{"admin", "-status", "-oozie", oozieUrl};
                assertEquals(0, new OozieCLI().run(args));
                assertTrue(OozieCLI4Test.getAuthTokenFile().exists());
                setSystemProperty("skip.auth.file", "true");
                assertEquals(0, new OozieCLI().run(args));
                assertFalse(OozieCLI4Test.getAuthTokenFile().exists());
                return null;
            }
        });
    }

}
