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

import junit.framework.Assert;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.LogManager;
import org.apache.oozie.test.XTestCase;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XLog;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;

public class TestXLogService extends XTestCase {

    protected void setUp() throws Exception {
        super.setUp();
        LogFactory.getFactory().release();
        LogManager.resetConfiguration();
    }

    protected void tearDown() throws Exception {
        LogFactory.getFactory().release();
        LogManager.resetConfiguration();
        super.tearDown();
    }

    public void testDefaultLog4jPropertiesFromClassLoader() throws Exception {
        XLogService ls = new XLogService();
        ls.init(null);
        Assert.assertTrue(ls.getFromClasspath());
        Assert.assertEquals(XLogService.DEFAULT_LOG4J_PROPERTIES, ls.getLog4jProperties());
        ls.destroy();
    }

    public void testCustomLog4jPropertiesFromClassLoader() throws Exception {
        setSystemProperty(XLogService.LOG4J_FILE_ENV, "test-custom-log4j.properties");
        XLogService ls = new XLogService();
        ls.init(null);
        Assert.assertTrue(ls.getFromClasspath());
        Assert.assertEquals("test-custom-log4j.properties", ls.getLog4jProperties());
        ls.destroy();
    }

    public void testDefaultLog4jFromConfigDir() throws Exception {
        File log4jFile = new File(getTestCaseConfDir(), XLogService.DEFAULT_LOG4J_PROPERTIES);
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        InputStream is = cl.getResourceAsStream("test-oozie-log4j.properties");
        IOUtils.copyStream(is, new FileOutputStream(log4jFile));
        XLogService ls = new XLogService();
        ls.init(null);
        Assert.assertFalse(ls.getFromClasspath());
        Assert.assertEquals(XLogService.DEFAULT_LOG4J_PROPERTIES, ls.getLog4jProperties());
        ls.destroy();
    }

    public void testCustomLog4jFromConfigDir() throws Exception {
        File log4jFile = new File(getTestCaseConfDir(), "test-log4j.properties");
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        InputStream is = cl.getResourceAsStream("test-oozie-log4j.properties");
        IOUtils.copyStream(is, new FileOutputStream(log4jFile));
        setSystemProperty(XLogService.LOG4J_FILE_ENV, "test-log4j.properties");
        XLogService ls = new XLogService();
        ls.init(null);
        Assert.assertFalse(ls.getFromClasspath());
        Assert.assertEquals("test-log4j.properties", ls.getLog4jProperties());
        ls.destroy();
    }

    public void testLog4jReload() throws Exception {
        File log4jFile = new File(getTestCaseConfDir(), XLogService.DEFAULT_LOG4J_PROPERTIES);
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        InputStream is = cl.getResourceAsStream("test-oozie-log4j.properties");
        IOUtils.copyStream(is, new FileOutputStream(log4jFile));
        setSystemProperty(XLogService.LOG4J_RELOAD_ENV, "1");
        XLogService ls = new XLogService();
        ls.init(null);
        assertTrue(LogFactory.getLog("a").isTraceEnabled());
        Thread.sleep(1 * 1000);
        is = cl.getResourceAsStream("test-custom-log4j.properties");
        IOUtils.copyStream(is, new FileOutputStream(log4jFile));
        waitFor(5 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return !LogFactory.getLog("a").isTraceEnabled();
            }
        });
        assertFalse(LogFactory.getLog("a").isTraceEnabled());
        ls.destroy();
    }

    public void testInfoParameters() throws Exception {
        XLogService ls = new XLogService();
        ls.init(null);
        assertEquals("USER[-] GROUP[-]", XLog.Info.get().createPrefix());
        ls.destroy();
    }

    public void testDefaultLogsDir() throws Exception {
        setSystemProperty(XLogService.OOZIE_LOGS_ENV, null);
        String logs = Services.getOozieHome() + "/logs";
        XLogService ls = new XLogService();
        ls.init(null);
        assertEquals(logs, System.getProperty(XLogService.OOZIE_LOGS_ENV));
        ls.destroy();
    }

    public void testCustomLogsDir() throws Exception {
        String logs = "/tmp/oozie/logs";
        setSystemProperty(XLogService.OOZIE_LOGS_ENV, logs);
        XLogService ls = new XLogService();
        ls.init(null);
        assertEquals(logs, System.getProperty(XLogService.OOZIE_LOGS_ENV));
        ls.destroy();
    }

}
