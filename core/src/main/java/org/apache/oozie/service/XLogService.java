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

import org.apache.commons.logging.LogFactory;
import org.apache.log4j.LogManager;
import org.apache.log4j.PropertyConfigurator;
import org.apache.oozie.util.Instrumentable;
import org.apache.oozie.util.Instrumentation;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XLogStreamer;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.BuildInfo;
import org.apache.oozie.ErrorCode;
import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.net.URL;
import java.util.Properties;
import java.util.Map;
import java.util.Date;

/**
 * Built-in service that initializes and manages Logging via  Log4j.
 * <p/>
 * Oozie Lo4gj default configuration file is <code>oozie-log4j.properties</code>.
 * <p/>
 * The file name can be changed by setting the Java System property <code>oozie.log4j.file</code>.
 * <p/>
 * The Log4j configuration files must be a properties file.
 * <p/>
 * The Log4j configuration file is first looked in the Oozie configuration directory see {@link ConfigurationService}.
 * If the file is not found there, it is looked in the classpath.
 * <p/>
 * If the Log4j configuration file is loaded from Oozie configuration directory, automatic reloading is enabled.
 * <p/>
 * If the Log4j configuration file is loaded from the classpath, automatic reloading is disabled.
 * <p/>
 * the automatic reloading interval is defined by the Java System property <code>oozie.log4j.reload</code>.
 * The default value is 10 seconds.
 */
public class XLogService implements Service, Instrumentable {
    private static final String INSTRUMENTATION_GROUP = "logging";

    /**
     * System property that indicates the logs directory.
     */
    public static final String OOZIE_LOGS_ENV = "OOZIE_LOGS";

    /**
     * System property that indicates the log4j configuration file to load.
     */
    public static final String LOG4J_FILE_ENV = "OOZIE_LOG4J_FILE";

    /**
     * System property that indicates the reload interval of the configuration file.
     */
    public static final String LOG4J_RELOAD_ENV = "OOZIE_LOG4J_RELOAD";

    /**
     * Default value for the log4j configuration file if {@link #LOG4J_FILE_ENV} is not set.
     */
    public static final String DEFAULT_LOG4J_PROPERTIES = "oozie-log4j.properties";

    /**
     * Default value for the reload interval if {@link #LOG4J_RELOAD_ENV} is not set.
     */
    public static final String DEFAULT_RELOAD_INTERVAL = "10";

    private XLog log;
    private long interval;
    private boolean fromClasspath;
    private String log4jFileName;
    private boolean logOverWS = true;

    private static final String STARTUP_MESSAGE = "{E}"
            + " ******************************************************************************* {E}"
            + "  STARTUP MSG: Oozie BUILD_VERSION [{0}] compiled by [{1}] on [{2}]{E}"
            + "  STARTUP MSG:       revision [{3}]@[{4}]{E}"
            + "*******************************************************************************";

    private String oozieLogPath;
    private String oozieLogName;
    private int oozieLogRotation = -1;

    public XLogService() {
    }

    /**
     * Obtains the value of a system property or if not defined from an environment variable.
     *
     * @param envName environment variable name
     * @param defaultValue default value if not set
     * @return the value of the environment variable.
     */
    private static String getEnvValue(String envName, String defaultValue) {
        String value = System.getProperty(envName);
        if (value == null) {
            value = System.getenv(envName);
        }
        return (value != null) ? value : defaultValue;
    }

    /**
     * Initialize the log service.
     *
     * @param services services instance.
     * @throws ServiceException thrown if the log service could not be initialized.
     */
    public void init(Services services) throws ServiceException {
        String oozieHome = Services.getOozieHome();
        String oozieLogs = getEnvValue(OOZIE_LOGS_ENV, oozieHome + "/logs");
        System.setProperty(OOZIE_LOGS_ENV, oozieLogs);            
        try {
            LogManager.resetConfiguration();
            log4jFileName = getEnvValue(LOG4J_FILE_ENV, DEFAULT_LOG4J_PROPERTIES);
            if (log4jFileName.contains("/")) {
                throw new ServiceException(ErrorCode.E0011, log4jFileName);
            }
            if (!log4jFileName.endsWith(".properties")) {
                throw new ServiceException(ErrorCode.E0012, log4jFileName);
            }
            String configPath = ConfigurationService.getConfigurationDirectory();
            File log4jFile = new File(configPath, log4jFileName);
            if (log4jFile.exists()) {
                fromClasspath = false;
            }
            else {
                ClassLoader cl = Thread.currentThread().getContextClassLoader();
                URL log4jUrl = cl.getResource(log4jFileName);
                if (log4jUrl == null) {
                    throw new ServiceException(ErrorCode.E0013, log4jFileName, configPath);
                }
                fromClasspath = true;
            }

            if (fromClasspath) {
                ClassLoader cl = Thread.currentThread().getContextClassLoader();
                URL log4jUrl = cl.getResource(log4jFileName);
                PropertyConfigurator.configure(log4jUrl);
            }
            else {
                interval = Long.parseLong(getEnvValue(LOG4J_RELOAD_ENV, DEFAULT_RELOAD_INTERVAL));
                PropertyConfigurator.configureAndWatch(log4jFile.toString(), interval * 1000);
            }

            log = new XLog(LogFactory.getLog(getClass()));

            log.info(XLog.OPS, STARTUP_MESSAGE, BuildInfo.getBuildInfo().getProperty(BuildInfo.BUILD_VERSION),
                     BuildInfo.getBuildInfo().getProperty(BuildInfo.BUILD_USER_NAME), BuildInfo.getBuildInfo()
                    .getProperty(BuildInfo.BUILD_TIME), BuildInfo.getBuildInfo().getProperty(
                    BuildInfo.BUILD_VC_REVISION), BuildInfo.getBuildInfo()
                    .getProperty(BuildInfo.BUILD_VC_URL));

            String from = (fromClasspath) ? "CLASSPATH" : configPath;
            String reload = (fromClasspath) ? "disabled" : Long.toString(interval) + " sec";
            log.info("Log4j configuration file [{0}]", log4jFileName);
            log.info("Log4j configuration file loaded from [{0}]", from);
            log.info("Log4j reload interval [{0}]", reload);

            XLog.Info.reset();
            XLog.Info.defineParameter(USER);
            XLog.Info.defineParameter(GROUP);
            XLogStreamer.Filter.reset();
            XLogStreamer.Filter.defineParameter(USER);
            XLogStreamer.Filter.defineParameter(GROUP);

            // Getting configuration for oozie log via WS
            ClassLoader cl = Thread.currentThread().getContextClassLoader();
            InputStream is = (fromClasspath) ? cl.getResourceAsStream(log4jFileName) : new FileInputStream(log4jFile);
            extractInfoForLogWebService(is);
        }
        catch (IOException ex) {
            throw new ServiceException(ErrorCode.E0010, ex.getMessage(), ex);
        }
    }

    private void extractInfoForLogWebService(InputStream is) throws IOException {
        Properties props = new Properties();
        props.load(is);

        Configuration conf = new XConfiguration();
        for (Map.Entry entry : props.entrySet()) {
            conf.set((String) entry.getKey(), (String) entry.getValue());
        }
        String logFile = conf.get("log4j.appender.oozie.File");
        if (logFile == null) {
            log.warn("Oozie WS log will be disabled, missing property 'log4j.appender.oozie.File' for 'oozie' appender");
        }
        else {
            logFile = logFile.trim();
            int i = logFile.lastIndexOf("/");
            if (i == -1) {
                log.warn("Oozie WS log will be disabled, log file is not an absolute path [{0}] for 'oozie' appender",
                         logFile);
                logOverWS = false;
            }
            else {
                String pattern = conf.get("log4j.appender.oozie.DatePattern");
                if (pattern == null) {
                    log.warn("Oozie WS log will be disabled, missing property [log4j.appender.oozie.DatePattern]");
                    logOverWS = false;
                }
                else {
                    pattern = pattern.trim();
                    if (pattern.endsWith("HH")) {
                        oozieLogRotation = 60 * 60;
                    }
                    else {
                        if (pattern.endsWith("dd")) {
                            oozieLogRotation = 60 * 60 * 24;
                        }
                        else {
                            log.warn("Oozie WS log will be disabled, DatePattern [{0}] should end with 'HH' or 'dd'",
                                     pattern);
                            logOverWS = false;
                        }
                    }
                    if (oozieLogRotation > 0) {
                        oozieLogPath = logFile.substring(0, i);
                        oozieLogName = logFile.substring(i + 1);
                    }
                    else {
                        logOverWS = false;
                    }
                }
            }
        }
    }

    /**
     * Destroy the log service.
     */
    public void destroy() {
        LogManager.shutdown();
        XLog.Info.reset();
        XLogStreamer.Filter.reset();
    }

    /**
     * Group log info constant.
     */
    public static final String USER = "USER";

    /**
     * Group log info constant.
     */
    public static final String GROUP = "GROUP";

    /**
     * Return the public interface for log service.
     *
     * @return {@link XLogService}.
     */
    public Class<? extends Service> getInterface() {
        return XLogService.class;
    }

    /**
     * Instruments the log service. <p/> It sets instrumentation variables indicating the config file, reload interval
     * and if loaded from the classpath.
     *
     * @param instr instrumentation to use.
     */
    public void instrument(Instrumentation instr) {
        instr.addVariable("oozie", "version", new Instrumentation.Variable<String>() {
            public String getValue() {
                return BuildInfo.getBuildInfo().getProperty(BuildInfo.BUILD_VERSION);
            }
        });
        instr.addVariable(INSTRUMENTATION_GROUP, "config.file", new Instrumentation.Variable<String>() {
            public String getValue() {
                return log4jFileName;
            }
        });
        instr.addVariable(INSTRUMENTATION_GROUP, "reload.interval", new Instrumentation.Variable<Long>() {
            public Long getValue() {
                return interval;
            }
        });
        instr.addVariable(INSTRUMENTATION_GROUP, "from.classpath", new Instrumentation.Variable<Boolean>() {
            public Boolean getValue() {
                return fromClasspath;
            }
        });
        instr.addVariable(INSTRUMENTATION_GROUP, "log.over.web-service", new Instrumentation.Variable<Boolean>() {
            public Boolean getValue() {
                return logOverWS;
            }
        });
    }

    /**
     * Stream the log of a job.
     *
     * @param filter log streamer filter.
     * @param startTime start time for log events to filter.
     * @param endTime end time for log events to filter.
     * @param writer writer to stream the log to.
     * @throws IOException thrown if the log cannot be streamed.
     */
    public void streamLog(XLogStreamer.Filter filter, Date startTime, Date endTime, Writer writer) throws IOException {
        if (logOverWS) {
            new XLogStreamer(filter, writer, oozieLogPath, oozieLogName, oozieLogRotation)
                    .streamLog(startTime, endTime);
        }
        else {
            writer.write("Log streaming disabled!!");
        }

    }

    String getLog4jProperties() {
        return log4jFileName;
    }

    boolean getFromClasspath() {
        return fromClasspath;
    }

}
