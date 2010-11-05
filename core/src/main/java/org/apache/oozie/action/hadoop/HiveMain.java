/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.oozie.action.hadoop;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.cli.CliDriver;

public class HiveMain extends LauncherMain {
    public static final String USER_HIVE_DEFAULT_FILE = "oozie-user-hive-default.xml";

    public static final String HIVE_L4J_PROPS = "hive-log4j.properties";
    public static final String HIVE_EXEC_L4J_PROPS = "hive-exec-log4j.properties";
    public static final String HIVE_SITE_CONF = "hive-site.xml";
    private static final String HIVE_SCRIPT = "oozie.hive.script";
    private static final String HIVE_PARAMS = "oozie.hive.params";

    public static void main(String[] args) throws Exception {
        run(HiveMain.class, args);
    }

    private static Configuration initActionConf() {
        // Loading action conf prepared by Oozie
        Configuration hiveConf = new Configuration(false);

        String actionXml = System.getProperty("oozie.action.conf.xml");

        if (actionXml == null) {
            throw new RuntimeException("Missing Java System Property [oozie.action.conf.xml]");
        }
        if (!new File(actionXml).exists()) {
            throw new RuntimeException("Action Configuration XML file [" + actionXml + "] does not exist");
        } else {
            System.out.println("Using action configuration file " + actionXml);
        }

        hiveConf.addResource(new Path("file:///", actionXml));

        // Propagate delegation related props from launcher job to Hive job
        String delegationToken = System.getenv("HADOOP_TOKEN_FILE_LOCATION");
        if (delegationToken != null) {
            hiveConf.set("mapreduce.job.credentials.binary", delegationToken);
            System.out.println("------------------------");
            System.out.println("Setting env property for mapreduce.job.credentials.binary to: " + delegationToken);
            System.out.println("------------------------");
            System.setProperty("mapreduce.job.credentials.binary", delegationToken);
        } else {
            System.out.println("Non-Kerberos execution");
        }

        // Have to explicitly unset this property or Hive will not set it.
        hiveConf.set("mapred.job.name", "");

        // See https://issues.apache.org/jira/browse/HIVE-1411
        hiveConf.set("datanucleus.plugin.pluginRegistryBundleCheck", "LOG");

        // to force hive to use the jobclient to submit the job, never using HADOOPBIN (to do localmode)
        hiveConf.setBoolean("hive.exec.mode.local.auto", false);

        return hiveConf;
    }

    public static String setUpHiveLog4J(Configuration hiveConf) throws IOException {
        //Logfile to capture job IDs
        String hadoopJobId = System.getProperty("oozie.launcher.job.id");
        if (hadoopJobId == null) {
            throw new RuntimeException("Launcher Hadoop Job ID system property not set");
        }

        String logFile = new File("hive-oozie-" + hadoopJobId + ".log").getAbsolutePath();

        Properties hadoopProps = new Properties();

        // Preparing log4j configuration
        URL log4jFile = Thread.currentThread().getContextClassLoader().getResource("log4j.properties");
        if (log4jFile != null) {
            // getting hadoop log4j configuration
            hadoopProps.load(log4jFile.openStream());
        }

        String logLevel = hiveConf.get("oozie.hive.log.level", "INFO");

        hadoopProps.setProperty("log4j.logger.org.apache.hadoop.hive", logLevel + ", A");
        hadoopProps.setProperty("log4j.logger.hive", logLevel + ", A");
        hadoopProps.setProperty("log4j.logger.DataNucleus", logLevel + ", A");
        hadoopProps.setProperty("log4j.logger.DataStore", logLevel + ", A");
        hadoopProps.setProperty("log4j.logger.JPOX", logLevel + ", A");
        hadoopProps.setProperty("log4j.appender.A", "org.apache.log4j.ConsoleAppender");
        hadoopProps.setProperty("log4j.appender.A.layout", "org.apache.log4j.PatternLayout");
        hadoopProps.setProperty("log4j.appender.A.layout.ConversionPattern", "%-4r [%t] %-5p %c %x - %m%n");

        hadoopProps.setProperty("log4j.appender.jobid", "org.apache.log4j.FileAppender");
        hadoopProps.setProperty("log4j.appender.jobid.file", logFile);
        hadoopProps.setProperty("log4j.appender.jobid.layout", "org.apache.log4j.PatternLayout");
        hadoopProps.setProperty("log4j.appender.jobid.layout.ConversionPattern", "%-4r [%t] %-5p %c %x - %m%n");
        hadoopProps.setProperty("log4j.logger.org.apache.hadoop.hive.ql.exec", "INFO, jobid");

        String localProps = new File(HIVE_L4J_PROPS).getAbsolutePath();
        OutputStream os1 = new FileOutputStream(localProps);
        hadoopProps.store(os1, "");
        os1.close();

        localProps = new File(HIVE_EXEC_L4J_PROPS).getAbsolutePath();
        os1 = new FileOutputStream(localProps);
        hadoopProps.store(os1, "");
        os1.close();
        return logFile;
    }

    public static Configuration setUpHiveSite() throws Exception {
        Configuration hiveConf = initActionConf();

        // Write the action configuration out to hive-site.xml
        OutputStream os = new FileOutputStream(HIVE_SITE_CONF);
        hiveConf.writeXml(os);
        os.close();

        System.out.println();
        System.out.println("Hive Configuration Properties:");
        System.out.println("------------------------");
        for (Entry<String, String> entry : hiveConf) {
            System.out.println(entry.getKey() + "=" + entry.getValue());
        }
        System.out.flush();
        System.out.println("------------------------");
        System.out.println();
        return hiveConf;
    }

    protected void run(String[] args) throws Exception {
        if (System.getenv("HADOOP_HOME") == null) {
            throw new RuntimeException("'HADOOP_HOME' environment variable undefined, Hive cannot run");
        }
        System.out.println();
        System.out.println("Oozie Hive action configuration");
        System.out.println("=================================================================");

        Configuration hiveConf = setUpHiveSite();

        List<String> arguments = new ArrayList<String>();
        String scriptPath = hiveConf.get(HIVE_SCRIPT);

        if (scriptPath == null) {
            throw new RuntimeException("Action Configuration does not have [oozie.hive.script] property");
        }

        if (!new File(scriptPath).exists()) {
            throw new RuntimeException("Hive script file [" + scriptPath + "] does not exist");
        }

        // check if hive-default.xml is in the classpath, if not look for oozie-hive-default.xml
        // in the current directory (it will be there if the Hive action has the 'oozie.hive.defaults'
        // property) and rename it to hive-default.xml
        if (Thread.currentThread().getContextClassLoader().getResource("hive-default.xml") == null) {
            File userProvidedDefault = new File(USER_HIVE_DEFAULT_FILE);
            if (userProvidedDefault.exists()) {
                if (!userProvidedDefault.renameTo(new File("hive-default.xml"))) {
                    throw new RuntimeException(
                            "Could not rename user provided Hive defaults file to 'hive-default.xml'");
                }
                System.out.println("Using 'hive-default.xml' defined in the Hive action");
            }
            else {
                throw new RuntimeException(
                        "Hive JAR does not bundle a 'hive-default.xml' and Hive action does not define one");
            }
        }
        else {
            System.out.println("Using 'hive-default.xml' defined in the Hive JAR");
            File userProvidedDefault = new File(USER_HIVE_DEFAULT_FILE);
            if (userProvidedDefault.exists()) {
                System.out.println("WARNING: Ignoring user provided Hive defaults");
            }
        }
        System.out.println();

        String logFile = setUpHiveLog4J(hiveConf);

        // print out current directory & its contents
        File localDir = new File("dummy").getAbsoluteFile().getParentFile();
        System.out.println("Current (local) dir = " + localDir.getAbsolutePath());
        System.out.println("------------------------");
        for (String file : localDir.list()) {
            System.out.println("  " + file);
        }
        System.out.println("------------------------");
        System.out.println();

        // Prepare the Hive Script
        String script = readStringFromFile(scriptPath);
        System.out.println();
        System.out.println("Original script [" + scriptPath + "] content: ");
        System.out.println("------------------------");
        System.out.println(script);
        System.out.println("------------------------");
        System.out.println();

        String[] params = MapReduceMain.getStrings(hiveConf, HIVE_PARAMS);
        if (params.length > 0) {
            Map<String, String> varMap = new HashMap<String, String>();
            System.out.println("Parameters:");
            System.out.println("------------------------");
            for (String param : params) {
                System.out.println("  " + param);

                int idx = param.indexOf('=');
                if (idx == -1) {
                    throw new RuntimeException("Parameter expression must contain an assignment: " + param);
                } else if (idx == 0) {
                    throw new RuntimeException("Parameter value not specified: " + param);
                }
                String var = param.substring(0, idx);
                String val = param.substring(idx + 1, param.length());
                varMap.put(var, val);
            }
            System.out.println("------------------------");
            System.out.println();

            String resolvedScript = substitute(varMap, script);
            scriptPath = scriptPath + ".sub";
            writeStringToFile(scriptPath, resolvedScript);

            System.out.println("Resolved script [" + scriptPath + "] content: ");
            System.out.println("------------------------");
            System.out.println(resolvedScript);
            System.out.println("------------------------");
            System.out.println();
        }

        arguments.add("-f");
        arguments.add(scriptPath);


        System.out.println("Hive command arguments :");
        for (String arg : arguments) {
            System.out.println("             " + arg);
        }
        System.out.println();

        System.out.println("=================================================================");
        System.out.println();
        System.out.println(">>> Invoking Hive command line now >>>");
        System.out.println();
        System.out.flush();

        try {
            runHive(arguments.toArray(new String[arguments.size()]));
        }
        catch (SecurityException ex) {
            if (LauncherSecurityManager.getExitInvoked()) {
                if (LauncherSecurityManager.getExitCode() != 0) {
                    throw ex;
                }
            }
        }

        System.out.println("\n<<< Invocation of Hive command completed <<<\n");

        // harvesting and recording Hadoop Job IDs
        Properties jobIds = getHadoopJobIds(logFile, JOB_ID_LOG_PREFIX);
        File file = new File(System.getProperty("oozie.action.output.properties"));
        OutputStream os = new FileOutputStream(file);
        jobIds.store(os, "");
        os.close();
        System.out.println(" Hadoop Job IDs executed by Hive: " + jobIds.getProperty("hadoopJobs"));
        System.out.println();
    }

    private void runHive(String[] args) throws Exception {
        CliDriver.main(args);
    }

    public static void setHiveScript(Configuration conf, String script, String[] params) {
        conf.set(HIVE_SCRIPT, script);
        MapReduceMain.setStrings(conf, HIVE_PARAMS, params);
    }

    private static String readStringFromFile(String filePath) throws IOException {
        String line;
        BufferedReader br = new BufferedReader(new FileReader(filePath));
        StringBuilder sb = new StringBuilder();
        String sep = System.getProperty("line.separator");
        while ((line = br.readLine()) != null) {
            sb.append(line).append(sep);
        }
        return sb.toString();
     }

    private static void writeStringToFile(String filePath, String str) throws IOException {
        BufferedWriter out = new BufferedWriter(new FileWriter(filePath));
        out.write(str);
        out.close();
    }

    static String substitute(Map<String, String> vars, String expr) {
        for (Map.Entry<String, String> entry : vars.entrySet()) {
            String var = "${" + entry.getKey() + "}";
            String value = entry.getValue();
            expr = expr.replace(var, value);
        }
        return expr;
    }

    //TODO: Hive should provide a programmatic way of spitting out Hadoop jobs
    private static final String JOB_ID_LOG_PREFIX = "Ended Job = ";

    public static Properties getHadoopJobIds(String logFile, String prefix) throws IOException {
        Properties props = new Properties();
        StringBuffer sb = new StringBuffer(100);
        if (!new File(logFile).exists()) {
            System.err.println("hive log file: " + logFile + "  not present. Therefore no Hadoop jobids found");
            props.setProperty("hadoopJobs", "");
        }
        else {
            BufferedReader br = new BufferedReader(new FileReader(logFile));
            String line = br.readLine();
            String separator = "";
            while (line != null) {
                if (line.contains(prefix)) {
                    int jobIdStarts = line.indexOf(prefix) + prefix.length();
                    String jobId = line.substring(jobIdStarts).trim();

                    //Doing this because Hive now does things like ConditionalTask which are not Hadoop jobs.
                    if (jobId.startsWith("job_")) {
                        sb.append(separator).append(jobId);
                        separator = ",";
                    }
                }
                line = br.readLine();
            }
            br.close();
            props.setProperty("hadoopJobs", sb.toString());
        }
        return props;
    }

}
