package org.apache.oozie.action.hadoop;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.io.Writer;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.jdo.Query;

import jline.FileNameCompletor;
import junit.framework.TestCase;

import org.antlr.runtime.ANTLRFileStream;
import org.apache.commons.codec.BinaryDecoder;
import org.apache.commons.collections.Bag;
import org.apache.commons.lang.text.StrMatcher;
import org.apache.commons.logging.LogSource;
import org.apache.commons.logging.impl.SimpleLog;
import org.apache.derby.iapi.services.io.DerbyIOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.contrib.serde2.RegexSerDe;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.ql.exec.ExecDriver;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.service.HiveServer;
import org.apache.hadoop.hive.shims.Hadoop20Shims;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.log4j.lf5.Log4JLogRecord;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.WorkflowAppService;
import org.apache.oozie.util.ClassUtils;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XmlUtils;
import org.apache.thrift.TBase;
import org.apache.velocity.app.VelocityEngine;
import org.datanucleus.FetchGroup;
import org.datanucleus.store.rdbms.RDBMSManager;
import org.jdom.Element;
import org.jdom.Namespace;
import org.json.JSONML;
import org.objectweb.asm.ByteVector;

import com.facebook.fb303.FacebookBase;

public class TestHiveActionExecutor extends ActionExecutorTestCase {

    private static final String NEW_LINE =
        System.getProperty("line.separator", "\n");

    private static final String SAMPLE_DATA_TEXT =
        "3\n4\n6\n1\n2\n7\n9\n0\n8\n";

    private static final String HIVE_SCRIPT_FILENAME = "script.q";

    private static final String INPUT_DIRNAME = "input";
    private static final String OUTPUT_DIRNAME = "output";
    private static final String DATA_FILENAME = "data.txt";

    protected void setSystemProps() {
        super.setSystemProps();
        setSystemProperty("oozie.service.ActionService.executor.classes",
                HiveActionExecutor.class.getName());
    }

    public void testVariableSubstitutionSimple() throws Exception {
        String key = "test.random.key";
        String keyExpression = "${" + key + "}";
        Map<String, String> varMap = new HashMap<String, String>();
        varMap.put(key, keyExpression);
        String value = HiveMain.substitute(varMap, keyExpression);
        assertTrue("Unexpected value " + value, value.equals(keyExpression));
    }

    public void testSetupMethods() throws Exception {
        HiveActionExecutor ae = new HiveActionExecutor();
        assertEquals("hive", ae.getType());
    }

    public void testLauncherJar() throws Exception {
        HiveActionExecutor ae = new HiveActionExecutor();
        Path jar = new Path(ae.getOozieRuntimeDir(), ae.getLauncherJarName());
        assertTrue(new File(jar.toString()).exists());
    }

    private String getHiveScript(String inputPath, String outputPath) {
        StringBuilder buffer = new StringBuilder(NEW_LINE);
        buffer.append("set -v;").append(NEW_LINE);
        buffer.append("CREATE EXTERNAL TABLE test (a INT) STORED AS");
        buffer.append(NEW_LINE).append("TEXTFILE LOCATION '");
        buffer.append(inputPath).append("';").append(NEW_LINE);
        buffer.append("INSERT OVERWRITE DIRECTORY '");
        buffer.append(outputPath).append("'").append(NEW_LINE);
        buffer.append("SELECT (a-1) FROM test;").append(NEW_LINE);

        return buffer.toString();
    }

    private String getActionXml() {
        String script = "<hive xmlns=''uri:oozie:hive-action:0.2''>" +
        "<job-tracker>{0}</job-tracker>" +
        "<name-node>{1}</name-node>" +
        "<configuration>" +
        "<property>" +
        "<name>javax.jdo.option.ConnectionURL</name>" +
        "<value>jdbc:hsqldb:mem:hive-main;create=true</value>" +
        "</property>" +
        "<property>" +
        "<name>javax.jdo.option.ConnectionDriverName</name>" +
        "<value>org.hsqldb.jdbcDriver</value>" +
        "</property>" +
        "<property>" +
        "<name>javax.jdo.option.ConnectionUserName</name>" +
        "<value>sa</value>" +
        "</property>" +
        "<property>" +
        "<name>javax.jdo.option.ConnectionPassword</name>" +
        "<value> </value>" +
        "</property>" +
        "<property>" +
        "<name>oozie.hive.defaults</name>" +
        "<value>user-hive-default.xml</value>" +
        "</property>" +
        "</configuration>" +
        "<script>" + HIVE_SCRIPT_FILENAME + "</script>" +
        "</hive>";
        return MessageFormat.format(script, getJobTrackerUri(), getNameNodeUri());
    }

    public void testHiveAction() throws Exception {
        Path inputDir = new Path(getFsTestCaseDir(), INPUT_DIRNAME);
        Path outputDir = new Path(getFsTestCaseDir(), OUTPUT_DIRNAME);

        FileSystem fs = getFileSystem();
        Path script = new Path(getAppPath(), HIVE_SCRIPT_FILENAME);
        Writer scriptWriter = new OutputStreamWriter(fs.create(script));
        scriptWriter.write(getHiveScript(inputDir.toString(), outputDir.toString()));
        scriptWriter.close();

        Writer dataWriter = new OutputStreamWriter(fs.create(new Path(inputDir, DATA_FILENAME)));
        dataWriter.write(SAMPLE_DATA_TEXT);
        dataWriter.close();

        InputStream is = IOUtils.getResourceAsStream("user-hive-default.xml", -1);
        OutputStream os = fs.create(new Path(getAppPath(), "user-hive-default.xml"));
        IOUtils.copyStream(is, os);

        Context context = createContext(getActionXml());
        final RunningJob launcherJob = submitAction(context);
        String launcherId = context.getAction().getExternalId();
        waitFor(200 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return launcherJob.isComplete();
            }
        });
        assertTrue(launcherJob.isSuccessful());

        assertFalse(LauncherMapper.hasIdSwap(launcherJob));

        HiveActionExecutor ae = new HiveActionExecutor();
        ae.check(context, context.getAction());
        assertTrue(launcherId.equals(context.getAction().getExternalId()));
        assertEquals("SUCCEEDED", context.getAction().getExternalStatus());
        assertNotNull(context.getAction().getData());
        ae.end(context, context.getAction());
        assertEquals(WorkflowAction.Status.OK, context.getAction().getStatus());

        assertNotNull(context.getAction().getData());
        Properties outputData = new Properties();
        outputData.load(new StringReader(context.getAction().getData()));
        assertTrue(outputData.containsKey("hadoopJobs"));
        assertTrue(outputData.getProperty("hadoopJobs").trim().length() > 0);

        assertTrue(fs.exists(outputDir));
        assertTrue(fs.isDirectory(outputDir));
    }

    private RunningJob submitAction(Context context) throws Exception {
        HiveActionExecutor ae = new HiveActionExecutor();

        WorkflowAction action = context.getAction();

        ae.prepareActionDir(getFileSystem(), context);
        ae.submitLauncher(context, action);

        String jobId = action.getExternalId();
        String jobTracker = action.getTrackerUri();
        String consoleUrl = action.getConsoleUrl();
        assertNotNull(jobId);
        assertNotNull(jobTracker);
        assertNotNull(consoleUrl);
        Element e = XmlUtils.parseXml(action.getConf());
        Namespace ns = Namespace.getNamespace("uri:oozie:hive-action:0.2");
        XConfiguration conf =
                new XConfiguration(new StringReader(XmlUtils.prettyPrint(e.getChild("configuration", ns)).toString()));
        conf.set("mapred.job.tracker", e.getChildTextTrim("job-tracker", ns));
        conf.set("fs.default.name", e.getChildTextTrim("name-node", ns));
        conf.set("user.name", context.getProtoActionConf().get("user.name"));
        conf.set("group.name", getTestGroup());
        injectKerberosInfo(conf);
        JobConf jobConf = new JobConf(conf);
        String user = jobConf.get("user.name");
        String group = jobConf.get("group.name");
        JobClient jobClient = Services.get().get(HadoopAccessorService.class).createJobClient(user, group, jobConf);
        final RunningJob runningJob = jobClient.getJob(JobID.forName(jobId));
        assertNotNull(runningJob);
        return runningJob;
    }

    private String copyJar(String targetFile, Class<?> anyContainedClass)
            throws Exception {
        String file = ClassUtils.findContainingJar(anyContainedClass);
        System.out.println("[copy-jar] class: " + anyContainedClass
                + ", local jar ==> " + file);
        Path targetPath = new Path(getAppPath(), targetFile);
        FileSystem fs = getFileSystem();
        InputStream is = new FileInputStream(file);
        OutputStream os = fs.create(new Path(getAppPath(), targetPath));
        IOUtils.copyStream(is, os);
        return targetPath.toString();
    }

    private Context createContext(String actionXml) throws Exception {
        List<String> jars = new ArrayList<String>();
        HiveActionExecutor ae = new HiveActionExecutor();

        jars.add(copyJar("lib/libfb303-x.jar", FacebookBase.class));
        jars.add(copyJar("lib/libthrift-x.jar", TBase.class));
        jars.add(copyJar("lib/libjson-x.jar", JSONML.class));
        jars.add(copyJar("lib/jdo2-api-x.jar", Query.class));
        jars.add(copyJar("lib/antlr-runtime-x.jar", ANTLRFileStream.class));
        jars.add(copyJar("lib/asm-x.jar", ByteVector.class));
        jars.add(copyJar("lib/commons-codec-x.jar", BinaryDecoder.class));
        jars.add(copyJar("lib/commons-collection-x.jar", Bag.class));
        jars.add(copyJar("lib/commons-lang-x.jar", StrMatcher.class));
        jars.add(copyJar("lib/commons-logging-x.jar", LogSource.class));
        jars.add(copyJar("lib/commons-logging-api-x.jar", SimpleLog.class));
        jars.add(copyJar("lib/datanucleus-core-x.jar", FetchGroup.class));
        jars.add(copyJar("lib/datanucleus-rdbms-x.jar", RDBMSManager.class));
        jars.add(copyJar("lib/derby.jar", DerbyIOException.class));
        jars.add(copyJar("lib/jline.jar", FileNameCompletor.class));
        jars.add(copyJar("lib/junit-x.jar", TestCase.class));
        jars.add(copyJar("lib/log4j-x.jar", Log4JLogRecord.class));
        jars.add(copyJar("lib/velocity-x.jar", VelocityEngine.class));

        jars.add(copyJar("lib/hive-cli-x.jar", CliSessionState.class));
        jars.add(copyJar("lib/hive-common-x.jar", HiveConf.class));
        jars.add(copyJar("lib/hive-exec-x.jar", ExecDriver.class));
        jars.add(copyJar("lib/hive-metastore-x.jar", HiveMetaStore.class));
        jars.add(copyJar("lib/hive-serde-x.jar", SerDe.class));
        jars.add(copyJar("lib/hive-service-x.jar", HiveServer.class));
        jars.add(copyJar("lib/hive-shims-x.jar", Hadoop20Shims.class));
        jars.add(copyJar("lib/hive-contrib-x.jar", RegexSerDe.class));

        XConfiguration protoConf = new XConfiguration();
        protoConf.set(WorkflowAppService.HADOOP_USER, getTestUser());
        protoConf.set(WorkflowAppService.HADOOP_UGI, getTestUser() + "," + getTestGroup());
        protoConf.set(OozieClient.GROUP_NAME, getTestGroup());
        injectKerberosInfo(protoConf);
        protoConf.setStrings(WorkflowAppService.APP_LIB_PATH_LIST,
                                    jars.toArray(new String[jars.size()]));

        WorkflowJobBean wf = createBaseWorkflow(protoConf, "hive-action");
        WorkflowActionBean action = (WorkflowActionBean) wf.getActions().get(0);
        action.setType(ae.getType());
        action.setConf(actionXml);

        return new Context(wf, action);
    }

}
