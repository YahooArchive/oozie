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
package org.apache.oozie.cli;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.oozie.BuildInfo;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.XOozieClient;
import org.apache.oozie.client.OozieClient.SYSTEM_MODE;
import org.apache.oozie.client.rest.JsonCoordinatorAction;
import org.apache.oozie.client.rest.RestConstants;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;
import org.xml.sax.SAXException;

/**
 * Oozie command line utility.
 */
public class OozieCLI {
    public static final String ENV_OOZIE_URL = "OOZIE_URL";
    public static final String WS_HEADER_PREFIX = "header:";

    public static final String HELP_CMD = "help";
    public static final String VERSION_CMD = "version";
    public static final String JOB_CMD = "job";
    public static final String JOBS_CMD = "jobs";
    public static final String ADMIN_CMD = "admin";
    public static final String VALIDATE_CMD = "validate";
    public static final String SLA_CMD = "sla";
    public static final String PIG_CMD = "pig";

    public static final String OOZIE_OPTION = "oozie";
    public static final String CONFIG_OPTION = "config";
    public static final String SUBMIT_OPTION = "submit";
    public static final String OFFSET_OPTION = "offset";
    public static final String START_OPTION = "start";
    public static final String RUN_OPTION = "run";
    public static final String DRYRUN_OPTION = "dryrun";
    public static final String SUSPEND_OPTION = "suspend";
    public static final String RESUME_OPTION = "resume";
    public static final String KILL_OPTION = "kill";
    public static final String CHANGE_OPTION = "change";
    public static final String CHANGE_VALUE_OPTION = "value";
    public static final String RERUN_OPTION = "rerun";
    public static final String INFO_OPTION = "info";
    public static final String LOG_OPTION = "log";
    public static final String DEFINITION_OPTION = "definition";

    public static final String LEN_OPTION = "len";
    public static final String FILTER_OPTION = "filter";
    public static final String JOBTYPE_OPTION = "jobtype";
    public static final String SYSTEM_MODE_OPTION = "systemmode";
    public static final String VERSION_OPTION = "version";
    public static final String STATUS_OPTION = "status";
    public static final String LOCAL_TIME_OPTION = "localtime";
    public static final String QUEUE_DUMP_OPTION = "queuedump";
    public static final String RERUN_ACTION_OPTION = "action";
    public static final String RERUN_DATE_OPTION = "date";
    public static final String RERUN_REFRESH_OPTION = "refresh";
    public static final String RERUN_NOCLEANUP_OPTION = "nocleanup";

    public static final String VERBOSE_OPTION = "verbose";
    public static final String VERBOSE_DELIMITER = "\t";

    public static final String PIGFILE_OPTION = "file";

    private static final String[] OOZIE_HELP = {
            "the env variable '" + ENV_OOZIE_URL + "' is used as default value for the '-" + OOZIE_OPTION + "' option",
            "custom headers for Oozie web services can be specified using '-D" + WS_HEADER_PREFIX + "NAME=VALUE'" };

    private static final String RULER;
    private static final int LINE_WIDTH = 132;

    private boolean used;

    static {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < LINE_WIDTH; i++) {
            sb.append("-");
        }
        RULER = sb.toString();
    }

    /**
     * Entry point for the Oozie CLI when invoked from the command line.
     * <p/>
     * Upon completion this method exits the JVM with '0' (success) or '-1'
     * (failure).
     *
     * @param args options and arguments for the Oozie CLI.
     */
    public static void main(String[] args) {
        System.exit(new OozieCLI().run(args));
    }

    /**
     * Create an Oozie CLI instance.
     */
    public OozieCLI() {
        used = false;
    }

    /**
     * Return Oozie CLI top help lines.
     *
     * @return help lines.
     */
    protected String[] getCLIHelp() {
        return OOZIE_HELP;
    }

    protected Options createAdminOptions() {
        Option oozie = new Option(OOZIE_OPTION, true, "Oozie URL");
        Option system_mode = new Option(SYSTEM_MODE_OPTION, true,
                "Supported in Oozie-2.0 or later versions ONLY. Change oozie system mode [NORMAL|NOWEBSERVICE|SAFEMODE]");
        Option status = new Option(STATUS_OPTION, false, "show the current system status");
        Option version = new Option(VERSION_OPTION, false, "show Oozie server build version");
        Option queuedump = new Option(QUEUE_DUMP_OPTION, false, "show Oozie server queue elements");
        Options adminOptions = new Options();
        adminOptions.addOption(oozie);
        OptionGroup group = new OptionGroup();
        group.addOption(system_mode);
        group.addOption(status);
        group.addOption(version);
        group.addOption(queuedump);
        adminOptions.addOptionGroup(group);
        return adminOptions;
    }

    protected Options createJobOptions() {
        Option oozie = new Option(OOZIE_OPTION, true, "Oozie URL");
        Option config = new Option(CONFIG_OPTION, true, "job configuration file '.xml' or '.properties'");
        Option submit = new Option(SUBMIT_OPTION, false, "submit a job");
        Option run = new Option(RUN_OPTION, false, "run a job");
        Option rerun = new Option(RERUN_OPTION, true,
                "rerun a job  (coordinator requires -action or -date)");
        Option dryrun = new Option(DRYRUN_OPTION, false,
                "Supported in Oozie-2.0 or later versions ONLY - dryrun or test run a coordinator job, job is not queued");
        Option start = new Option(START_OPTION, true, "start a job");
        Option suspend = new Option(SUSPEND_OPTION, true, "suspend a job");
        Option resume = new Option(RESUME_OPTION, true, "resume a job");
        Option kill = new Option(KILL_OPTION, true, "kill a job");
        Option change = new Option(CHANGE_OPTION, true, "change a coordinator job");
        Option changeValue = new Option(CHANGE_VALUE_OPTION, true, "new endtime/concurrency/pausetime value for changing a coordinator job");
        Option info = new Option(INFO_OPTION, true, "info of a job");
        Option offset = new Option(OFFSET_OPTION, true, "job info offset of actions (default '1', requires -info)");
        Option len = new Option(LEN_OPTION, true, "number of actions (default TOTAL ACTIONS, requires -info)");
        Option localtime = new Option(LOCAL_TIME_OPTION, false, "use local time (default GMT)");
        Option log = new Option(LOG_OPTION, true, "job log");
        Option definition = new Option(DEFINITION_OPTION, true, "job definition");
        Option verbose = new Option(VERBOSE_OPTION, false, "verbose mode");
        Option rerun_action = new Option(RERUN_ACTION_OPTION, true, "coordinator rerun on action ids (requires -rerun)");
        Option rerun_date = new Option(RERUN_DATE_OPTION, true, "coordinator rerun on action dates (requires -rerun)");
        Option rerun_refresh = new Option(RERUN_REFRESH_OPTION, false,
                "re-materialize the coordinator rerun actions (requires -rerun)");
        Option rerun_nocleanup = new Option(RERUN_NOCLEANUP_OPTION, false,
                "do not clean up output-events of the coordiantor rerun actions (requires -rerun)");
        Option property = OptionBuilder.withArgName( "property=value" ).hasArgs(2)
                .withValueSeparator().withDescription( "set/override value for given property" ).create( "D" );

        OptionGroup actions = new OptionGroup();
        actions.addOption(submit);
        actions.addOption(start);
        actions.addOption(run);
        actions.addOption(dryrun);
        actions.addOption(suspend);
        actions.addOption(resume);
        actions.addOption(kill);
        actions.addOption(change);
        actions.addOption(info);
        actions.addOption(rerun);
        actions.addOption(log);
        actions.addOption(definition);
        actions.setRequired(true);
        Options jobOptions = new Options();
        jobOptions.addOption(oozie);
        jobOptions.addOption(config);
        jobOptions.addOption(property);
        jobOptions.addOption(changeValue);
        jobOptions.addOption(localtime);
        jobOptions.addOption(verbose);
        jobOptions.addOption(offset);
        jobOptions.addOption(len);
        jobOptions.addOption(rerun_action);
        jobOptions.addOption(rerun_date);
        jobOptions.addOption(rerun_refresh);
        jobOptions.addOption(rerun_nocleanup);
        jobOptions.addOptionGroup(actions);
        return jobOptions;
    }

    protected Options createJobsOptions() {
        Option oozie = new Option(OOZIE_OPTION, true, "Oozie URL");
        Option start = new Option(OFFSET_OPTION, true, "jobs offset (default '1')");
        Option jobtype = new Option(JOBTYPE_OPTION, true, "job type ('Supported in Oozie-2.0 or later versions ONLY - coordinator' or 'wf' (default))");
        Option len = new Option(LEN_OPTION, true, "number of jobs (default '100')");
        Option filter = new Option(FILTER_OPTION, true, "user=<U>;name=<N>;group=<G>;status=<S>;...");
        Option localtime = new Option(LOCAL_TIME_OPTION, false, "use local time (default GMT)");
        Option verbose = new Option(VERBOSE_OPTION, false, "verbose mode");
        start.setType(Integer.class);
        len.setType(Integer.class);
        Options jobsOptions = new Options();
        jobsOptions.addOption(oozie);
        jobsOptions.addOption(localtime);
        jobsOptions.addOption(start);
        jobsOptions.addOption(len);
        jobsOptions.addOption(oozie);
        jobsOptions.addOption(filter);
        jobsOptions.addOption(jobtype);
        jobsOptions.addOption(verbose);
        return jobsOptions;
    }

    protected Options createSlaOptions() {
        Option oozie = new Option(OOZIE_OPTION, true, "Oozie URL");
        Option start = new Option(OFFSET_OPTION, true, "start offset (default '0')");
        Option len = new Option(LEN_OPTION, true, "number of results (default '100')");
        start.setType(Integer.class);
        len.setType(Integer.class);
        Options slaOptions = new Options();
        slaOptions.addOption(start);
        slaOptions.addOption(len);
        slaOptions.addOption(oozie);
        return slaOptions;
    }

    protected Options createPigOptions() {
        Option oozie = new Option(OOZIE_OPTION, true, "Oozie URL");
        Option config = new Option(CONFIG_OPTION, true, "job configuration file '.properties'");
        Option pigFile = new Option(PIGFILE_OPTION, true, "Pig script");
        Option property = OptionBuilder.withArgName( "property=value" ).hasArgs(2)
                .withValueSeparator().withDescription( "set/override value for given property" ).create( "D" );
        Options pigOptions = new Options();
        pigOptions.addOption(oozie);
        pigOptions.addOption(config);
        pigOptions.addOption(property);
        pigOptions.addOption(pigFile);
        return pigOptions;
    }

    /**
     * Run a CLI programmatically.
     * <p/>
     * It does not exit the JVM.
     * <p/>
     * A CLI instance can be used only once.
     *
     * @param args options and arguments for the Oozie CLI.
     * @return '0' (success), '-1' (failure).
     */
    public synchronized int run(String[] args) {
        if (used) {
            throw new IllegalStateException("CLI instance already used");
        }
        used = true;

        CLIParser parser = new CLIParser(OOZIE_OPTION, getCLIHelp());
        parser.addCommand(HELP_CMD, "", "display usage", new Options(), false);
        parser.addCommand(VERSION_CMD, "", "show client version", new Options(), false);
        parser.addCommand(JOB_CMD, "", "job operations", createJobOptions(), false);
        parser.addCommand(JOBS_CMD, "", "jobs status", createJobsOptions(), false);
        parser.addCommand(ADMIN_CMD, "", "admin operations", createAdminOptions(), false);
        parser.addCommand(VALIDATE_CMD, "", "validate a workflow XML file", new Options(), true);
        parser.addCommand(SLA_CMD, "", "sla operations (Supported in Oozie-2.0 or later)", createSlaOptions(), false);
        parser.addCommand(PIG_CMD, "-X ", "submit a pig job, everything after '-X' are pass-through parameters to pig",
                          createPigOptions(), true);

        try {
            CLIParser.Command command = parser.parse(args);
            if (command.getName().equals(HELP_CMD)) {
                parser.showHelp();
            }
            else if (command.getName().equals(JOB_CMD)) {
                jobCommand(command.getCommandLine());
            }
            else if (command.getName().equals(JOBS_CMD)) {
                jobsCommand(command.getCommandLine());
            }
            else if (command.getName().equals(ADMIN_CMD)) {
                adminCommand(command.getCommandLine());
            }
            else if (command.getName().equals(VERSION_CMD)) {
                versionCommand();
            }
            else if (command.getName().equals(VALIDATE_CMD)) {
                validateCommand(command.getCommandLine());
            }
            else if (command.getName().equals(SLA_CMD)) {
                slaCommand(command.getCommandLine());
            }
            else if (command.getName().equals(PIG_CMD)) {
                pigCommand(command.getCommandLine());
            }

            return 0;
        }
        catch (OozieCLIException ex) {
            System.err.println("Error: " + ex.getMessage());
            return -1;
        }
        catch (ParseException ex) {
            System.err.println("Invalid sub-command: " + ex.getMessage());
            System.err.println();
            System.err.println(parser.shortHelp());
            return -1;
        }
        catch (Exception ex) {
            ex.printStackTrace();
            System.err.println(ex.getMessage());
            return -1;
        }
    }

    protected String getOozieUrl(CommandLine commandLine) {
        String url = commandLine.getOptionValue(OOZIE_OPTION);
        if (url == null) {
            url = System.getenv(ENV_OOZIE_URL);
            if (url == null) {
                throw new IllegalArgumentException(
                        "Oozie URL is not available neither in command option or in the environment");
            }
        }
        return url;
    }

    // Canibalized from Hadoop <code>Configuration.loadResource()</code>.
    private Properties parse(InputStream is, Properties conf) throws IOException {
        try {
            DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
            // ignore all comments inside the xml file
            docBuilderFactory.setIgnoringComments(true);
            DocumentBuilder builder = docBuilderFactory.newDocumentBuilder();
            Document doc = builder.parse(is);
            return parseDocument(doc, conf);
        }
        catch (SAXException e) {
            throw new IOException(e);
        }
        catch (ParserConfigurationException e) {
            throw new IOException(e);
        }
    }

    // Canibalized from Hadoop <code>Configuration.loadResource()</code>.
    private Properties parseDocument(Document doc, Properties conf) throws IOException {
        try {
            Element root = doc.getDocumentElement();
            if (!"configuration".equals(root.getTagName())) {
                throw new RuntimeException("bad conf file: top-level element not <configuration>");
            }
            NodeList props = root.getChildNodes();
            for (int i = 0; i < props.getLength(); i++) {
                Node propNode = props.item(i);
                if (!(propNode instanceof Element)) {
                    continue;
                }
                Element prop = (Element) propNode;
                if (!"property".equals(prop.getTagName())) {
                    throw new RuntimeException("bad conf file: element not <property>");
                }
                NodeList fields = prop.getChildNodes();
                String attr = null;
                String value = null;
                for (int j = 0; j < fields.getLength(); j++) {
                    Node fieldNode = fields.item(j);
                    if (!(fieldNode instanceof Element)) {
                        continue;
                    }
                    Element field = (Element) fieldNode;
                    if ("name".equals(field.getTagName()) && field.hasChildNodes()) {
                        attr = ((Text) field.getFirstChild()).getData();
                    }
                    if ("value".equals(field.getTagName()) && field.hasChildNodes()) {
                        value = ((Text) field.getFirstChild()).getData();
                    }
                }

                if (attr != null && value != null) {
                    conf.setProperty(attr, value);
                }
            }
            return conf;
        }
        catch (DOMException e) {
            throw new IOException(e);
        }
    }

    private Properties getConfiguration(CommandLine commandLine) throws IOException {
        Properties conf = new Properties();
        conf.setProperty("user.name", System.getProperty("user.name"));
        String configFile = commandLine.getOptionValue(CONFIG_OPTION);
        if (configFile != null) {
            File file = new File(configFile);
            if (!file.exists()) {
                throw new IOException("configuration file [" + configFile + "] not found");
            }
            if (configFile.endsWith(".properties")) {
                conf.load(new FileReader(file));
            }
            else if (configFile.endsWith(".xml")) {
                parse(new FileInputStream(configFile), conf);
            }
            else {
                throw new IllegalArgumentException("configuration must be a '.properties' or a '.xml' file");
            }
        }
        if (commandLine.hasOption("D")) {
            Properties commandLineProperties = commandLine.getOptionProperties("D");
            conf.putAll(commandLineProperties);
        }
        return conf;
    }

    /**
     * @param commandLine command line string.
     * @return change value specified by -value.
     * @throws OozieCLIException
     */
    private String getChangeValue(CommandLine commandLine) throws OozieCLIException {
        String changeValue = commandLine.getOptionValue(CHANGE_VALUE_OPTION);

        if (changeValue == null) {
            throw new OozieCLIException("-value option needs to be specified for -change option");
        }

        return changeValue;
    }

    private void addHeader(OozieClient wc) {
        for (Map.Entry entry : System.getProperties().entrySet()) {
            String key = (String) entry.getKey();
            if (key.startsWith(WS_HEADER_PREFIX)) {
                String header = key.substring(WS_HEADER_PREFIX.length());
                wc.setHeader(header, (String) entry.getValue());
            }
        }
    }

    /**
     * Create a OozieClient. <p/> It injects any '-Dheader:' as header to the the {@link
     * org.apache.oozie.client.OozieClient}.
     *
     * @param commandLine the parsed command line options.
     * @return a pre configured eXtended workflow client.
     * @throws OozieCLIException thrown if the OozieClient could not be
     *         configured.
     */
    protected OozieClient createOozieClient(CommandLine commandLine) throws OozieCLIException {
        OozieClient wc = new OozieClient(getOozieUrl(commandLine));
        addHeader(wc);
        return wc;
    }

    /**
     * Create a XOozieClient. <p/> It injects any '-Dheader:' as header to the the {@link
     * org.apache.oozie.client.OozieClient}.
     *
     * @param commandLine the parsed command line options.
     * @return a pre configured eXtended workflow client.
     * @throws OozieCLIException thrown if the XOozieClient could not be
     *         configured.
     */
    protected XOozieClient createXOozieClient(CommandLine commandLine) throws OozieCLIException {
        XOozieClient wc = new XOozieClient(getOozieUrl(commandLine));
        addHeader(wc);
        return wc;
    }

    private static String JOB_ID_PREFIX = "job: ";

    private void jobCommand(CommandLine commandLine) throws IOException, OozieCLIException {
        XOozieClient wc = createXOozieClient(commandLine);

        List<String> options = new ArrayList<String>();
        for (Option option : commandLine.getOptions()) {
            options.add(option.getOpt());
        }

        try {
            if (options.contains(SUBMIT_OPTION)) {
                System.out.println(JOB_ID_PREFIX + wc.submit(getConfiguration(commandLine)));
            }
            else if (options.contains(START_OPTION)) {
                wc.start(commandLine.getOptionValue(START_OPTION));
            }
            else if (options.contains(DRYRUN_OPTION)) {
                String[] dryrunStr = wc.dryrun(getConfiguration(commandLine)).split("action for new instance");
                int arraysize = dryrunStr.length;
                System.out.println("***coordJob after parsing: ***");
                System.out.println(dryrunStr[0]);
                int aLen = dryrunStr.length - 1;
                if (aLen < 0) {
                    aLen = 0;
                }
                System.out.println("***total coord actions is " + aLen + " ***");
                for (int i = 1; i <= arraysize - 1; i++) {
                    System.out.println(RULER);
                    System.out.println("coordAction instance: " + i + ":");
                    System.out.println(dryrunStr[i]);
                }
            }
            else if (options.contains(SUSPEND_OPTION)) {
                wc.suspend(commandLine.getOptionValue(SUSPEND_OPTION));
            }
            else if (options.contains(RESUME_OPTION)) {
                wc.resume(commandLine.getOptionValue(RESUME_OPTION));
            }
            else if (options.contains(KILL_OPTION)) {
                wc.kill(commandLine.getOptionValue(KILL_OPTION));
            }
            else if (options.contains(CHANGE_OPTION)) {
                wc.change(commandLine.getOptionValue(CHANGE_OPTION), getChangeValue(commandLine));
            }
            else if (options.contains(RUN_OPTION)) {
                System.out.println(JOB_ID_PREFIX + wc.run(getConfiguration(commandLine)));
            }
            else if (options.contains(RERUN_OPTION)) {
                if (commandLine.getOptionValue(RERUN_OPTION).contains("-W")) {
                    wc.reRun(commandLine.getOptionValue(RERUN_OPTION), getConfiguration(commandLine));
                } else {
                    String coordJobId = commandLine.getOptionValue(RERUN_OPTION);
                    String scope = null;
                    String rerunType = null;
                    boolean refresh = false;
                    boolean noCleanup = false;
                    if (options.contains(RERUN_DATE_OPTION) && options.contains(RERUN_ACTION_OPTION)) {
                        throw new OozieCLIException("Invalid options provided for rerun: either" + RERUN_DATE_OPTION
                                + " or " + RERUN_ACTION_OPTION + " expected. Don't use both at the same time.");
                    }
                    if (options.contains(RERUN_DATE_OPTION)) {
                        rerunType = RestConstants.JOB_COORD_RERUN_DATE;
                        scope = commandLine.getOptionValue(RERUN_DATE_OPTION);
                    } else if (options.contains(RERUN_ACTION_OPTION)){
                        rerunType = RestConstants.JOB_COORD_RERUN_ACTION;
                        scope = commandLine.getOptionValue(RERUN_ACTION_OPTION);
                    }
                    else {
                        throw new OozieCLIException("Invalid options provided for rerun: " + RERUN_DATE_OPTION + " or "
                                + RERUN_ACTION_OPTION + " expected.");
                    }
                    if (options.contains(RERUN_REFRESH_OPTION)) {
                        refresh = true;
                    }
                    if (options.contains(RERUN_NOCLEANUP_OPTION)) {
                        noCleanup = true;
                    }
                    printRerunCoordActions(wc.reRunCoord(coordJobId, rerunType, scope, refresh, noCleanup));
                }
            }
            else if (options.contains(INFO_OPTION)) {
                if (commandLine.getOptionValue(INFO_OPTION).endsWith("-C")) {
                    String s = commandLine.getOptionValue(OFFSET_OPTION);
                    int start = Integer.parseInt((s != null) ? s : "0");
                    s = commandLine.getOptionValue(LEN_OPTION);
                    int len = Integer.parseInt((s != null) ? s : "0");
                    printCoordJob(wc.getCoordJobInfo(commandLine.getOptionValue(INFO_OPTION), start, len), options
                            .contains(LOCAL_TIME_OPTION), options.contains(VERBOSE_OPTION));
                }
                else if (commandLine.getOptionValue(INFO_OPTION).contains("-C@")) {
                    printCoordAction(wc.getCoordActionInfo(commandLine.getOptionValue(INFO_OPTION)), options
                            .contains(LOCAL_TIME_OPTION));
                }
                else if (commandLine.getOptionValue(INFO_OPTION).contains("-W@")) {
                    printWorkflowAction(wc.getWorkflowActionInfo(commandLine.getOptionValue(INFO_OPTION)), options
                            .contains(LOCAL_TIME_OPTION));
                }
                else {
                    String s = commandLine.getOptionValue(OFFSET_OPTION);
                    int start = Integer.parseInt((s != null) ? s : "0");
                    s = commandLine.getOptionValue(LEN_OPTION);
                    String jobtype = commandLine.getOptionValue(JOBTYPE_OPTION);
                    jobtype = (jobtype != null) ? jobtype : "wf";
                    int len = Integer.parseInt((s != null) ? s : "0");
                    printJob(wc.getJobInfo(commandLine.getOptionValue(INFO_OPTION), start, len), options
                            .contains(LOCAL_TIME_OPTION), options.contains(VERBOSE_OPTION));
                }
            }
            else if (options.contains(LOG_OPTION)) {
                System.out.println(wc.getJobLog(commandLine.getOptionValue(LOG_OPTION)));
            }
            else if (options.contains(DEFINITION_OPTION)) {
                System.out.println(wc.getJobDefinition(commandLine.getOptionValue(DEFINITION_OPTION)));
            }
        }
        catch (OozieClientException ex) {
            throw new OozieCLIException(ex.toString(), ex);
        }
    }

    private void printCoordJob(CoordinatorJob coordJob, boolean localtime, boolean verbose) {
        System.out.println("Job ID : " + coordJob.getId());

        System.out.println(RULER);

        List<CoordinatorAction> actions = coordJob.getActions();
        System.out.println("Job Name : " + maskIfNull(coordJob.getAppName()));
        System.out.println("App Path : " + maskIfNull(coordJob.getAppPath()));
        System.out.println("Status   : " + coordJob.getStatus());
        System.out.println(RULER);

        if (verbose) {
            System.out.println("ID" + VERBOSE_DELIMITER + "Action Number" + VERBOSE_DELIMITER + "Console URL"
                    + VERBOSE_DELIMITER + "Error Code" + VERBOSE_DELIMITER + "Error Message" + VERBOSE_DELIMITER
                    + "External ID" + VERBOSE_DELIMITER + "External Status" + VERBOSE_DELIMITER + "Job ID"
                    + VERBOSE_DELIMITER + "Tracker URI" + VERBOSE_DELIMITER + "Created" + VERBOSE_DELIMITER
                    + "Nominal Time" + VERBOSE_DELIMITER + "Status" + VERBOSE_DELIMITER + "Last Modified" + VERBOSE_DELIMITER + "Missing Dependencies");
            System.out.println(RULER);

            for (CoordinatorAction action : actions) {
                System.out.println(maskIfNull(action.getId()) + VERBOSE_DELIMITER + action.getActionNumber()
                        + VERBOSE_DELIMITER + maskIfNull(action.getConsoleUrl()) + VERBOSE_DELIMITER
                        + maskIfNull(action.getErrorCode()) + VERBOSE_DELIMITER + maskIfNull(action.getErrorMessage())
                        + VERBOSE_DELIMITER + maskIfNull(action.getExternalId()) + VERBOSE_DELIMITER
                        + maskIfNull(action.getExternalStatus()) + VERBOSE_DELIMITER + maskIfNull(action.getJobId())
                        + VERBOSE_DELIMITER + maskIfNull(action.getTrackerUri()) + VERBOSE_DELIMITER
                        + maskDate(action.getCreatedTime(), localtime) + VERBOSE_DELIMITER +  maskDate(action.getNominalTime(), localtime)
                        + action.getStatus() + VERBOSE_DELIMITER + maskDate(action.getLastModifiedTime(), localtime) + VERBOSE_DELIMITER
                        + maskIfNull(action.getMissingDependencies()));

                System.out.println(RULER);
            }
        }
        else {
            System.out.println(String.format(COORD_ACTION_FORMATTER, "ID", "Status", "Ext ID", "Err Code", "Created",
                    "Nominal Time", "Last Mod"));

            for (CoordinatorAction action : actions) {
                System.out.println(String
                        .format(COORD_ACTION_FORMATTER, maskIfNull(action.getId()), action.getStatus(),
                                maskIfNull(action.getExternalId()), maskIfNull(action.getErrorCode()), maskDate(action
                                        .getCreatedTime(), localtime), maskDate(action.getNominalTime(), localtime),
                                maskDate(action.getLastModifiedTime(), localtime)));

                System.out.println(RULER);
            }
        }
    }

    private void printCoordAction(CoordinatorAction coordAction, boolean contains) {
        System.out.println("ID : " + maskIfNull(coordAction.getId()));

        System.out.println(RULER);

        System.out.println("Action Number        : " + coordAction.getActionNumber());
        System.out.println("Console URL          : " + maskIfNull(coordAction.getConsoleUrl()));
        System.out.println("Error Code           : " + maskIfNull(coordAction.getErrorCode()));
        System.out.println("Error Message        : " + maskIfNull(coordAction.getErrorMessage()));
        System.out.println("External ID          : " + maskIfNull(coordAction.getExternalId()));
        System.out.println("External Status      : " + maskIfNull(coordAction.getExternalStatus()));
        System.out.println("Job ID               : " + maskIfNull(coordAction.getJobId()));
        System.out.println("Tracker URI          : " + maskIfNull(coordAction.getTrackerUri()));
        System.out.println("Created              : " + maskDate(coordAction.getCreatedTime(), contains));
        System.out.println("Nominal Time         : " + maskDate(coordAction.getNominalTime(), contains));
        System.out.println("Status               : " + coordAction.getStatus());
        System.out.println("Last Modified        : " + maskDate(coordAction.getLastModifiedTime(), contains));
        System.out.println("Missing Dependencies : " + maskIfNull(coordAction.getMissingDependencies()));

        System.out.println(RULER);
    }

    private void printRerunCoordActions(List<JsonCoordinatorAction> actions) {
        if (actions != null && actions.size() > 0) {
            System.out.println("Action ID" + VERBOSE_DELIMITER + "Nominal Time");
            System.out.println(RULER);
            for (CoordinatorAction action : actions) {
                System.out.println(maskIfNull(action.getId()) + VERBOSE_DELIMITER
                        + maskDate(action.getNominalTime(), false));
            }
        }
        else {
            System.out.println("No Actions match your rerun criteria!");
        }
    }

    private void printWorkflowAction(WorkflowAction action, boolean contains) {
        System.out.println("ID : " + maskIfNull(action.getId()));

        System.out.println(RULER);

        System.out.println("Console URL     : " + maskIfNull(action.getConsoleUrl()));
        System.out.println("Error Code      : " + maskIfNull(action.getErrorCode()));
        System.out.println("Error Message   : " + maskIfNull(action.getErrorMessage()));
        System.out.println("External ID     : " + maskIfNull(action.getExternalId()));
        System.out.println("External Status : " + maskIfNull(action.getExternalStatus()));
        System.out.println("Name            : " + maskIfNull(action.getName()));
        System.out.println("Retries         : " + action.getRetries());
        System.out.println("Tracker URI     : " + maskIfNull(action.getTrackerUri()));
        System.out.println("Type            : " + maskIfNull(action.getType()));
        System.out.println("Started         : " + maskDate(action.getStartTime(), contains));
        System.out.println("Status          : " + action.getStatus());
        System.out.println("Ended           : " + maskDate(action.getEndTime(), contains));

        System.out.println(RULER);
    }

    private static final String WORKFLOW_JOBS_FORMATTER = "%-41s%-13s%-10s%-10s%-10s%-24s%-24s";
    private static final String COORD_JOBS_FORMATTER = "%-41s%-15s%-10s%-5s%-13s%-24s%-24s";

    private static final String WORKFLOW_ACTION_FORMATTER = "%-78s%-10s%-23s%-11s%-10s";
    private static final String COORD_ACTION_FORMATTER = "%-41s%-10s%-37s%-10s%-17s%-17s";

    private void printJob(WorkflowJob job, boolean localtime, boolean verbose) throws IOException {
        System.out.println("Job ID : " + maskIfNull(job.getId()));

        System.out.println(RULER);

        System.out.println("Workflow Name : " + maskIfNull(job.getAppName()));
        System.out.println("App Path      : " + maskIfNull(job.getAppPath()));
        System.out.println("Status        : " + job.getStatus());
        System.out.println("Run           : " + job.getRun());
        System.out.println("User          : " + maskIfNull(job.getUser()));
        System.out.println("Group         : " + maskIfNull(job.getGroup()));
        System.out.println("Created       : " + maskDate(job.getCreatedTime(), localtime));
        System.out.println("Started       : " + maskDate(job.getStartTime(), localtime));
        System.out.println("Last Modified : " + maskDate(job.getLastModifiedTime(), localtime));
        System.out.println("Ended         : " + maskDate(job.getEndTime(), localtime));

        List<WorkflowAction> actions = job.getActions();

        if (actions != null && actions.size() > 0) {
            System.out.println();
            System.out.println("Actions");
            System.out.println(RULER);

            if (verbose) {
                System.out.println("ID" + VERBOSE_DELIMITER + "Console URL" + VERBOSE_DELIMITER + "Error Code"
                        + VERBOSE_DELIMITER + "Error Message" + VERBOSE_DELIMITER + "External ID" + VERBOSE_DELIMITER
                        + "External Status" + VERBOSE_DELIMITER + "Name" + VERBOSE_DELIMITER + "Retries"
                        + VERBOSE_DELIMITER + "Tracker URI" + VERBOSE_DELIMITER + "Type" + VERBOSE_DELIMITER
                        + "Started" + VERBOSE_DELIMITER + "Status" + VERBOSE_DELIMITER + "Ended");
                System.out.println(RULER);

                for (WorkflowAction action : job.getActions()) {
                    System.out.println(maskIfNull(action.getId()) + VERBOSE_DELIMITER
                            + maskIfNull(action.getConsoleUrl()) + VERBOSE_DELIMITER
                            + maskIfNull(action.getErrorCode()) + VERBOSE_DELIMITER
                            + maskIfNull(action.getErrorMessage()) + VERBOSE_DELIMITER
                            + maskIfNull(action.getExternalId()) + VERBOSE_DELIMITER
                            + maskIfNull(action.getExternalStatus()) + VERBOSE_DELIMITER + maskIfNull(action.getName())
                            + VERBOSE_DELIMITER + action.getRetries() + VERBOSE_DELIMITER
                            + maskIfNull(action.getTrackerUri()) + VERBOSE_DELIMITER + maskIfNull(action.getType())
                            + VERBOSE_DELIMITER + maskDate(action.getStartTime(), localtime) + VERBOSE_DELIMITER
                            + action.getStatus() + VERBOSE_DELIMITER + maskDate(action.getEndTime(), localtime));

                    System.out.println(RULER);
                }
            }
            else {
                System.out.println(String.format(WORKFLOW_ACTION_FORMATTER, "ID", "Status", "Ext ID", "Ext Status",
                        "Err Code"));

                System.out.println(RULER);

                for (WorkflowAction action : job.getActions()) {
                    System.out.println(String.format(WORKFLOW_ACTION_FORMATTER, maskIfNull(action.getId()), action
                            .getStatus(), maskIfNull(action.getExternalId()), maskIfNull(action.getExternalStatus()),
                            maskIfNull(action.getErrorCode())));

                    System.out.println(RULER);
                }
            }
        }
        else {
            System.out.println(RULER);
        }

        System.out.println();
    }

    private void jobsCommand(CommandLine commandLine) throws IOException, OozieCLIException {
        XOozieClient wc = createXOozieClient(commandLine);

        String filter = commandLine.getOptionValue(FILTER_OPTION);
        String s = commandLine.getOptionValue(OFFSET_OPTION);
        int start = Integer.parseInt((s != null) ? s : "0");
        s = commandLine.getOptionValue(LEN_OPTION);
        String jobtype = commandLine.getOptionValue(JOBTYPE_OPTION);
        jobtype = (jobtype != null) ? jobtype : "wf";
        int len = Integer.parseInt((s != null) ? s : "0");
        try {
            if (jobtype.contains("wf")) {
                printJobs(wc.getJobsInfo(filter, start, len), commandLine.hasOption(LOCAL_TIME_OPTION), commandLine
                        .hasOption(VERBOSE_OPTION));
            }
            else {
                printCoordJobs(wc.getCoordJobsInfo(filter, start, len), commandLine.hasOption(LOCAL_TIME_OPTION),
                        commandLine.hasOption(VERBOSE_OPTION));
            }

        }
        catch (OozieClientException ex) {
            throw new OozieCLIException(ex.toString(), ex);
        }
    }

    private void printCoordJobs(List<CoordinatorJob> jobs, boolean localtime, boolean verbose) throws IOException {
        if (jobs != null && jobs.size() > 0) {
            if (verbose) {
                System.out.println("Job ID" + VERBOSE_DELIMITER + "App Name" + VERBOSE_DELIMITER + "App Path"
                        + VERBOSE_DELIMITER + "Console URL" + VERBOSE_DELIMITER + "User" + VERBOSE_DELIMITER + "Group"
                        + VERBOSE_DELIMITER + "Concurrency" + VERBOSE_DELIMITER + "Frequency" + VERBOSE_DELIMITER
                        + "Time Unit" + VERBOSE_DELIMITER + "Time Zone" + VERBOSE_DELIMITER + "Time Out"
                        + VERBOSE_DELIMITER + "Started" + VERBOSE_DELIMITER + "Next Materialize" + VERBOSE_DELIMITER
                        + "Status" + VERBOSE_DELIMITER + "Last Action" + VERBOSE_DELIMITER + "Ended");
                System.out.println(RULER);

                for (CoordinatorJob job : jobs) {
                    System.out.println(maskIfNull(job.getId()) + VERBOSE_DELIMITER + maskIfNull(job.getAppName())
                            + VERBOSE_DELIMITER + maskIfNull(job.getAppPath()) + VERBOSE_DELIMITER
                            + maskIfNull(job.getConsoleUrl()) + VERBOSE_DELIMITER + maskIfNull(job.getUser())
                            + VERBOSE_DELIMITER + maskIfNull(job.getGroup()) + VERBOSE_DELIMITER + job.getConcurrency()
                            + VERBOSE_DELIMITER + job.getFrequency() + VERBOSE_DELIMITER + job.getTimeUnit()
                            + VERBOSE_DELIMITER + maskIfNull(job.getTimeZone()) + VERBOSE_DELIMITER + job.getTimeout()
                            + VERBOSE_DELIMITER + maskDate(job.getStartTime(), localtime) + VERBOSE_DELIMITER
                            + maskDate(job.getNextMaterializedTime(), localtime) + VERBOSE_DELIMITER + job.getStatus()
                            + VERBOSE_DELIMITER + maskDate(job.getLastActionTime(), localtime) + VERBOSE_DELIMITER
                            + maskDate(job.getEndTime(), localtime));

                    System.out.println(RULER);
                }
            }
            else {
                System.out.println(String.format(COORD_JOBS_FORMATTER, "Job ID", "App Name", "Status", "Freq", "Unit",
                        "Started", "Next Materialized"));
                System.out.println(RULER);

                for (CoordinatorJob job : jobs) {
                    System.out.println(String.format(COORD_JOBS_FORMATTER, maskIfNull(job.getId()), maskIfNull(job
                            .getAppName()), job.getStatus(), job.getFrequency(), job.getTimeUnit(), maskDate(job
                            .getStartTime(), localtime), maskDate(job.getNextMaterializedTime(), localtime)));

                    System.out.println(RULER);
                }
            }
        }
        else {
            System.out.println("No Jobs match your criteria!");
        }
    }

    private void slaCommand(CommandLine commandLine) throws IOException, OozieCLIException {
        XOozieClient wc = createXOozieClient(commandLine);
        String s = commandLine.getOptionValue(OFFSET_OPTION);
        int start = Integer.parseInt((s != null) ? s : "0");
        s = commandLine.getOptionValue(LEN_OPTION);
        int len = Integer.parseInt((s != null) ? s : "100");
        try {
            wc.getSlaInfo(start, len);
        }
        catch (OozieClientException ex) {
            throw new OozieCLIException(ex.toString(), ex);
        }
    }

    private void adminCommand(CommandLine commandLine) throws OozieCLIException {
        XOozieClient wc = createXOozieClient(commandLine);

        List<String> options = new ArrayList<String>();
        for (Option option : commandLine.getOptions()) {
            options.add(option.getOpt());
        }

        try {
            SYSTEM_MODE status = SYSTEM_MODE.NORMAL;
            if (options.contains(VERSION_OPTION)) {
                System.out.println("Oozie server build version: " + wc.getServerBuildVersion());
            }
            else if (options.contains(SYSTEM_MODE_OPTION)) {
                    String systemModeOption = commandLine.getOptionValue(SYSTEM_MODE_OPTION).toUpperCase();
                    try {
                        status = SYSTEM_MODE.valueOf(systemModeOption);
                    }
                    catch (Exception e) {
                        throw new OozieCLIException("Invalid input provided for option: " + SYSTEM_MODE_OPTION
                                + " value given :" + systemModeOption
                                + " Expected values are: NORMAL/NOWEBSERVICE/SAFEMODE ");
                    }
                    wc.setSystemMode(status);
                    System.out.println("System mode: " + status);
            }
            else if (options.contains(STATUS_OPTION)) {
                    status = wc.getSystemMode();
                    System.out.println("System mode: " + status);
            }
            else if (options.contains(QUEUE_DUMP_OPTION)) {
                System.out.println("[Server Queue Dump]:");
                List<String> list = wc.getQueueDump();
                if (list != null && list.size() != 0) {
                    for (String str: list) {
                        System.out.println(str);
                    }
                }
                else {
                    System.out.println("QueueDump is null!");
                }
            }
        }
        catch (OozieClientException ex) {
            throw new OozieCLIException(ex.toString(), ex);
        }
    }

    private void versionCommand() throws OozieCLIException {
        System.out.println("Oozie client build version: "
                + BuildInfo.getBuildInfo().getProperty(BuildInfo.BUILD_VERSION));
    }

    private void printJobs(List<WorkflowJob> jobs, boolean localtime, boolean verbose) throws IOException {
        if (jobs != null && jobs.size() > 0) {
            if (verbose) {
                System.out.println("Job ID" + VERBOSE_DELIMITER + "App Name" + VERBOSE_DELIMITER + "App Path"
                        + VERBOSE_DELIMITER + "Console URL" + VERBOSE_DELIMITER + "User" + VERBOSE_DELIMITER + "Group"
                        + VERBOSE_DELIMITER + "Run" + VERBOSE_DELIMITER + "Created" + VERBOSE_DELIMITER + "Started"
                        + VERBOSE_DELIMITER + "Status" + VERBOSE_DELIMITER + "Last Modified" + VERBOSE_DELIMITER
                        + "Ended");
                System.out.println(RULER);

                for (WorkflowJob job : jobs) {
                    System.out.println(maskIfNull(job.getId()) + VERBOSE_DELIMITER + maskIfNull(job.getAppName())
                            + VERBOSE_DELIMITER + maskIfNull(job.getAppPath()) + VERBOSE_DELIMITER
                            + maskIfNull(job.getConsoleUrl()) + VERBOSE_DELIMITER + maskIfNull(job.getUser())
                            + VERBOSE_DELIMITER + maskIfNull(job.getGroup()) + VERBOSE_DELIMITER + job.getRun()
                            + VERBOSE_DELIMITER + maskDate(job.getCreatedTime(), localtime) + VERBOSE_DELIMITER
                            + maskDate(job.getStartTime(), localtime) + VERBOSE_DELIMITER + job.getStatus()
                            + VERBOSE_DELIMITER + maskDate(job.getLastModifiedTime(), localtime) + VERBOSE_DELIMITER
                            + maskDate(job.getEndTime(), localtime));

                    System.out.println(RULER);
                }
            }
            else {
                System.out.println(String.format(WORKFLOW_JOBS_FORMATTER, "Job ID", "App Name", "Status", "User",
                        "Group", "Started", "Ended"));
                System.out.println(RULER);

                for (WorkflowJob job : jobs) {
                    System.out.println(String.format(WORKFLOW_JOBS_FORMATTER, maskIfNull(job.getId()), maskIfNull(job
                            .getAppName()), job.getStatus(), maskIfNull(job.getUser()), maskIfNull(job.getGroup()),
                            maskDate(job.getStartTime(), localtime), maskDate(job.getEndTime(), localtime)));

                    System.out.println(RULER);
                }
            }
        }
        else {
            System.out.println("No Jobs match your criteria!");
        }
    }

    private String maskIfNull(String value) {
        if (value != null && value.length() > 0) {
            return value;
        }
        return "-";
    }

    private String maskDate(Date date, boolean isLocalTimeZone) {
        if (date == null) {
            return "-";
        }

        // SimpleDateFormat dateFormater = new SimpleDateFormat("yyyy-MM-dd
        // HH:mm Z", Locale.US);
        SimpleDateFormat dateFormater = new SimpleDateFormat("yyyy-MM-dd HH:mm", Locale.US);
        if (!isLocalTimeZone) {
            dateFormater.setTimeZone(TimeZone.getTimeZone("GMT"));
        }
        return dateFormater.format(date);
    }

    private void validateCommand(CommandLine commandLine) throws OozieCLIException {
        String[] args = commandLine.getArgs();
        if (args.length != 1) {
            throw new OozieCLIException("One file must be specified");
        }
        File file = new File(args[0]);
        if (file.exists()) {
            try {
                List<StreamSource> sources = new ArrayList<StreamSource>();
                sources.add(new StreamSource(Thread.currentThread().getContextClassLoader().getResourceAsStream(
                        "oozie-workflow-0.1.xsd")));
                SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
                Schema schema = factory.newSchema(sources.toArray(new StreamSource[sources.size()]));
                Validator validator = schema.newValidator();
                validator.validate(new StreamSource(new FileReader(file)));
                System.out.println("Valid worflow-app");
            }
            catch (Exception ex) {
                throw new OozieCLIException("Invalid workflow-app, " + ex.toString(), ex);
            }
        }
        else {
            throw new OozieCLIException("File does not exists");
        }
    }

    private void pigCommand(CommandLine commandLine) throws IOException, OozieCLIException {
        List<String> pigArgs = (List<String>) commandLine.getArgList();
        if (pigArgs.size() > 0) {
            // checking is a pigArgs starts with -X (because CLIParser cannot check this)
            if (!pigArgs.get(0).equals("-X")) {
                throw new OozieCLIException("Unrecognized option: " + pigArgs.get(0) + " Expecting -X");
            }
            pigArgs.remove(0);
        }

        List<String> options = new ArrayList<String>();
        for (Option option : commandLine.getOptions()) {
            options.add(option.getOpt());
        }

        if (!options.contains(PIGFILE_OPTION)) {
            throw new OozieCLIException("Need to specify -file <scriptfile>");
        }

        Properties conf = getConfiguration(commandLine);
        String script = commandLine.getOptionValue(PIGFILE_OPTION);

        try {
            XOozieClient wc = createXOozieClient(commandLine);
            System.out.println(JOB_ID_PREFIX + wc.submitPig(conf, script, pigArgs.toArray(new String[pigArgs.size()])));
        }
        catch (OozieClientException ex) {
            throw new OozieCLIException(ex.toString(), ex);
        }
    }
}
