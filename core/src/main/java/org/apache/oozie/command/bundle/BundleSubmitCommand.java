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
package org.apache.oozie.command.bundle;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Validator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.BundleJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.bundle.BundleJobException;
import org.apache.oozie.client.BundleJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.coord.CoordUtils;
import org.apache.oozie.service.DagXLogInfoService;
import org.apache.oozie.service.HadoopAccessorException;
import org.apache.oozie.service.SchemaService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.UUIDService;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.WorkflowAppService;
import org.apache.oozie.service.SchemaService.SchemaName;
import org.apache.oozie.service.UUIDService.ApplicationType;
import org.apache.oozie.store.BundleStore;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.PropertiesUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;
import org.xml.sax.SAXException;

/**
 * This class provides the functionalities to resolve a bundle job XML and write the job information into a DB table.
 */
public class BundleSubmitCommand extends BundleCommand<String> {

    private Configuration conf;
    private String authToken;

    public static final String CONFIG_DEFAULT = "bundle-config-default.xml";
    public static final String BUNDLE_XML_FILE = "bundle.xml";

    private static final Set<String> DISALLOWED_USER_PROPERTIES = new HashSet<String>();
    private static final Set<String> DISALLOWED_DEFAULT_PROPERTIES = new HashSet<String>();

    private XLog log = XLog.getLog(getClass());

    static {
        String[] badUserProps = { PropertiesUtils.YEAR, PropertiesUtils.MONTH, PropertiesUtils.DAY,
                PropertiesUtils.HOUR, PropertiesUtils.MINUTE, PropertiesUtils.DAYS, PropertiesUtils.HOURS,
                PropertiesUtils.MINUTES, PropertiesUtils.KB, PropertiesUtils.MB, PropertiesUtils.GB,
                PropertiesUtils.TB, PropertiesUtils.PB, PropertiesUtils.RECORDS, PropertiesUtils.MAP_IN,
                PropertiesUtils.MAP_OUT, PropertiesUtils.REDUCE_IN, PropertiesUtils.REDUCE_OUT, PropertiesUtils.GROUPS };
        PropertiesUtils.createPropertySet(badUserProps, DISALLOWED_USER_PROPERTIES);

        String[] badDefaultProps = { PropertiesUtils.HADOOP_USER, PropertiesUtils.HADOOP_UGI,
                WorkflowAppService.HADOOP_JT_KERBEROS_NAME, WorkflowAppService.HADOOP_NN_KERBEROS_NAME };
        PropertiesUtils.createPropertySet(badUserProps, DISALLOWED_DEFAULT_PROPERTIES);
        PropertiesUtils.createPropertySet(badDefaultProps, DISALLOWED_DEFAULT_PROPERTIES);
    }

    /**
     * Constructor to create the Bundle Submit Command.
     *
     * @param conf Configuration for Bundle job
     * @param authToken To be used for authentication
     */
    public BundleSubmitCommand(Configuration conf, String authToken) {
        super("bundle_submit", "bundle_submit", 1, XLog.STD);
        this.conf = ParamChecker.notNull(conf, "conf");
        this.authToken = ParamChecker.notEmpty(authToken, "authToken");
    }

    /**
     * Constructor to create the Bundle Submit Command.
     *
     * @param dryrun If dryrun
     * @param conf Configuration for Bundle job
     * @param authToken To be used for authentication
     */
    public BundleSubmitCommand(boolean dryrun, Configuration conf, String authToken) {
        super("bundle_submit", "bundle_submit", 1, XLog.STD, dryrun);
        this.conf = ParamChecker.notNull(conf, "conf");
        this.authToken = ParamChecker.notEmpty(authToken, "authToken");
        this.dryrun = dryrun;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.oozie.command.Command#call(org.apache.oozie.store.Store)
     */
    @Override
    protected String call(BundleStore store) throws StoreException, CommandException {
        String jobId = null;
        log.info("STARTED Bundle Submit");
        incrJobCounter(1);
        BundleJobBean bundleJob = new BundleJobBean();
        try {
            XLog.Info.get().setParameter(DagXLogInfoService.TOKEN, conf.get(OozieClient.LOG_TOKEN));
            mergeDefaultConfig();

            String appXmlStr = readAndValidateXml();
            bundleJob.setOrigJobXml(appXmlStr);
            log.debug("jobXml after initial validation " + XmlUtils.prettyPrint(appXmlStr).toString());
            appXmlStr = XmlUtils.removeComments(appXmlStr);
            Element appXml = XmlUtils.parseXml(appXmlStr);

            jobId = storeToDB(appXml, store, bundleJob);
            // log JOB info for bundle job
            setLogInfo(bundleJob);
            log = XLog.getLog(getClass());
        }
        catch (BundleJobException ex) {
            log.warn("ERROR:  ", ex);
            throw new CommandException(ex);
        }
        catch (IllegalArgumentException iex) {
            log.warn("ERROR:  ", iex);
            throw new CommandException(ErrorCode.E1103, iex);
        }
        catch (Exception ex) {
            log.warn("ERROR:  ", ex);
            throw new CommandException(ErrorCode.E0803, ex);
        }
        log.info("ENDED Bundle Submit jobId=" + jobId);
        return jobId;
    }

    /**
     * Read the application XML and validate against bundle Schema
     *
     * @return validated bundle XML
     * @throws BundleJobException
     */
    private String readAndValidateXml() throws BundleJobException {
        String appPath = ParamChecker.notEmpty(conf.get(OozieClient.BUNDLE_APP_PATH), OozieClient.BUNDLE_APP_PATH);
        String bundleXml = readDefinition(appPath, BUNDLE_XML_FILE);
        validateXml(bundleXml);
        return bundleXml;
    }

    /**
     * Validate against Bundle XSD file
     *
     * @param xmlContent : Bundle xml
     * @throws BundleJobException
     */
    private void validateXml(String xmlContent) throws BundleJobException {
        javax.xml.validation.Schema schema = Services.get().get(SchemaService.class).getSchema(SchemaName.BUNDLE);
        Validator validator = schema.newValidator();
        log.debug("XML " + xmlContent);
        try {
            validator.validate(new StreamSource(new StringReader(xmlContent)));
        }
        catch (SAXException ex) {
            log.error("SAXException :", ex);
            throw new BundleJobException(ErrorCode.E0701, ex.getMessage(), ex);
        }
        catch (IOException ex) {
            log.error("IOException :", ex);
            throw new BundleJobException(ErrorCode.E0702, ex.getMessage(), ex);
        }
    }

    /**
     * Merge default configuration with user-defined configuration.
     *
     * @throws CommandException
     */
    protected void mergeDefaultConfig() throws CommandException {
        Path configDefault = new Path(conf.get(OozieClient.BUNDLE_APP_PATH), CONFIG_DEFAULT);
        try {
            String user = ParamChecker.notEmpty(conf.get(OozieClient.USER_NAME), OozieClient.USER_NAME);
            String group = ParamChecker.notEmpty(conf.get(OozieClient.GROUP_NAME), OozieClient.GROUP_NAME);
            FileSystem fs = Services.get().get(HadoopAccessorService.class).createFileSystem(user, group,
                    configDefault.toUri(), new Configuration());
            if (fs.exists(configDefault)) {
                Configuration defaultConf = new XConfiguration(fs.open(configDefault));
                PropertiesUtils.checkDisallowedProperties(defaultConf, DISALLOWED_DEFAULT_PROPERTIES);
                XConfiguration.injectDefaults(defaultConf, conf);
            }
            else {
                log.info("configDefault Doesn't exist " + configDefault);
            }
            PropertiesUtils.checkDisallowedProperties(conf, DISALLOWED_USER_PROPERTIES);
        }
        catch (IOException e) {
            throw new CommandException(ErrorCode.E0702, e.getMessage() + " : Problem reading default config "
                    + configDefault, e);
        }
        catch (HadoopAccessorException e) {
            throw new CommandException(e);
        }
        log.debug("Merged Bundle CONF :" + XmlUtils.prettyPrint(conf).toString());
    }

    /**
     * Read Bundle definition.
     *
     * @param appPath application path.
     * @return Bundle definition.
     * @throws BundleJobException thrown if the definition could not be read.
     */
    protected String readDefinition(String appPath, String fileName) throws BundleJobException {
        String user = ParamChecker.notEmpty(conf.get(OozieClient.USER_NAME), OozieClient.USER_NAME);
        String group = ParamChecker.notEmpty(conf.get(OozieClient.GROUP_NAME), OozieClient.GROUP_NAME);
        Configuration confHadoop = CoordUtils.getHadoopConf(conf);
        try {
            URI uri = new URI(appPath);
            log.debug("user =" + user + " group =" + group);
            FileSystem fs = Services.get().get(HadoopAccessorService.class).createFileSystem(user, group, uri,
                    new Configuration());
            //Path p = new Path(uri.getPath());
            Path p;
            if (fileName == null || fileName.length() == 0) {
                p = new Path(uri.getPath());
            }
            else {
                p = new Path(uri.getPath(), fileName);
            }

            Reader reader = new InputStreamReader(fs.open(p));
            StringWriter writer = new StringWriter();
            IOUtils.copyCharStream(reader, writer);
            return writer.toString();
        }
        catch (IOException ioex) {
            log.warn("IOException :" + XmlUtils.prettyPrint(confHadoop), ioex);
            throw new BundleJobException(ErrorCode.E1101, ioex.getMessage(), ioex);
        }
        catch (URISyntaxException uex) {
            log.warn("URISyException :" + uex.getMessage());
            throw new BundleJobException(ErrorCode.E1102, appPath, uex.getMessage(), uex);
        }
        catch (HadoopAccessorException hex) {
            throw new BundleJobException(hex);
        }
        catch (Exception ex) {
            log.warn("Exception :", ex);
            throw new BundleJobException(ErrorCode.E1101, ex.getMessage(), ex);
        }
    }

    /**
     * Write a Bundle Job into database
     *
     * @param bJob : XML element of job
     * @param store : Bundle Store to write.
     * @param bundleJob : Bundle job bean
     * @return Job id.
     * @throws Exception
     */
    private String storeToDB(Element jobXml, BundleStore store, BundleJobBean bundleJob) throws Exception {
        String jobId = Services.get().get(UUIDService.class).generateId(ApplicationType.BUNDLE);
        bundleJob.setId(jobId);
        bundleJob.setAuthToken(this.authToken);
        bundleJob.setBundleName(jobXml.getAttributeValue("name"));
        bundleJob.setCreatedTime(new Date());
        bundleJob.setBundlePath(conf.get(OozieClient.BUNDLE_APP_PATH));
        bundleJob.setStatus(BundleJob.Status.PREP);
        bundleJob.setUser(conf.get(OozieClient.USER_NAME));
        bundleJob.setGroup(conf.get(OozieClient.GROUP_NAME));
        bundleJob.setConf(XmlUtils.prettyPrint(conf).toString());
        bundleJob.setJobXml(XmlUtils.prettyPrint(jobXml).toString());

        Element controls = jobXml.getChild("controls", jobXml.getNamespace());
        if (controls != null) {
            Element kickOffElem = controls.getChild("kick-off-time", jobXml.getNamespace());
            if (kickOffElem != null) {
                String kickOffVal = kickOffElem.getValue();
                if (kickOffVal != null) {
                    bundleJob.setKickoffTime(DateUtils.parseDateUTC(kickOffVal));
                }
            }
        }

        if (!dryrun) {
            store.insertBundleJob(bundleJob);
        }
        return jobId;
    }

}
