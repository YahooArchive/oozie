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
     * @param conf : Configuration for Bundle job
     * @param authToken : To be used for authentication
     */
    public BundleSubmitCommand(Configuration conf, String authToken) {
        super("bundle_submit", "bundle_submit", 1, XLog.STD);
        this.conf = ParamChecker.notNull(conf, "conf");
        this.authToken = ParamChecker.notEmpty(authToken, "authToken");
    }

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
        log.warn("XML " + xmlContent);
        try {
            validator.validate(new StreamSource(new StringReader(xmlContent)));
        }
        catch (SAXException ex) {
            log.warn("SAXException :", ex);
            throw new BundleJobException(ErrorCode.E0701, ex.getMessage(), ex);
        }
        catch (IOException ex) {
            log.warn("IOException :", ex);
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
     * Resolve basic entities using job Configuration.
     *
     * @param conf :Job configuration
     * @param appXml : Original job XML
     * @param coordJob : Coordinator job bean to be populated.
     * @return Resolved job XML element.
     * @throws Exception
     */
    /*protected Element resolveInitial(Configuration conf, String appXml, CoordinatorJobBean coordJob)
            throws CoordinatorJobException, Exception {
        Element eAppXml = XmlUtils.parseXml(appXml);
        // job's main attributes
        // frequency
        String val = resolveAttribute("frequency", eAppXml, evalFreq);
        int ival = ParamChecker.checkInteger(val, "frequency");
        ParamChecker.checkGTZero(ival, "frequency");
        coordJob.setFrequency(ival);
        TimeUnit tmp = (evalFreq.getVariable("timeunit") == null) ? TimeUnit.MINUTE : ((TimeUnit) evalFreq
                .getVariable("timeunit"));
        addAnAttribute("freq_timeunit", eAppXml, tmp.toString()); // TODO: Store
        // TimeUnit
        coordJob.setTimeUnit(CoordinatorJob.Timeunit.valueOf(tmp.toString()));
        // End Of Duration
        tmp = evalFreq.getVariable("endOfDuration") == null ? TimeUnit.NONE : ((TimeUnit) evalFreq
                .getVariable("endOfDuration"));
        addAnAttribute("end_of_duration", eAppXml, tmp.toString());
        // coordJob.setEndOfDuration(tmp) // TODO: Add new attribute in Job bean

        // start time
        val = resolveAttribute("start", eAppXml, evalNofuncs);
        ParamChecker.checkUTC(val, "start");
        coordJob.setStartTime(DateUtils.parseDateUTC(val));
        // end time
        val = resolveAttribute("end", eAppXml, evalNofuncs);
        ParamChecker.checkUTC(val, "end");
        coordJob.setEndTime(DateUtils.parseDateUTC(val));
        // Time zone
        val = resolveAttribute("timezone", eAppXml, evalNofuncs);
        ParamChecker.checkTimeZone(val, "timezone");
        coordJob.setTimeZone(val);

        // controls
        val = resolveTagContents("timeout", eAppXml.getChild("controls", eAppXml.getNamespace()), evalNofuncs);
        if (val == "") {
            val = Services.get().getConf().get(CONF_DEFAULT_TIMEOUT_NORMAL);
        }

        ival = ParamChecker.checkInteger(val, "timeout");
        // ParamChecker.checkGEZero(ival, "timeout");
        coordJob.setTimeout(ival);
        val = resolveTagContents("concurrency", eAppXml.getChild("controls", eAppXml.getNamespace()), evalNofuncs);
        if (val == "") {
            val = "-1";
        }
        ival = ParamChecker.checkInteger(val, "concurrency");
        // ParamChecker.checkGEZero(ival, "concurrency");
        coordJob.setConcurrency(ival);
        val = resolveTagContents("execution", eAppXml.getChild("controls", eAppXml.getNamespace()), evalNofuncs);
        if (val == "") {
            val = Execution.FIFO.toString();
        }
        coordJob.setExecution(Execution.valueOf(val));
        String[] acceptedVals = { Execution.LIFO.toString(), Execution.FIFO.toString(), Execution.LAST_ONLY.toString() };
        ParamChecker.isMember(val, acceptedVals, "execution");

        // datasets
        resolveTagContents("include", eAppXml.getChild("datasets", eAppXml.getNamespace()), evalNofuncs);
        // for each data set
        resolveDataSets(eAppXml);
        HashMap<String, String> dataNameList = new HashMap<String, String>();
        resolveIOEvents(eAppXml, dataNameList);

        resolveTagContents("app-path", eAppXml.getChild("action", eAppXml.getNamespace()).getChild("workflow",
                eAppXml.getNamespace()), evalNofuncs);
        // TODO: If action or workflow tag is missing, NullPointerException will
        // occur
        Element configElem = eAppXml.getChild("action", eAppXml.getNamespace()).getChild("workflow",
                eAppXml.getNamespace()).getChild("configuration", eAppXml.getNamespace());
        evalData = CoordELEvaluator.createELEvaluatorForDataEcho(conf, "coord-job-submit-data", dataNameList);
        if (configElem != null) {
            for (Element propElem : (List<Element>) configElem.getChildren("property", configElem.getNamespace())) {
                resolveTagContents("name", propElem, evalData);
                // log.warn("Value :");
                // Want to check the data-integrity but don't want to modify the
                // XML
                // for properties only
                Element tmpProp = (Element) propElem.clone();
                resolveTagContents("value", tmpProp, evalData);
                // val = resolveTagContents("value", propElem, evalData);
                // log.warn("Value OK :" + val);
            }
        }
        resolveSLA(eAppXml, coordJob);
        return eAppXml;
    }

    private void resolveSLA(Element eAppXml, CoordinatorJobBean coordJob) throws CommandException {
        // String prefix = XmlUtils.getNamespacePrefix(eAppXml,
        // SchemaService.SLA_NAME_SPACE_URI);
        Element eSla = eAppXml.getChild("action", eAppXml.getNamespace()).getChild("info",
                Namespace.getNamespace(SchemaService.SLA_NAME_SPACE_URI));

        if (eSla != null) {
            String slaXml = XmlUtils.prettyPrint(eSla).toString();
            try {
                // EL evaluation
                slaXml = evalSla.evaluate(slaXml, String.class);
                // Validate against semantic SXD
                XmlUtils.validateData(slaXml, SchemaName.SLA_ORIGINAL);
            }
            catch (Exception e) {
                throw new CommandException(ErrorCode.E1004, "Validation ERROR :" + e.getMessage(), e);
            }
        }
    }
*/

    /**
     * Add an attribute into XML element.
     *
     * @param attrName :attribute name
     * @param elem : Element to add attribute
     * @param value :Value of attribute
     */
/*    private void addAnAttribute(String attrName, Element elem, String value) {
        elem.setAttribute(attrName, value);
    }*/


    /**
     * Resolve the content of a tag.
     *
     * @param tagName : Tag name of job XML i.e. <timeout> 10 </timeout>
     * @param elem : Element where the tag exists.
     * @param eval :
     * @return Resolved tag content.
     * @throws CoordinatorJobException
     */
/*    private String resolveTagContents(String tagName, Element elem, ELEvaluator eval) throws CoordinatorJobException {
        String ret = "";
        if (elem != null) {
            for (Element tagElem : (List<Element>) elem.getChildren(tagName, elem.getNamespace())) {
                if (tagElem != null) {
                    String updated;
                    try {
                        updated = CoordELFunctions.evalAndWrap(eval, tagElem.getText().trim());

                    }
                    catch (Exception e) {
                        // e.printStackTrace();
                        throw new CoordinatorJobException(ErrorCode.E1004, e.getMessage(), e);
                    }
                    tagElem.removeContent();
                    tagElem.addContent(updated);
                    ret += updated;
                }

                * else { //TODO: unlike event }

            }
        }
        return ret;
    }
*/
    /**
     * Resolve an attribute value.
     *
     * @param attrName : Attribute name.
     * @param elem : XML Element where attribute is defiend
     * @param eval : ELEvaluator used to resolve
     * @return Resolved attribute value
     * @throws CoordinatorJobException
     */
/*    private String resolveAttribute(String attrName, Element elem, ELEvaluator eval) throws CoordinatorJobException {
        Attribute attr = elem.getAttribute(attrName);
        String val = null;
        if (attr != null) {
            try {
                val = CoordELFunctions.evalAndWrap(eval, attr.getValue().trim());

            }
            catch (Exception e) {
                // e.printStackTrace();
                throw new CoordinatorJobException(ErrorCode.E1004, e.getMessage(), e);
            }
            attr.setValue(val);
        }
        return val;
    }*/

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
     * @throws StoreException
     */
    private String storeToDB(Element jobXml, BundleStore store, BundleJobBean bundleJob) throws StoreException {
        String jobId = Services.get().get(UUIDService.class).generateId(ApplicationType.BUNDLE);
        bundleJob.setId(jobId);
        bundleJob.setAuthToken(this.authToken);
        bundleJob.setBundleName(jobXml.getAttributeValue("name"));
        bundleJob.setBundlePath(conf.get(OozieClient.BUNDLE_APP_PATH));
        bundleJob.setStatus(BundleJob.Status.PREP);
        bundleJob.setUser(conf.get(OozieClient.USER_NAME));
        bundleJob.setGroup(conf.get(OozieClient.GROUP_NAME));
        bundleJob.setConf(XmlUtils.prettyPrint(conf).toString());
        bundleJob.setJobXml(XmlUtils.prettyPrint(jobXml).toString());

        if (!dryrun) {
            store.insertBundleJob(bundleJob);
        }
        return jobId;
    }

}
