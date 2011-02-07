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

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.service.WorkflowAppService;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;
import org.jdom.Namespace;
import org.apache.oozie.client.XOozieClient;
import org.apache.oozie.command.CommandException;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class SubmitMRXCommand extends SubmitHttpXCommand {
    private static final Set<String> SKIPPED_CONFS = new HashSet<String>();

    public SubmitMRXCommand(Configuration conf, String authToken) {
        super("submitMR", "submitMR", conf, authToken);
    }

    static {
        SKIPPED_CONFS.add(WorkflowAppService.HADOOP_USER);
        SKIPPED_CONFS.add(WorkflowAppService.HADOOP_UGI);
        SKIPPED_CONFS.add(XOozieClient.JT);
        SKIPPED_CONFS.add(XOozieClient.NN);
        SKIPPED_CONFS.add(WorkflowAppService.HADOOP_JT_KERBEROS_NAME);
        SKIPPED_CONFS.add(WorkflowAppService.HADOOP_NN_KERBEROS_NAME);
    }

    private Element generateConfigurationSection(Configuration conf, Namespace ns) {
        Element configuration = null;
        Iterator<Map.Entry<String, String>> iter = conf.iterator();
        while (iter.hasNext()) {
            Map.Entry<String, String> entry = iter.next();
            String name = entry.getKey();
            if (MANDATORY_OOZIE_CONFS.contains(name) || OPTIONAL_OOZIE_CONFS.contains(name)
                    || SKIPPED_CONFS.contains(name)) {
                continue;
            }

            if (configuration == null) {
                configuration = new Element("configuration", ns);
            }

            String value = entry.getValue();
            Element property = new Element("property", ns);
            Element nameElement = new Element("name", ns);
            nameElement.addContent(name != null ? name : "");
            property.addContent(nameElement);
            Element valueElement = new Element("value", ns);
            valueElement.addContent(value != null ? value : "");
            property.addContent(valueElement);
            configuration.addContent(property);
        }

        return configuration;
    }

    private Element generateMRSection(Configuration conf, Namespace ns) {
        Element mapreduce = new Element("map-reduce", ns);
        Element jt = new Element("job-tracker", ns);
        jt.addContent(conf.get(XOozieClient.JT));
        mapreduce.addContent(jt);
        Element nn = new Element("name-node", ns);
        nn.addContent(conf.get(XOozieClient.NN));
        mapreduce.addContent(nn);

        if (conf.size() > MANDATORY_OOZIE_CONFS.size()) { // excluding JT, NN,
                                                          // LIBPATH
            // configuration section
            Element configuration = generateConfigurationSection(conf, ns);
            if (configuration != null) {
                mapreduce.addContent(configuration);
            }

            // file section
            addFileSection(mapreduce, conf, ns);

            // archive section
            addArchiveSection(mapreduce, conf, ns);
        }

        return mapreduce;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.apache.oozie.command.wf.SubmitHttpCommand#getWorkflowXml(org.apache
     * .hadoop.conf.Configuration)
     */
    @Override
    protected String getWorkflowXml(Configuration conf) {
        for (String key : MANDATORY_OOZIE_CONFS) {
            String value = conf.get(key);
            if (value == null) {
                throw new RuntimeException(key + " is not specified");
            }
        }

        Namespace ns = Namespace.getNamespace("uri:oozie:workflow:0.2");
        Element root = new Element("workflow-app", ns);
        root.setAttribute("name", "oozie-mapreduce");

        Element start = new Element("start", ns);
        start.setAttribute("to", "hadoop1");
        root.addContent(start);

        Element action = new Element("action", ns);
        action.setAttribute("name", "hadoop1");

        Element mapreduce = generateMRSection(conf, ns);
        action.addContent(mapreduce);

        Element ok = new Element("ok", ns);
        ok.setAttribute("to", "end");
        action.addContent(ok);

        Element error = new Element("error", ns);
        error.setAttribute("to", "fail");
        action.addContent(error);

        root.addContent(action);

        Element kill = new Element("kill", ns);
        kill.setAttribute("name", "fail");
        Element message = new Element("message", ns);
        message.addContent("Map/Reduce failed, error message[${wf:errorMessage(wf:lastErrorNode())}]");
        kill.addContent(message);
        root.addContent(kill);

        Element end = new Element("end", ns);
        end.setAttribute("name", "end");
        root.addContent(end);

        return XmlUtils.prettyPrint(root).toString();
    }

    @Override
    protected String getEntityKey() {
        return null;
    }

    @Override
    protected boolean isLockRequired() {
        return false;
    }

    @Override
    protected void loadState() {

    }

    @Override
    protected void verifyPrecondition() throws CommandException {

    }
}