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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.oozie.BuildInfo;
import org.apache.oozie.client.AuthOozieClient;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClient.SYSTEM_MODE;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.XOozieClient;
import org.apache.oozie.client.rest.JsonCoordinatorAction;
import org.apache.oozie.client.rest.RestConstants;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
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

/**
 * Oozie command line utility.
 */
public class AuthOozieCLI extends OozieCLI {
    /**
     * Entry point for the Oozie CLI when invoked from the command line.
     * <p/>
     * Upon completion this method exits the JVM with '0' (success) or '-1'
     * (failure).
     *
     * @param args options and arguments for the Oozie CLI.
     */
    public static void main(String[] args) {
        if (!System.getProperties().contains(AuthOozieClient.USE_AUTH_TOKEN_CACHE_SYS_PROP)) {
            System.setProperty(AuthOozieClient.USE_AUTH_TOKEN_CACHE_SYS_PROP, "true");
        }
        System.exit(new AuthOozieCLI().run(args));
    }

    /**
     * Create a OozieClient. <p/> It injects any '-Dheader:' as header to the the {@link
     * OozieClient}.
     *
     * @param commandLine the parsed command line options.
     * @return a pre configured eXtended workflow client.
     * @throws OozieCLIException thrown if the OozieClient could not be
     *         configured.
     */
    @Override
    protected OozieClient createOozieClient(CommandLine commandLine) throws OozieCLIException {
        return createXOozieClient(commandLine);
    }

    //TODO: This method should be made protected in OozieCLI so we don't have to reimplement it here
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
     * Create a XOozieClient. <p/> It injects any '-Dheader:' as header to the the {@link
     * OozieClient}.
     *
     * @param commandLine the parsed command line options.
     * @return a pre configured eXtended workflow client.
     * @throws OozieCLIException thrown if the XOozieClient could not be
     *         configured.
     */
    @Override
    protected XOozieClient createXOozieClient(CommandLine commandLine) throws OozieCLIException {
        XOozieClient wc = new AuthOozieClient(getOozieUrl(commandLine));
        addHeader(wc);
        return wc;
    }

}
