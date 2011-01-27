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
package org.apache.hadoop.http.exception;

import org.znerd.xmlenc.XMLOutputter;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.StringWriter;

/**
 * Provide utility functions for http errors.
 *
 */
public class HttpExceptionUtil {

    /**
     * Output error xml
     *
     * @param t throwable
     * @param path url
     * @param doc the outputter
     * @throws IOException
     */
    public static void writeErrorXml(Throwable t, String path, XMLOutputter doc) throws IOException {
        doc.startTag(HttpRemoteException.class.getSimpleName());
        if (path == null)
            path = "/";
        doc.attribute("path", path);

        if (t instanceof HttpRemoteException) {
            doc.attribute("class", ((HttpRemoteException) t).getClassName());
        }
        else {
            doc.attribute("class", t.getClass().getName());
        }

        String msg = t.getLocalizedMessage();
        if (msg == null)
            msg = "";

        Throwable cause = t.getCause();
        if (cause != null) {
            msg += " " + cause.getClass().getName() + ":" + cause.getMessage();
        }

        doc.attribute("message", msg);
        doc.endTag();
    }

    /**
     * Response error in xml
     *
     * @param response http servlet response
     * @param errorCode error code
     * @param t throwable
     * @param path the url
     * @throws IOException thrown if error
     */
    public static void sendErrorAsXml(HttpServletResponse response, int errorCode, Throwable t, String path)
            throws IOException {
        StringWriter writer = new StringWriter();
        XMLOutputter doc = new XMLOutputter(writer, "UTF-8");
        doc.declaration();
        writeErrorXml(t, path, doc);
        doc.endDocument();
        response.sendError(errorCode, writer.toString());
    }
}
