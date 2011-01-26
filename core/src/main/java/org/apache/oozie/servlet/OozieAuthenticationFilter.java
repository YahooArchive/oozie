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
package org.apache.oozie.servlet;

import com.cloudera.alfredo.server.AuthenticationFilter;
import com.cloudera.alfredo.server.KerberosAuthenticationHandler;
import com.cloudera.alfredo.server.PseudoAuthenticationHandler;
import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.service.Services;

import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import java.util.Properties;

/**
 * Authentication filter that extends Alfredo AuthenticationFilter to override
 * the configuration loading.
 */
public class OozieAuthenticationFilter extends AuthenticationFilter {
    private static final String OOZIE_PREFIX = "oozie.authentication.";

    private static final String OOZIE_AUTH_TYPE = OOZIE_PREFIX + "type";
    private static final String OOZIE_AUTH_TOKEN_VALIDITY = OOZIE_PREFIX + "token.validity";
    private static final String OOZIE_SIGNATURE_SECRET = OOZIE_PREFIX + "signature.secret";

    private static final String OOZIE_COOKIE_DOMAIN = OOZIE_PREFIX + "cookie.domain";

    private static final String OOZIE_SIMPLE_ANONYMOUS_ALLOWED = OOZIE_PREFIX + "simple.anonymous.allowed";

    private static final String OOZIE_KERBEROS_PRINCIPAL = OOZIE_PREFIX + "kerberos.principal";
    private static final String OOZIE_KERBEROS_KEYTAB = OOZIE_PREFIX + "kerberos.keytab";

    @Override
    protected Properties getConfiguration(String configPrefix, FilterConfig filterConfig) throws ServletException {
        Properties props = new Properties();
        Configuration conf = Services.get().getConf();

        String type = conf.get(OOZIE_AUTH_TYPE, PseudoAuthenticationHandler.TYPE);
        props.setProperty(AUTH_TYPE, type);

        props.setProperty(AUTH_TOKEN_VALIDITY, conf.get(OOZIE_AUTH_TOKEN_VALIDITY, "36000"));

        props.setProperty(SIGNATURE_SECRET, conf.get(OOZIE_AUTH_TOKEN_VALIDITY, "36000"));

        if (conf.get(OOZIE_SIGNATURE_SECRET) != null) {
            props.setProperty(SIGNATURE_SECRET, conf.get(OOZIE_SIGNATURE_SECRET));
        }

        if (conf.get(OOZIE_COOKIE_DOMAIN) != null) {
            props.setProperty(COOKIE_DOMAIN, conf.get(OOZIE_COOKIE_DOMAIN));
        }

        props.setProperty(COOKIE_PATH, "/");

        if (type.equals(PseudoAuthenticationHandler.TYPE)) {
            props.setProperty(PseudoAuthenticationHandler.ANONYMOUS_ALLOWED,
                              conf.get(OOZIE_SIMPLE_ANONYMOUS_ALLOWED, "true"));
        }
        else if (type.equals(KerberosAuthenticationHandler.TYPE)) {
            props.setProperty(KerberosAuthenticationHandler.PRINCIPAL, conf.get(OOZIE_KERBEROS_PRINCIPAL));
            props.setProperty(KerberosAuthenticationHandler.KEYTAB, conf.get(OOZIE_KERBEROS_KEYTAB));
        }

        return props;
    }
}
