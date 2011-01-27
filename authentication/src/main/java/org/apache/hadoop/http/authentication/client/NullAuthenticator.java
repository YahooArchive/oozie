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
package org.apache.hadoop.http.authentication.client;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Map;

/**
 * NO-OP authenticator
 *
 */
public class NullAuthenticator extends HttpAuthenticator {

    /* (non-Javadoc)
     * @see org.apache.hadoop.http.authentication.client.HttpAuthenticator#authenticate(java.util.Map, java.net.HttpURLConnection)
     */
    @Override
    public void authenticate(Map<String, String> conf, HttpURLConnection connection) throws IOException {
    }

}
