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

import java.lang.reflect.Constructor;

import org.xml.sax.Attributes;

public class HttpRemoteException extends Exception {

    private static final long serialVersionUID = 1L;

    private String className;

    public HttpRemoteException(String className, String msg) {
        super(msg);
        this.className = className;
    }

    public String getClassName() {
        return className;
    }

    /**
     * Instantiate and return the exception wrapped up by this remote exception.
     * This unwraps any <code>Throwable</code> that has a constructor taking a
     * <code>String</code> as a parameter. Otherwise it returns this.
     *
     * @return <code>Throwable</code>
     */
    public Exception unwrapRemoteException() {
        try {
            Class<?> realClass = Class.forName(getClassName());
            return instantiateException(realClass.asSubclass(Exception.class));
        }
        catch (Throwable ignore) {
            // cannot instantiate the original exception, just return this
        }
        return this;
    }

    private Exception instantiateException(Class<? extends Exception> cls) throws Exception {
        Constructor<? extends Exception> cn = cls.getConstructor(String.class);
        cn.setAccessible(true);
        String firstLine = this.getMessage();
        Exception ex = cn.newInstance(firstLine);
        ex.initCause(this);
        return ex;
    }

    /** Create RemoteException from attributes */
    public static HttpRemoteException valueOf(Attributes attrs) {
        return new HttpRemoteException(attrs.getValue("class"), attrs.getValue("message"));
    }
}
