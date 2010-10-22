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
package org.apache.oozie.bundle;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.XException;

public class BundleJobException extends XException {

	private static final long serialVersionUID = 9087252556066046839L;

	/**
     * Create a bundle job exception from a XException.
     *
     * @param cause the XException cause.
     */
    public BundleJobException(XException cause) {
        super(cause);
    }

    /**
     * Create a bundle job exception.
     *
     * @param errorCode error code.
     * @param params parameters for the error code message template.
     */
    public BundleJobException(ErrorCode errorCode, Object... params) {
        super(errorCode, params);
    }

}
