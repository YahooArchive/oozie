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
package org.apache.oozie.client.rest;

/**
 * Constansts used by Oozie REST WS API
 */
public interface RestConstants {

    public static final String VERSIONS = "versions";

    public static final String JOB = "job";

    public static final String JOBS = "jobs";

    public static final String ADMIN = "admin";

    public static final String JSON_CONTENT_TYPE = "application/json";

    public static final String XML_CONTENT_TYPE = "application/xml";

    public static final String FORM_CONTENT_TYPE = "application/x-www-form-urlencoded";

    public static final String TEXT_CONTENT_TYPE = "text/plain";

    public static final String ACTION_PARAM = "action";

    public static final String OFFSET_PARAM = "offset";

    public static final String LEN_PARAM = "len";

    public static final String JOB_RESOURCE = "/job";

    public static final String JOB_ACTION_START = "start";

    public static final String JOB_ACTION_DRYRUN = "dryrun";

    public static final String JOB_ACTION_SUSPEND = "suspend";

    public static final String JOB_ACTION_RESUME = "resume";

    public static final String JOB_ACTION_KILL = "kill";

    public static final String JOB_ACTION_CHANGE = "change";
    public static final String JOB_CHANGE_VALUE = "value";

    public static final String JOB_ACTION_RERUN = "rerun";

    public static final String JOB_COORD_ACTION_RERUN = "coord-rerun";
    
    public static final String JOB_BUNDLE_ACTION_RERUN = "bundle-rerun";

    public static final String JOB_SHOW_PARAM = "show";

    public static final String JOB_SHOW_CONFIG = "config";

    public static final String JOB_SHOW_INFO = "info";

    public static final String JOB_SHOW_LOG = "log";

    public static final String JOB_SHOW_DEFINITION = "definition";

    public static final String JOB_BUNDLE_RERUN_COORD_SCOPE_PARAM = "coord-scope";
    
    public static final String JOB_BUNDLE_RERUN_DATE_SCOPE_PARAM = "date-scope";
    
    public static final String JOB_COORD_RERUN_TYPE_PARAM = "type";

    public static final String JOB_COORD_RERUN_DATE = "date";

    public static final String JOB_COORD_RERUN_ACTION = "action";

    public static final String JOB_COORD_RERUN_SCOPE_PARAM = "scope";

    public static final String JOB_COORD_RERUN_REFRESH_PARAM = "refresh";

    public static final String JOB_COORD_RERUN_NOCLEANUP_PARAM = "nocleanup";

    public static final String JOBS_FILTER_PARAM = "filter";

    public static final String JOBS_EXTERNAL_ID_PARAM = "external-id";

    public static final String ADMIN_STATUS_RESOURCE = "status";

    public static final String ADMIN_SAFE_MODE_PARAM = "safemode";

    public static final String ADMIN_SYSTEM_MODE_PARAM = "systemmode";

    public static final String ADMIN_LOG_RESOURCE = "log";

    public static final String ADMIN_OS_ENV_RESOURCE = "os-env";

    public static final String ADMIN_JAVA_SYS_PROPS_RESOURCE = "java-sys-properties";

    public static final String ADMIN_CONFIG_RESOURCE = "configuration";

    public static final String ADMIN_INSTRUMENTATION_RESOURCE = "instrumentation";

    public static final String ADMIN_BUILD_VERSION_RESOURCE = "build-version";

    public static final String ADMIN_QUEUE_DUMP_RESOURCE = "queue-dump";

    public static final String OOZIE_ERROR_CODE = "oozie-error-code";

    public static final String OOZIE_ERROR_MESSAGE = "oozie-error-message";

    public static final String JOBTYPE_PARAM = "jobtype";

    public static final String SLA_GT_SEQUENCE_ID = "gt-sequence-id";

    public static final String MAX_EVENTS = "max-events";

    public static final String SLA = "sla";
}
