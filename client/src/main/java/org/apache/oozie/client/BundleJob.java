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

package org.apache.oozie.client;

import java.util.Date;
import java.util.List;

/**
 * Bean that represents an Oozie bundle.
 */
public interface BundleJob {

	/**
     * Defines the possible stati of an Oozie Bundle.
     */
    public static enum Status {
        PREP, PREMATER, RUNNING, SUSPENDED, SUCCEEDED, KILLED, FAILED
    }

    /**
     * Defines the possible frequency unit of all Oozie applications in Bundle.
     */
    public static enum Timeunit {
        MINUTE, HOUR, DAY, WEEK, MONTH, END_OF_DAY, END_OF_MONTH, NONE
    }
    
    /**
     * Return the path to the Oozie Bundle.
     *
     * @return the path to the Oozie Bundle.
     */
    String getBundlePath();

    /**
     * Return the name of the Oozie Bundle (from the application definition).
     *
     * @return the name of the Oozie Bundle.
     */
    String getBundleName();

    /**
     * Return the Bundle ID.
     *
     * @return the Bundle ID.
     */
    String getId();

    /**
     * Return the Bundle configuration.
     *
     * @return the Bundle configuration.
     */
    String getConf();

    /**
     * Return the Bundle status.
     *
     * @return the Bundle status.
     */
    Status getStatus();

    /**
     * Return the timeUnit for the Bundle job, it could be, Timeunit enum, e.g. MINUTE, HOUR, DAY, WEEK or MONTH
     *
     * @return the time unit for the Bundle job
     */
    Timeunit getTimeUnit();

    /**
     * Return the time out value for all the coord jobs within Bundle
     *
     * @return the time out value for the coord jobs within Bundle
     */
    int getTimeout();

    /**
     * Return the Bundle Kickoff time.
     *
     * @return the Bundle Kickoff time.
     */
    Date getKickoffTime();

    /**
     * Return the Bundle end time.
     *
     * @return the Bundle end time.
     */
    Date getEndTime();

    /**
     * Return the Bundle user owner.
     *
     * @return the Bundle user owner.
     */
    String getUser();

    /**
     * Return the Bundle group.
     *
     * @return the Bundle group.
     */
    String getGroup();

    /**
     * Return the CoordinatorJob.
     *
     * @return the CoordinatorJob.
     */
    List<CoordinatorJob> getCoordinators();

    /**
     * Return the application console URL.
     *
     * @return the application console URL.
     */
    String getConsoleUrl();

}
