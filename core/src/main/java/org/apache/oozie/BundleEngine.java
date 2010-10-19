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
package org.apache.oozie;

import java.io.IOException;
import java.io.Writer;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.bundle.BundleSubmitCommand;
import org.apache.oozie.util.ParamChecker;

public class BundleEngine extends BaseEngine {
    /**
     * Create a system Bundle engine, with no user and no group.
     */
    public BundleEngine() {
    }

    /**
     * Create a Bundle engine to perform operations on behave of a user.
     *
     * @param user user name.
     * @param authToken the authentication token.
     */
    public BundleEngine(String user, String authToken) {
        this.user = ParamChecker.notEmpty(user, "user");
        this.authToken = ParamChecker.notEmpty(authToken, "authToken");
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.BaseEngine#change(java.lang.String, java.lang.String)
     */
    @Override
    public void change(String jobId, String changeValue) throws BaseEngineException {
        throw new BaseEngineException(new XException(ErrorCode.E0301));
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.BaseEngine#dryrunSubmit(org.apache.hadoop.conf.Configuration, boolean)
     */
    @Override
    public String dryrunSubmit(Configuration conf, boolean startJob) throws BundleEngineException {
        BundleSubmitCommand submit = new BundleSubmitCommand(true, conf, getAuthToken());
        try {
            String jobId = submit.call();
            return jobId;
        }
        catch (CommandException ex) {
            throw new BundleEngineException(ex);
        }
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.BaseEngine#getCoordJob(java.lang.String)
     */
    @Override
    public CoordinatorJob getCoordJob(String jobId) throws BaseEngineException {
        throw new BaseEngineException(new XException(ErrorCode.E0301));
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.BaseEngine#getCoordJob(java.lang.String, int, int)
     */
    @Override
    public CoordinatorJob getCoordJob(String jobId, int start, int length) throws BaseEngineException {
        throw new BaseEngineException(new XException(ErrorCode.E0301));
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.BaseEngine#getDefinition(java.lang.String)
     */
    @Override
    public String getDefinition(String jobId) throws BaseEngineException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.BaseEngine#getJob(java.lang.String)
     */
    @Override
    public WorkflowJob getJob(String jobId) throws BaseEngineException {
        throw new BaseEngineException(new XException(ErrorCode.E0301));
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.BaseEngine#getJob(java.lang.String, int, int)
     */
    @Override
    public WorkflowJob getJob(String jobId, int start, int length) throws BaseEngineException {
        throw new BaseEngineException(new XException(ErrorCode.E0301));
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.BaseEngine#getJobIdForExternalId(java.lang.String)
     */
    @Override
    public String getJobIdForExternalId(String externalId) throws BaseEngineException {
        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.BaseEngine#kill(java.lang.String)
     */
    @Override
    public void kill(String jobId) throws BaseEngineException {
        // TODO Auto-generated method stub
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.BaseEngine#reRun(java.lang.String, org.apache.hadoop.conf.Configuration)
     */
    @Override
    public void reRun(String jobId, Configuration conf) throws BaseEngineException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see org.apache.oozie.BaseEngine#resume(java.lang.String)
     */
    @Override
    public void resume(String jobId) throws BaseEngineException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see org.apache.oozie.BaseEngine#start(java.lang.String)
     */
    @Override
    public void start(String jobId) throws BaseEngineException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see org.apache.oozie.BaseEngine#streamLog(java.lang.String, java.io.Writer)
     */
    @Override
    public void streamLog(String jobId, Writer writer) throws IOException, BaseEngineException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see org.apache.oozie.BaseEngine#submitJob(org.apache.hadoop.conf.Configuration, boolean)
     */
    @Override
    public String submitJob(Configuration conf, boolean startJob) throws BundleEngineException {
        BundleSubmitCommand submit = new BundleSubmitCommand(conf, getAuthToken());
        try {
            String jobId = submit.call();
            return jobId;
        }
        catch (CommandException ex) {
            throw new BundleEngineException(ex);
        }
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.BaseEngine#suspend(java.lang.String)
     */
    @Override
    public void suspend(String jobId) throws BaseEngineException {
        // TODO Auto-generated method stub
    }
}
