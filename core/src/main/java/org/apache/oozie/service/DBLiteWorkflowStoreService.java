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
package org.apache.oozie.service;

import java.io.IOException;
import java.io.StringReader;
import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.XException;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.executor.jpa.WorkflowJobUpdateJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobsRunningGetJPAExecutor;
import org.apache.oozie.service.SchemaService.SchemaName;
import org.apache.oozie.store.Store;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.store.WorkflowStore;
import org.apache.oozie.util.Instrumentable;
import org.apache.oozie.util.Instrumentation;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XLog;
import org.apache.oozie.workflow.WorkflowApp;
import org.apache.oozie.workflow.WorkflowException;
import org.apache.oozie.workflow.WorkflowInstance;
import org.apache.oozie.workflow.WorkflowLib;
import org.apache.oozie.workflow.lite.DBLiteWorkflowLib;

public class DBLiteWorkflowStoreService extends LiteWorkflowStoreService implements Instrumentable {
    private boolean selectForUpdate;
    private XLog LOG;
    private int statusWindow;

    public static final String CONF_PREFIX = Service.CONF_PREFIX + "DBLiteWorkflowStoreService.";
    public static final String CONF_METRICS_INTERVAL_MINS = CONF_PREFIX + "status.metrics.collection.interval";
    public static final String CONF_METRICS_INTERVAL_WINDOW = CONF_PREFIX + "status.metrics.window";

    private static final String INSTRUMENTATION_GROUP = "jobstatus";
    private static final String INSTRUMENTATION_GROUP_WINDOW = "windowjobstatus";

    private Map<String, Integer> statusCounts = new HashMap<String, Integer>();
    private Map<String, Integer> statusWindowCounts = new HashMap<String, Integer>();

    /**
     * Gets the number of workflows for each status and populates the hash.
     */
    class JobStatusCountCallable implements Runnable {
        @Override
        public void run() {
            WorkflowStore store = null;
            try {
                store = Services.get().get(WorkflowStoreService.class).create();
                store.beginTrx();
                WorkflowJob.Status[] wfStatusArr = WorkflowJob.Status.values();
                for (WorkflowJob.Status aWfStatusArr : wfStatusArr) {
                    statusCounts.put(aWfStatusArr.name(), store.getWorkflowCountWithStatus(aWfStatusArr.name()));
                    statusWindowCounts.put(aWfStatusArr.name(), store.getWorkflowCountWithStatusInLastNSeconds(
                            aWfStatusArr.name(), statusWindow));
                }
                store.commitTrx();
            }
            catch (StoreException e) {
                if (store != null) {
                    store.rollbackTrx();
                }
                LOG.warn("Exception while accessing the store", e);
            }
            catch (Exception ex) {
                LOG.error("Exception, {0}", ex.getMessage(), ex);
                if (store != null && store.isActive()) {
                    try {
                        store.rollbackTrx();
                    }
                    catch (RuntimeException rex) {
                        LOG.warn("openjpa error, {0}", rex.getMessage(), rex);
                    }
                }
            }
            finally {
                if (store != null) {
                    if (!store.isActive()) {
                        try {
                            store.closeTrx();
                        }
                        catch (RuntimeException rex) {
                            LOG.warn("Exception while attempting to close store", rex);
                        }
                    }
                    else {
                        LOG.warn("transaction is not committed or rolled back before closing entitymanager.");
                    }
                }
            }
        }
    }

    public void init(Services services) throws ServiceException {
        Configuration conf = services.getConf();
        statusWindow = conf.getInt(CONF_METRICS_INTERVAL_WINDOW, 3600);
        int statusMetricsCollectionInterval = conf.getInt(CONF_METRICS_INTERVAL_MINS, 5);
        LOG = XLog.getLog(getClass());
        selectForUpdate = false;

        WorkflowJob.Status[] wfStatusArr = WorkflowJob.Status.values();
        for (WorkflowJob.Status aWfStatusArr : wfStatusArr) {
            statusCounts.put(aWfStatusArr.name(), 0);
            statusWindowCounts.put(aWfStatusArr.name(), 0);
        }
        Runnable jobStatusCountCallable = new JobStatusCountCallable();
        services.get(SchedulerService.class).schedule(jobStatusCountCallable, 1, statusMetricsCollectionInterval,
                                                      SchedulerService.Unit.MIN);
    }

    public void destroy() {
    }

    /**
     * Return the workflow lib without DB connection. Will be used for parsing purpose.
     *
     * @return Workflow Library
     */
    @Override
    public WorkflowLib getWorkflowLibWithNoDB() {
        return getWorkflowLib(null);
    }

    private WorkflowLib getWorkflowLib(Connection conn) {
        javax.xml.validation.Schema schema = Services.get().get(SchemaService.class).getSchema(SchemaName.WORKFLOW);
        return new DBLiteWorkflowLib(schema, LiteDecisionHandler.class, LiteActionHandler.class, conn);
    }

    @Override
    public WorkflowStore create() throws StoreException {
        try {
            return new WorkflowStore(selectForUpdate);
        }
        catch (Exception ex) {
            throw new StoreException(ErrorCode.E0600, ex.getMessage(), ex);
        }
    }

    @Override
    public <S extends Store> WorkflowStore create(S store) throws StoreException {
        try {
            return new WorkflowStore(store, selectForUpdate);
        }
        catch (Exception ex) {
            throw new StoreException(ErrorCode.E0600, ex.getMessage(), ex);
        }
    }

    /**
     * Reset workflow instances for all running jobs. Highly suggest to use '-systemmode SAFEMODE' before calling this
     * function.
     *
     * @throws ServiceException thrown if instance has errors
     */
    @Override
    public void resetWorkflowInstanceForRunningJobs() throws ServiceException {
        try {
            List<WorkflowJobBean> jobList = null;
            JPAService jpaService = Services.get().get(JPAService.class);
            if (jpaService != null) {
                jobList = jpaService.execute(new WorkflowJobsRunningGetJPAExecutor());
            }
            else {
                throw new CommandException(ErrorCode.E0610);
            }
            try {
                if (jobList != null && !jobList.isEmpty()) {
                    for (WorkflowJobBean job : jobList) {
                        resetWorkflowInstance(job);
                        jpaService.execute(new WorkflowJobUpdateJPAExecutor(job));
                    }
                }
            }
            catch (IOException ioe) {
                throw new ServiceException(ErrorCode.E0702, ioe.getMessage());
            }
        }
        catch (XException ex) {
            throw new ServiceException(ex);
        }
    }

    /**
     * Create new instance for existing running workflow job
     *
     * @param wfBean workflow job bean
     * @throws ServiceException thrown if instance has errors
     * @throws IOException thrown if can not create XConfiguration
     * @throws WorkflowException thrown if unable to parse wf definition
     */
    private void resetWorkflowInstance(WorkflowJobBean wfBean) throws ServiceException, IOException, WorkflowException {
        LOG.debug("Ready to reset workflow instance for wf job = " + wfBean.getId());
        WorkflowLib workflowLib = Services.get().get(WorkflowStoreService.class).getWorkflowLibWithNoDB();
        WorkflowAppService wps = Services.get().get(WorkflowAppService.class);
        XConfiguration jobConf = new XConfiguration(new StringReader(wfBean.getConf()));
        WorkflowApp app = wps.parseDef(jobConf, wfBean.getAuthToken());
        WorkflowInstance oldWfInstance = null;
        try {
            oldWfInstance = wfBean.getWorkflowInstance();
        }
        catch (Exception ex) {
            LOG.warn("Can not get old workflow instance, it has to be killed.", ex);
        }
        WorkflowInstance newWfInstance = null;

        try {
            newWfInstance = workflowLib.createInstance(app, jobConf, wfBean.getId());
        }
        catch (WorkflowException e) {
            throw new ServiceException(e);
        }

        if (oldWfInstance != null) {
            Map<String, String> oldVars = new HashMap<String, String>();
            Map<String, String> newVars = new HashMap<String, String>();
            oldVars = oldWfInstance.getAllVars();
            for (String var : oldVars.keySet()) {
                newVars.put(var, oldVars.get(var));
            }
            newWfInstance.setAllVars(newVars);
            LOG.debug("Able to move all vars from old wf instance to new one for wf job = " + wfBean.getId());
        }
        else {
            wfBean.setStatus(WorkflowJob.Status.KILLED);
            LOG.debug("Unable to set all vars, so kill wf job = " + wfBean.getId());
        }
        wfBean.setWorkflowInstance(newWfInstance);
    }

    @Override
    public void instrument(Instrumentation instr) {
        final WorkflowJob.Status[] wfStatusArr = WorkflowJob.Status.values();
        for (WorkflowJob.Status aWfStatusArr : wfStatusArr) {
            final String statusName = aWfStatusArr.name();
            instr.addVariable(INSTRUMENTATION_GROUP, statusName, new Instrumentation.Variable<Long>() {
                public Long getValue() {
                    return statusCounts.get(statusName).longValue();
                }
            });
            instr.addVariable(INSTRUMENTATION_GROUP_WINDOW, statusName, new Instrumentation.Variable<Long>() {
                public Long getValue() {
                    return statusWindowCounts.get(statusName).longValue();
                }
            });
        }
    }
}
