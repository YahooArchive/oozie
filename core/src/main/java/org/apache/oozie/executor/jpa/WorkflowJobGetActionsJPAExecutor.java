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
package org.apache.oozie.executor.jpa;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.util.ParamChecker;

/**
 * Load workflow actions for a workflow job.
 */
public class WorkflowJobGetActionsJPAExecutor implements JPAExecutor<List<WorkflowActionBean>> {

    private String wfJobId = null;

    public WorkflowJobGetActionsJPAExecutor(String wfJobId) {
        ParamChecker.notNull(wfJobId, "wfJobId");
        this.wfJobId = wfJobId;
    }

    @Override
    public String getName() {
        return "WorkflowJobGetActionsJPAExecutor";
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<WorkflowActionBean> execute(EntityManager em) throws JPAExecutorException {
        List<WorkflowActionBean> actions;
        List<WorkflowActionBean> actionList = new ArrayList<WorkflowActionBean>();
        try {
            Query q = em.createNamedQuery("GET_ACTIONS_FOR_WORKFLOW");
            q.setParameter("wfId", wfJobId);
            actions = q.getResultList();
            for (WorkflowActionBean a : actions) {
                WorkflowActionBean aa = getBeanForRunningAction(a);
                actionList.add(aa);
            }
        }
        catch (SQLException e) {
            throw new JPAExecutorException(ErrorCode.E0603, e.getMessage(), e);
        }
        return actionList;
    }

    private WorkflowActionBean getBeanForRunningAction(WorkflowActionBean a) throws SQLException {
        if (a != null) {
            WorkflowActionBean action = new WorkflowActionBean();
            action.setId(a.getId());
            action.setConf(a.getConf());
            action.setConsoleUrl(a.getConsoleUrl());
            action.setData(a.getData());
            action.setErrorInfo(a.getErrorCode(), a.getErrorMessage());
            action.setExternalId(a.getExternalId());
            action.setExternalStatus(a.getExternalStatus());
            action.setName(a.getName());
            action.setCred(a.getCred());
            action.setRetries(a.getRetries());
            action.setTrackerUri(a.getTrackerUri());
            action.setTransition(a.getTransition());
            action.setType(a.getType());
            action.setEndTime(a.getEndTime());
            action.setExecutionPath(a.getExecutionPath());
            action.setLastCheckTime(a.getLastCheckTime());
            action.setLogToken(a.getLogToken());
            if (a.getPending() == true) {
                action.setPending();
            }
            action.setPendingAge(a.getPendingAge());
            action.setSignalValue(a.getSignalValue());
            action.setSlaXml(a.getSlaXml());
            action.setStartTime(a.getStartTime());
            action.setStatus(a.getStatus());
            action.setJobId(a.getWfId());
            action.setUserRetryCount(a.getUserRetryCount());
            action.setUserRetryInterval(a.getUserRetryInterval());
            action.setUserRetryMax(a.getUserRetryMax());
            action.setUserProductVersion(a.getUserProductVersion());
            return action;
        }
        return null;
    }

}
