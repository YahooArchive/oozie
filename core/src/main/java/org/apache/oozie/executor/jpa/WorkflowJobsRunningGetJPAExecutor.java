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

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.WorkflowJob;

/**
 * Load the list of running workflow jobs
 */
public class WorkflowJobsRunningGetJPAExecutor implements JPAExecutor<List<WorkflowJobBean>> {

    public WorkflowJobsRunningGetJPAExecutor() {
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.executor.jpa.JPAExecutor#getName()
     */
    @Override
    public String getName() {
        return "WorkflowJobsRunningGetJPAExecutor";
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.executor.jpa.JPAExecutor#execute(javax.persistence.EntityManager)
     */
    @Override
    @SuppressWarnings("unchecked")
    public List<WorkflowJobBean> execute(EntityManager em) throws JPAExecutorException {
        List<WorkflowJobBean> workflows = null;
        try {
            Query jobQ = em.createNamedQuery("GET_WORKFLOWS_WITH_STATUS");
            jobQ.setParameter("status", WorkflowJob.Status.RUNNING.toString());
            workflows = jobQ.getResultList();
        }
        catch (Exception e) {
            throw new JPAExecutorException(ErrorCode.E0603, e);
        }
        return workflows;
    }

}