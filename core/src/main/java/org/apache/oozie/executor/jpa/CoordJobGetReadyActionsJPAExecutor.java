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

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.util.ParamChecker;

/**
 * Load coordinator actions in READY state for a coordinator job.
 */
public class CoordJobGetReadyActionsJPAExecutor implements JPAExecutor<List<CoordinatorActionBean>> {

    private String coordJobId = null;
    private int numResults;
    private String executionOrder = null;

    public CoordJobGetReadyActionsJPAExecutor(String coordJobId, int numResults, String executionOrder) {
        ParamChecker.notNull(coordJobId, "coordJobId");
        this.coordJobId = coordJobId;
        this.numResults = numResults;
        this.executionOrder = executionOrder;
    }

    @Override
    public String getName() {
        return "CoordJobGetReadyActionsJPAExecutor";
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<CoordinatorActionBean> execute(EntityManager em) throws JPAExecutorException {
        List<CoordinatorActionBean> actionBeans = null;
        try {
            Query q;
            // check if executionOrder is FIFO, LIFO, or LAST_ONLY
            if (executionOrder.equalsIgnoreCase("FIFO")) {
                q = em.createNamedQuery("GET_COORD_ACTIONS_FOR_JOB_FIFO");
            }
            else {
                q = em.createNamedQuery("GET_COORD_ACTIONS_FOR_JOB_LIFO");
            }
            q.setParameter("jobId", coordJobId);
            
            // if executionOrder is LAST_ONLY, only retrieve first record in LIFO,
            // otherwise, use numResults if it is positive.
            if (executionOrder.equalsIgnoreCase("LAST_ONLY")) {
                q.setMaxResults(1);
            }
            else {
                if (numResults > 0) {
                    q.setMaxResults(numResults);
                }
            }
            actionBeans = q.getResultList();
            return actionBeans;
        }
        catch (Exception e) {
            throw new JPAExecutorException(ErrorCode.E0603, e);
        }        
    }

}