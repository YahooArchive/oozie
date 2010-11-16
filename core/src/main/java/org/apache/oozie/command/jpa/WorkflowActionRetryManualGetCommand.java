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
package org.apache.oozie.command.jpa;

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.util.ParamChecker;

public class WorkflowActionRetryManualGetCommand implements JPACommand<List<WorkflowActionBean>>{

    private String wfId = null;
    List<WorkflowActionBean> actions;

    /**
     * @param wfId
     */
    public WorkflowActionRetryManualGetCommand(String wfId) {
        ParamChecker.notNull(wfId, "wfId");
        this.wfId = wfId;
        this.actions = null;
    }
    /* (non-Javadoc)
     * @see org.apache.oozie.command.jpa.JPACommand#execute(javax.persistence.EntityManager)
     */
    @Override
    public List<WorkflowActionBean> execute(EntityManager em) throws CommandException {
        try {
            Query q = em.createNamedQuery("GET_RETRY_MANUAL_ACTIONS");
            q.setParameter("wfId", wfId);
            actions = q.getResultList();
        }
        catch (IllegalStateException e) {
            throw new CommandException(ErrorCode.E0601, e.getMessage(), e);
        }
        return actions;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.jpa.JPACommand#getName()
     */
    @Override
    public String getName() {
        return "WorkflowActionGetCommand";
    }

}
