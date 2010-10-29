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
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.util.ParamChecker;

/**
 * Load the WorkflowJob into a Bean and return it.
 */
public class WorkflowJobGetCommand implements JPACommand<WorkflowJobBean> {

    private String wfJobId = null;

    public WorkflowJobGetCommand(String wfJobId) {
        ParamChecker.notNull(wfJobId, "wfJobId");
        this.wfJobId = wfJobId;
    }

    @Override
    public String getName() {
        return "CoordinatorJobGetCommand";
    }

    @Override
    @SuppressWarnings("unchecked")
    public WorkflowJobBean execute(EntityManager em) throws CommandException {
        List<WorkflowJobBean> wjBeans;
        try {
            Query q = em.createNamedQuery("GET_WORKFLOW");
            q.setParameter("id", wfJobId);
            wjBeans = q.getResultList();
        }
        catch (Exception e) {
            throw new CommandException(ErrorCode.E0603, e);
        }
        WorkflowJobBean bean = null;
        if (wjBeans != null && wjBeans.size() > 0) {
            bean = wjBeans.get(0);
            bean.setStatus(bean.getStatus());
            return bean;
        }
        else {
            throw new CommandException(ErrorCode.E0604, wfJobId);
        }
    }
}