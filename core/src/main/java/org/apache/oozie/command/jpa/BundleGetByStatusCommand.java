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

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.oozie.BundleJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.util.ParamChecker;

/**
 * A list of Bundle Jobs that are matched with the status and have last materialized time' older than
 * checkAgeSecs will be returned.
 */
public class BundleGetByStatusCommand implements JPACommand<List<BundleJobBean>> {

    private long checkAgeSecs;
    private String status;
    private int limit;

    public BundleGetByStatusCommand(long checkAgeSecs, String status, int limit) {
        ParamChecker.notNull(status, "status");
        this.checkAgeSecs = checkAgeSecs;
        this.status = status;
        this.limit = limit;
    }

    @Override
    public String getName() {
        return "BundleGetByStatusCommand";
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<BundleJobBean> execute(EntityManager em) throws CommandException {
        em.getTransaction().begin();

        List<BundleJobBean> bjBeans;
        List<BundleJobBean> jobList = new ArrayList<BundleJobBean>();
        try {
            Query q = em.createNamedQuery("GET_BUNDLE_JOBS_OLDER_THAN_STATUS");
            Timestamp ts = new Timestamp(System.currentTimeMillis() - checkAgeSecs * 1000);
            q.setParameter("lastModTime", ts);
            q.setParameter("status", status);
            if (limit > 0) {
                q.setMaxResults(limit);
            }
            bjBeans = q.getResultList();
            for (BundleJobBean j : bjBeans) {
                jobList.add(j);
            }
        }
        catch (Exception e) {
            throw new CommandException(ErrorCode.E0603, e);
        }
        em.getTransaction().commit();
        return jobList;
    }
}