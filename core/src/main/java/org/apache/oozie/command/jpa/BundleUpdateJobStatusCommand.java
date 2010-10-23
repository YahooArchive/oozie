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

import java.util.Date;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.oozie.BundleJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.util.ParamChecker;

/**
 * Update the given bundle job bean to DB.
 */
public class BundleUpdateJobStatusCommand implements JPACommand<String> {

    private BundleJobBean bundleJob = null;

    public BundleUpdateJobStatusCommand(BundleJobBean bundleJob) {
        ParamChecker.notNull(bundleJob, "bundleJob");
        this.bundleJob = bundleJob;
    }

    @Override
    public String getName() {
        return "BundleUpdateJobStatusCommand";
    }

    @Override
    public String execute(EntityManager em) throws CommandException {
        em.getTransaction().begin();
        try {
            Query q = em.createNamedQuery("UPDATE_BUNDLE_JOB_STATUS");
            q.setParameter("id", bundleJob.getId());
            q.setParameter("status", bundleJob.getStatus().toString());
            q.setParameter("lastModifiedTime", new Date());
            q.executeUpdate();
        }
        catch (Exception e) {
            throw new CommandException(ErrorCode.E0603, e);
        }
        em.getTransaction().commit();
        return null;
    }
}