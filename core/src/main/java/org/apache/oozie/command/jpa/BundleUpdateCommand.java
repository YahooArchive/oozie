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
 *  Update the given bundle job bean to DB.
 */
public class BundleUpdateCommand implements JPACommand<String> {

    private BundleJobBean bundleJob = null;

    public BundleUpdateCommand(BundleJobBean bundleJob) {
        ParamChecker.notNull(bundleJob, "bundleJob");
        this.bundleJob = bundleJob;
    }

    @Override
    public String getName() {
        return "BundleUpdateCommand";
    }

    @Override
    public String execute(EntityManager em) throws CommandException {
        em.getTransaction().begin();
        try {
            Query q = em.createNamedQuery("UPDATE_BUNDLE_JOB");
            q.setParameter("id", bundleJob.getId());
            setJobQueryParameters(bundleJob, q);
            q.executeUpdate();
        }
        catch (Exception e) {
            throw new CommandException(ErrorCode.E0603, e);
        }
        em.getTransaction().commit();
        return null;
    }

    private void setJobQueryParameters(BundleJobBean jBean, Query q) {
        q.setParameter("bundleName", jBean.getBundleName());
        q.setParameter("bundlePath", jBean.getBundlePath());
        q.setParameter("conf", jBean.getConf());
        q.setParameter("externalId", jBean.getExternalId());
        q.setParameter("timeOut", jBean.getTimeout());
        q.setParameter("authToken", jBean.getAuthToken());
        q.setParameter("createdTime", jBean.getCreatedTimestamp());
        q.setParameter("endTime", jBean.getEndTimestamp());
        q.setParameter("jobXml", jBean.getJobXml());
        q.setParameter("lastModifiedTime", new Date());
        q.setParameter("origJobXml", jBean.getOrigJobXml());
        q.setParameter("kickoffTime", jBean.getKickoffTimestamp());
        q.setParameter("status", jBean.getStatus().toString());
        q.setParameter("timeUnit", jBean.getTimeUnitStr());
    }
}