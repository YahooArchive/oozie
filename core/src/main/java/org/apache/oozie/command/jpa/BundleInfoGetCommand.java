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
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.oozie.BundleJobBean;
import org.apache.oozie.BundleJobInfo;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.BundleJob.Status;
import org.apache.oozie.client.BundleJob.Timeunit;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.store.StoreStatusFilter;
import org.apache.oozie.util.ParamChecker;
import org.apache.openjpa.persistence.OpenJPAPersistence;
import org.apache.openjpa.persistence.OpenJPAQuery;
import org.apache.openjpa.persistence.jdbc.FetchDirection;
import org.apache.openjpa.persistence.jdbc.JDBCFetchPlan;
import org.apache.openjpa.persistence.jdbc.LRSSizeAlgorithm;
import org.apache.openjpa.persistence.jdbc.ResultSetType;

/**
 * Load the BundleJobInfo and return it.
 */
public class BundleInfoGetCommand implements JPACommand<BundleJobInfo> {

    private int start;
    private int len;
    private Map<String, List<String>> filter;

    public BundleInfoGetCommand(Map<String, List<String>> filter, int start, int len) {
        ParamChecker.notNull(filter, "filter");
        this.start = start;
        this.len = len;
        this.filter = filter;
    }

    @Override
    public String getName() {
        return "BundleInfoGetCommand";
    }

    @Override
    @SuppressWarnings("unchecked")
    public BundleJobInfo execute(EntityManager em) throws CommandException {
        em.getTransaction().begin();

        List<String> orArray = new ArrayList<String>();
        List<String> colArray = new ArrayList<String>();
        List<String> valArray = new ArrayList<String>();
        StringBuilder sb = new StringBuilder("");

        StoreStatusFilter.filter(filter, orArray, colArray, valArray, sb, StoreStatusFilter.bundleSeletStr,
                StoreStatusFilter.bundleCountStr);

        BundleJobInfo info;
        try {
            Query q = null;
            Query qTotal = null;
            if (orArray.size() == 0) {
                q = em.createNamedQuery("GET_BUNDLE_JOBS_COLUMNS");
                q.setFirstResult(start - 1);
                q.setMaxResults(len);
                qTotal = em.createNamedQuery("GET_BUNDLE_JOBS_COUNT");
            }
            else {
                StringBuilder sbTotal = new StringBuilder(sb);
                sb.append(" order by w.createdTimestamp desc ");
                q = em.createQuery(sb.toString());
                q.setFirstResult(start - 1);
                q.setMaxResults(len);
                qTotal = em.createQuery(sbTotal.toString().replace(StoreStatusFilter.bundleSeletStr,
                        StoreStatusFilter.bundleCountStr));
            }

            for (int i = 0; i < orArray.size(); i++) {
                q.setParameter(colArray.get(i), valArray.get(i));
                qTotal.setParameter(colArray.get(i), valArray.get(i));
            }

            OpenJPAQuery kq = OpenJPAPersistence.cast(q);
            JDBCFetchPlan fetch = (JDBCFetchPlan) kq.getFetchPlan();
            fetch.setFetchBatchSize(20);
            fetch.setResultSetType(ResultSetType.SCROLL_INSENSITIVE);
            fetch.setFetchDirection(FetchDirection.FORWARD);
            fetch.setLRSSizeAlgorithm(LRSSizeAlgorithm.LAST);
            List<?> resultList = kq.getResultList();
            List<Object[]> objectArrList = (List<Object[]>) resultList;
            List<BundleJobBean> bundleBeansList = new ArrayList<BundleJobBean>();

            for (Object[] arr : objectArrList) {
                BundleJobBean ww = getBeanForBundleJobFromArray(arr);
                bundleBeansList.add(ww);
            }

            int realLen = ((Long) qTotal.getSingleResult()).intValue();
            info = new BundleJobInfo(bundleBeansList, start, len, realLen);
        }
        catch (Exception e) {
            throw new CommandException(ErrorCode.E0603, e);
        }

        em.getTransaction().commit();
        return info;
    }

    private BundleJobBean getBeanForBundleJobFromArray(Object[] arr) {
        BundleJobBean bean = new BundleJobBean();
        bean.setId((String) arr[0]);
        if (arr[1] != null) {
            bean.setBundleName((String) arr[1]);
        }
        if (arr[2] != null) {
            bean.setStatus(Status.valueOf((String) arr[2]));
        }
        if (arr[3] != null) {
            bean.setUser((String) arr[3]);
        }
        if (arr[4] != null) {
            bean.setGroup((String) arr[4]);
        }
        if (arr[5] != null) {
            bean.setKickoffTime((Timestamp) arr[5]);
        }
        if (arr[6] != null) {
            bean.setEndTime((Timestamp) arr[6]);
        }
        if (arr[7] != null) {
            bean.setBundlePath((String) arr[7]);
        }
        if (arr[13] != null) {
            bean.setTimeUnit(Timeunit.valueOf((String) arr[13]));
        }
        if (arr[15] != null) {
            bean.setTimeOut((Integer) arr[15]);
        }
        return bean;
    }
}