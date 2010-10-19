package org.apache.oozie.store;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.oozie.BundleJobBean;
import org.apache.oozie.BundleJobInfo;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.BundleJob.Status;
import org.apache.oozie.client.BundleJob.Timeunit;
import org.apache.oozie.service.InstrumentationService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.Instrumentation;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XLog;
import org.apache.openjpa.persistence.OpenJPAPersistence;
import org.apache.openjpa.persistence.OpenJPAQuery;
import org.apache.openjpa.persistence.jdbc.FetchDirection;
import org.apache.openjpa.persistence.jdbc.JDBCFetchPlan;
import org.apache.openjpa.persistence.jdbc.LRSSizeAlgorithm;
import org.apache.openjpa.persistence.jdbc.ResultSetType;

public class BundleStore extends Store {

    private final XLog log = XLog.getLog(getClass());

    private EntityManager entityManager;
    private static final String INSTR_GROUP = "db";
    public static final int LOCK_TIMEOUT = 50000;
    private static final long DAY_IN_MS = 24 * 60 * 60 * 1000;

    public BundleStore(boolean selectForUpdate) throws StoreException {
        super();
        entityManager = getEntityManager();
    }

    public BundleStore(Store store, boolean selectForUpdate) throws StoreException {
        super(store);
        entityManager = getEntityManager();
    }

    /**
     * Create a BundleJobBean. It also creates the process instance for the job.
     *
     * @param bundle bundle bean
     * @throws StoreException
     */

    public void insertBundleJob(final BundleJobBean bundleJob) throws StoreException {
        ParamChecker.notNull(bundleJob, "bundleJob");

        doOperation("insertBundleJob", new Callable<Void>() {
            public Void call() throws StoreException {
                entityManager.persist(bundleJob);
                return null;
            }
        });
    }

    /**
     * Load the BundleJob into a Bean and return it. Also load the bundle Instance into the bean. And lock the bundle
     * depending on the locking parameter.
     *
     * @param id Job ID
     * @param locking Flag for Table Lock
     * @return BundleJobBean
     * @throws StoreException
     */
    public BundleJobBean getBundleJob(final String id, final boolean locking) throws StoreException {
        ParamChecker.notEmpty(id, "BundleJobId");
        BundleJobBean bjBean = doOperation("getBundleJob", new Callable<BundleJobBean>() {
            @SuppressWarnings("unchecked")
            public BundleJobBean call() throws StoreException {
                Query q = entityManager.createNamedQuery("GET_BUNDLE_JOB");
                q.setParameter("id", id);
                List<BundleJobBean> cjBeans = q.getResultList();

                if (cjBeans.size() > 0) {
                    return cjBeans.get(0);
                }
                else {
                    throw new StoreException(ErrorCode.E0604, id);
                }
            }
        });

        bjBean.setStatus(bjBean.getStatus());
        return bjBean;
    }

    private <V> V doOperation(String name, Callable<V> command) throws StoreException {
        try {
            Instrumentation.Cron cron = new Instrumentation.Cron();
            cron.start();
            V retVal;
            try {
                retVal = command.call();
            }
            finally {
                cron.stop();
            }
            Services.get().get(InstrumentationService.class).get().addCron(INSTR_GROUP, name, cron);
            return retVal;
        }
        catch (StoreException ex) {
            throw ex;
        }
        catch (SQLException ex) {
            throw new StoreException(ErrorCode.E0603, name, ex.getMessage(), ex);
        }
        catch (Exception e) {
            throw new StoreException(ErrorCode.E0607, name, e.getMessage(), e);
        }
    }

    /**
     * A list of Bundle Jobs that are matched with the status and have last materialized time' older than
     * checkAgeSecs will be returned.
     *
     * @param checkAgeSecs Job age in Seconds
     * @param status bundle Job Status
     * @param limit Number of results to return
     * @param locking Flag for Table Lock
     * @return List of bundle Jobs that are matched with the parameters.
     * @throws StoreException
     */
    public List<BundleJobBean> getBundleJobsOlderThanStatus(final long checkAgeSecs, final String status,
            final int limit, final boolean locking) throws StoreException {

        ParamChecker.notNull(status, "Bundle Job Status");
        List<BundleJobBean> bjBeans = doOperation("getBundleOlderThanStatus", new Callable<List<BundleJobBean>>() {
            public List<BundleJobBean> call() throws StoreException {

                List<BundleJobBean> bjBeans;
                List<BundleJobBean> jobList = new ArrayList<BundleJobBean>();
                try {
                    Query q = entityManager.createNamedQuery("GET_BUNDLE_JOBS_OLDER_THAN_STATUS");
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
                    throw new StoreException(ErrorCode.E0603, e.getMessage(), e);
                }
                return jobList;

            }
        });
        return bjBeans;
    }

    /**
     * Update the given bundle job bean to DB.
     *
     * @param jobbean Bundle Job Bean
     * @throws StoreException if action doesn't exist
     */
    public void updateBundleJob(final BundleJobBean job) throws StoreException {
        ParamChecker.notNull(job, "BundleJobBean");
        doOperation("updateJob", new Callable<Void>() {
            public Void call() throws StoreException {
                Query q = entityManager.createNamedQuery("UPDATE_BUNDLE_JOB");
                q.setParameter("id", job.getId());
                setJobQueryParameters(job, q);
                q.executeUpdate();
                return null;
            }
        });
    }

    public void updateBundleJobStatus(final BundleJobBean job) throws StoreException {
        ParamChecker.notNull(job, "BundleJobBean");
        doOperation("updateJobStatus", new Callable<Void>() {
            public Void call() throws StoreException {
                Query q = entityManager.createNamedQuery("UPDATE_BUNDLE_JOB_STATUS");
                q.setParameter("id", job.getId());
                q.setParameter("status", job.getStatus().toString());
                q.setParameter("lastModifiedTime", new Date());
                q.executeUpdate();
                return null;
            }
        });
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

    public BundleJobInfo getBundleInfo(final Map<String, List<String>> filter, final int start, final int len)
            throws StoreException {

        BundleJobInfo bundleJobInfo = doOperation("getBundleJobInfo", new Callable<BundleJobInfo>() {
            public BundleJobInfo call() throws SQLException, StoreException {
                List<String> orArray = new ArrayList<String>();
                List<String> colArray = new ArrayList<String>();
                List<String> valArray = new ArrayList<String>();
                StringBuilder sb = new StringBuilder("");

                StoreStatusFilter.filter(filter, orArray, colArray, valArray, sb, StoreStatusFilter.bundleSeletStr,
                        StoreStatusFilter.bundleCountStr);

                int realLen = 0;

                Query q = null;
                Query qTotal = null;
                if (orArray.size() == 0) {
                    q = entityManager.createNamedQuery("GET_BUNDLE_JOBS_COLUMNS");
                    q.setFirstResult(start - 1);
                    q.setMaxResults(len);
                    qTotal = entityManager.createNamedQuery("GET_BUNDLE_JOBS_COUNT");
                }
                else {
                    StringBuilder sbTotal = new StringBuilder(sb);
                    sb.append(" order by w.createdTimestamp desc ");
                    XLog.getLog(getClass()).debug("Created String is **** " + sb.toString());
                    q = entityManager.createQuery(sb.toString());
                    q.setFirstResult(start - 1);
                    q.setMaxResults(len);
                    qTotal = entityManager.createQuery(sbTotal.toString().replace(StoreStatusFilter.bundleSeletStr,
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
                List<?> resultList = q.getResultList();
                List<Object[]> objectArrList = (List<Object[]>) resultList;
                List<BundleJobBean> bundleBeansList = new ArrayList<BundleJobBean>();

                for (Object[] arr : objectArrList) {
                    BundleJobBean ww = getBeanForBundleJobFromArray(arr);
                    bundleBeansList.add(ww);
                }

                realLen = ((Long) qTotal.getSingleResult()).intValue();

                return new BundleJobInfo(bundleBeansList, start, len, realLen);
            }
        });
        return bundleJobInfo;
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
        if (arr[8] != null) {
            bean.setTimeUnit(Timeunit.valueOf((String) arr[13]));
        }
        if (arr[9] != null) {
            bean.setTimeOut((Integer) arr[15]);
        }
        return bean;
    }

}
