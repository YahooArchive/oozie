package org.apache.oozie.command.jpa;

import java.util.Date;
import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.util.ParamChecker;

/**
 * Update the CoordinatorJob into a Bean and persist it.
 */
public class CoordJobUpdateCommand implements JPACommand<Void> {

    private CoordinatorJobBean coordJob = null;

    /**
     * @param coordJob
     */
    public CoordJobUpdateCommand(CoordinatorJobBean coordJob) {
        ParamChecker.notNull(coordJob, "CoordinatorJobBean");
        this.coordJob = coordJob;
    }

    /*
     * (non-Javadoc)
     * 
     * @seeorg.apache.oozie.command.jpa.JPACommand#execute(javax.persistence.
     * EntityManager)
     */
    @Override
    public Void execute(EntityManager em) throws CommandException {

        try {
            Query q = em.createNamedQuery("UPDATE_COORD_JOB");
            q.setParameter("id", this.coordJob.getId());
            setJobQueryParameters(this.coordJob, q);
            q.executeUpdate();
            return null;
        }
        catch (Exception e) {
            throw new CommandException(ErrorCode.E0603, e);
        }

    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.oozie.command.jpa.JPACommand#getName()
     */
    @Override
    public String getName() {
        return "CoordinatorUpdateJobCommand";
    }

    /**
     * @param jBean
     * @param q
     */
    private void setJobQueryParameters(CoordinatorJobBean jBean, Query q) {
        q.setParameter("appName", jBean.getAppName());
        q.setParameter("appPath", jBean.getAppPath());
        q.setParameter("concurrency", jBean.getConcurrency());
        q.setParameter("conf", jBean.getConf());
        q.setParameter("externalId", jBean.getExternalId());
        q.setParameter("frequency", jBean.getFrequency());
        q.setParameter("lastActionNumber", jBean.getLastActionNumber());
        q.setParameter("timeOut", jBean.getTimeout());
        q.setParameter("timeZone", jBean.getTimeZone());
        q.setParameter("authToken", jBean.getAuthToken());
        q.setParameter("createdTime", jBean.getCreatedTimestamp());
        q.setParameter("endTime", jBean.getEndTimestamp());
        q.setParameter("execution", jBean.getExecution());
        q.setParameter("jobXml", jBean.getJobXml());
        q.setParameter("lastAction", jBean.getLastActionTimestamp());
        q.setParameter("lastModifiedTime", new Date());
        q.setParameter("nextMaterializedTime", jBean.getNextMaterializedTimestamp());
        q.setParameter("origJobXml", jBean.getOrigJobXml());
        q.setParameter("slaXml", jBean.getSlaXml());
        q.setParameter("startTime", jBean.getStartTimestamp());
        q.setParameter("status", jBean.getStatus().toString());
        q.setParameter("timeUnit", jBean.getTimeUnitStr());
    }
}
