package org.apache.oozie.command.jpa;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.util.ParamChecker;

/**
 * JPA command to get coordinator jobs which are qualify for Materialization.
 */
public class CoordJobsToBeMaterializedCommand implements JPACommand<List<CoordinatorJobBean>> {

    private Date dateInput;
    private int limit;
    private List<CoordinatorJobBean> jobList;

    /**
     * @param date
     * @param limit
     */
    public CoordJobsToBeMaterializedCommand(Date date, int limit) {
        ParamChecker.notNull(date, "Coord Job Materialization Date");
        this.dateInput = date;
        this.limit = limit;
        jobList = new ArrayList<CoordinatorJobBean>();
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.jpa.JPACommand#execute(javax.persistence.EntityManager)
     */
    @SuppressWarnings("unchecked")
    @Override
    public List<CoordinatorJobBean> execute(EntityManager em) throws CommandException {
        try {
            Query q = em.createNamedQuery("GET_COORD_JOBS_OLDER_THAN");
            q.setParameter("matTime", new Timestamp(this.dateInput.getTime()));
            if (limit > 0) {
                q.setMaxResults(limit);
            }

            List<CoordinatorJobBean> cjBeans = q.getResultList();
            // copy results to a new object
            for (CoordinatorJobBean j : cjBeans) {
                jobList.add(j);
            }
        }
        catch (IllegalStateException e) {
            throw new CommandException(ErrorCode.E0601, e.getMessage(), e);
        }
        return jobList;
    }

    @Override
    public String getName() {
        return "CoordJobsToBeMaterializedCommand";
    }

    /**
     * @return the dateInput
     */
    public Date getDateInput() {
        return dateInput;
    }

    /**
     * @param dateInput the dateInput to set
     */
    public void setDateInput(Date dateInput) {
        this.dateInput = dateInput;
    }

    /**
     * @return the limit
     */
    public int getLimit() {
        return limit;
    }

    /**
     * @param limit the limit to set
     */
    public void setLimit(int limit) {
        this.limit = limit;
    }
}