package org.apache.oozie.command.jpa;

import javax.persistence.EntityManager;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.util.ParamChecker;

/**
 * Update the CoordinatorAction into a Bean and persist it.
 */
public class CoordActionUpdateCommand implements JPACommand<Void> {

    private CoordinatorActionBean coordAction = null;

    /**
     * @param coordAction
     */
    public CoordActionUpdateCommand(CoordinatorActionBean coordAction) {
        ParamChecker.notNull(coordAction, "coordAction");
        this.coordAction = coordAction;
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
            em.merge(coordAction);
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
        return "CoordActionUpdateCommand";
    }
}
