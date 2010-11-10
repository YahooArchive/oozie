package org.apache.oozie.command.coord;

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.jpa.CoordActionGetCommand;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XLog;

public class CoordActionInfoXCommand extends CoordinatorXCommand<CoordinatorActionBean> {

    private final String id;
    private static XLog LOG = XLog.getLog(CoordinatorXCommand.class);

    /**
     * @param id
     */
    public CoordActionInfoXCommand(String id) {
        super("action.info", "action.info", 1);
        this.id = ParamChecker.notEmpty(id, "id");
        LOG.debug("Command for coordinator action " + id);
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#execute()
     */
    @Override
    protected CoordinatorActionBean execute() throws CommandException {
        JPAService jpaService = Services.get().get(JPAService.class);
        if (jpaService != null) {
            CoordinatorActionBean action = jpaService.execute(new CoordActionGetCommand(this.id));
            return action;
        }
        else {
            LOG.error(ErrorCode.E0610);
            return null;
        }
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#getEntityKey()
     */
    @Override
    protected String getEntityKey() {
        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#loadState()
     */
    @Override
    protected void loadState() {

    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#verifyPrecondition()
     */
    @Override
    protected void verifyPrecondition() throws CommandException {

    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#isLockRequired()
     */
    @Override
    protected boolean isLockRequired() {
        return false;
    }
}
