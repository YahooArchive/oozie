package org.apache.oozie.command.coord;

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.jpa.CoordActionGetCommand;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XLog;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;

public class XCoordActionInfoCommand extends XCoordinatorCommand<CoordinatorActionBean> {

    private String id;
    private static XLog LOG = XLog.getLog(XCoordinatorCommand.class);

    /**
     * @param id
     */
    public XCoordActionInfoCommand(String id) {
        super("action.info", "action.info", 1);
        this.id = ParamChecker.notEmpty(id, "id");
        getLog().debug("Command for coordinator action " + id);
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
            getLog().error(ErrorCode.E0610);
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

    /**
     * Return the {link XLog} instance used by the command.
     * <p/>
     * The log instance belongs to the {link XCoordinatorCommand}.
     * <p/>
     * Subclasses should override this method if the want to use a different log instance.
     * 
     * @return the log instance.
     */
    @Override
    protected XLog getLog() {
        return LOG;
    }
}
