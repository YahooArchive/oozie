package org.apache.oozie.command.coord;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.XException;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.jpa.CoordJobGetCommand;
import org.apache.oozie.command.jpa.CoordinatorUpdateJobCommand;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XLog;

public class XCoordChangeCommand extends XCoordinatorCommand<Void> {
    private String jobId;
    private Date newEndTime = null;
    private Integer newConcurrency = null;
    private Date newPauseTime = null;
    private boolean resetPauseTime = false;
    private final XLog LOG = XLog.getLog(XCoordChangeCommand.class);
    private String changeValue;
    private CoordinatorJobBean coordJob;

    /**
     * Update the coordinator job bean and update that to database.
     * 
     * @param id
     * @param changeValue
     */
    public XCoordChangeCommand(String id, String changeValue) {
        super("coord_change", "coord_change", 0);
        this.jobId = ParamChecker.notEmpty(id, "id");
        ParamChecker.notEmpty(changeValue, "value");
        this.changeValue = changeValue;
    }

    /**
     * @param changeValue change value.
     * @throws CommandException thrown if changeValue cannot be parsed properly.
     */
    private void parseChangeValue(String changeValue) throws CommandException {
        Map<String, String> map = new HashMap<String, String>();
        String[] tokens = changeValue.split(";");
        int size = tokens.length;

        if (size < 0 || size > 3) {
            throw new CommandException(ErrorCode.E1015, changeValue, "must change endtime|concurrency|pausetime");
        }

        for (String token : tokens) {
            String[] pair = token.split("=");
            String key = pair[0];

            if (!key.equals(OozieClient.CHANGE_VALUE_ENDTIME) && !key.equals(OozieClient.CHANGE_VALUE_CONCURRENCY)
                    && !key.equals(OozieClient.CHANGE_VALUE_PAUSETIME)) {
                throw new CommandException(ErrorCode.E1015, changeValue, "must change endtime|concurrency|pausetime");
            }

            if (!key.equals(OozieClient.CHANGE_VALUE_PAUSETIME) && pair.length != 2) {
                throw new CommandException(ErrorCode.E1015, changeValue, "elements on " + key
                        + " must be name=value pair");
            }

            if (key.equals(OozieClient.CHANGE_VALUE_PAUSETIME) && pair.length != 2 && pair.length != 1) {
                throw new CommandException(ErrorCode.E1015, changeValue, "elements on " + key
                        + " must be name=value pair or name=(empty string to reset pause time to null)");
            }

            if (map.containsKey(key)) {
                throw new CommandException(ErrorCode.E1015, changeValue, "can not specify repeated change values on "
                        + key);
            }

            if (pair.length == 2) {
                map.put(key, pair[1]);
            }
            else {
                map.put(key, "");
            }
        }

        if (map.containsKey(OozieClient.CHANGE_VALUE_ENDTIME)) {
            String value = map.get(OozieClient.CHANGE_VALUE_ENDTIME);
            try {
                newEndTime = DateUtils.parseDateUTC(value);
            }
            catch (Exception ex) {
                throw new CommandException(ErrorCode.E1015, value, "must be a valid date");
            }
        }

        if (map.containsKey(OozieClient.CHANGE_VALUE_CONCURRENCY)) {
            String value = map.get(OozieClient.CHANGE_VALUE_CONCURRENCY);
            try {
                newConcurrency = Integer.parseInt(value);
            }
            catch (NumberFormatException ex) {
                throw new CommandException(ErrorCode.E1015, value, "must be a valid integer");
            }
        }

        if (map.containsKey(OozieClient.CHANGE_VALUE_PAUSETIME)) {
            String value = map.get(OozieClient.CHANGE_VALUE_PAUSETIME);
            if (value.equals("")) { // this is to reset pause time to null;
                resetPauseTime = true;
            }
            else {
                try {
                    newPauseTime = DateUtils.parseDateUTC(value);
                }
                catch (Exception ex) {
                    throw new CommandException(ErrorCode.E1015, value, "must be a valid date");
                }
            }
        }
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#eagerVerifyPrecondition()
     */
    @Override
    protected void eagerVerifyPrecondition() throws CommandException {
        super.eagerVerifyPrecondition();
        parseChangeValue(this.changeValue);
    }

    /**
     * @param coordJob coordinator job id.
     * @param newEndTime new end time.
     * @throws CommandException thrown if new end time is not valid.
     */
    private void checkEndTime(CoordinatorJobBean coordJob, Date newEndTime) throws CommandException {
        // New endTime cannot be before coordinator job's start time.
        Date startTime = coordJob.getStartTime();
        if (newEndTime.before(startTime)) {
            throw new CommandException(ErrorCode.E1015, newEndTime, "cannot be before coordinator job's start time ["
                    + startTime + "]");
        }

        // New endTime cannot be before coordinator job's last action time.
        Date lastActionTime = coordJob.getLastActionTime();
        if (lastActionTime != null) {
            Date d = new Date(lastActionTime.getTime() - coordJob.getFrequency() * 60 * 1000);
            if (!newEndTime.after(d)) {
                throw new CommandException(ErrorCode.E1015, newEndTime,
                        "must be after coordinator job's last action time [" + d + "]");
            }
        }
    }

    /**
     * @param coordJob coordinator job id.
     * @param newPauseTime new pause time.
     * @param newEndTime new end time, can be null meaning no change on end time.
     * @throws CommandException thrown if new pause time is not valid.
     */
    private void checkPauseTime(CoordinatorJobBean coordJob, Date newPauseTime, Date newEndTime)
    throws CommandException {
        // New pauseTime cannot be before coordinator job's start time.
        Date startTime = coordJob.getStartTime();
        if (newPauseTime.before(startTime)) {
            throw new CommandException(ErrorCode.E1015, newPauseTime, "cannot be before coordinator job's start time ["
                    + startTime + "]");
        }

        // New pauseTime cannot be before coordinator job's last action time.
        Date lastActionTime = coordJob.getLastActionTime();
        if (lastActionTime != null) {
            Date d = new Date(lastActionTime.getTime() - coordJob.getFrequency() * 60 * 1000);
            if (!newPauseTime.after(d)) {
                throw new CommandException(ErrorCode.E1015, newPauseTime,
                        "must be after coordinator job's last action time [" + d + "]");
            }
        }

        // New pauseTime must be before coordinator job's end time.
        Date endTime = (newEndTime != null) ? newEndTime : coordJob.getEndTime();
        if (!newPauseTime.before(endTime)) {
            throw new CommandException(ErrorCode.E1015, newPauseTime, "must be before coordinator job's end time ["
                    + endTime + "]");
        }
    }

    /**
     * @param coordJob coordinator job id.
     * @param newEndTime new end time.
     * @param newConcurrency new concurrency.
     * @param newPauseTime new pause time.
     * @throws CommandException thrown if new values are not valid.
     */
    private void check(CoordinatorJobBean coordJob, Date newEndTime, Integer newConcurrency, Date newPauseTime)
    throws CommandException {
        if (coordJob.getStatus() == CoordinatorJob.Status.KILLED) {
            throw new CommandException(ErrorCode.E1016);
        }

        if (newEndTime != null) {
            checkEndTime(coordJob, newEndTime);
        }

        if (newPauseTime != null) {
            checkPauseTime(coordJob, newPauseTime, newEndTime);
        }
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#execute()
     */
    @Override
    protected Void execute() throws CommandException {
        try {
            setLogInfo(this.coordJob);

            check(this.coordJob, newEndTime, newConcurrency, newPauseTime);

            if (newEndTime != null) {
                this.coordJob.setEndTime(newEndTime);
                if (this.coordJob.getStatus() == CoordinatorJob.Status.SUCCEEDED) {
                    this.coordJob.setStatus(CoordinatorJob.Status.RUNNING);
                }
            }

            if (newConcurrency != null) {
                this.coordJob.setConcurrency(newConcurrency);
            }

            if (newPauseTime != null || resetPauseTime == true) {
                this.coordJob.setPauseTime(newPauseTime);
            }

            incrJobCounter(1);

            JPAService jpaServiceCoordUpdate = Services.get().get(JPAService.class);
            if (jpaServiceCoordUpdate != null) {
                jpaServiceCoordUpdate.execute(new CoordinatorUpdateJobCommand(this.coordJob));
            }
            else {
                getLog().error(ErrorCode.E0610);
                return null;
            }

            return null;
        }
        catch (XException ex) {
            throw new CommandException(ex);
        }
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#getEntityKey()
     */
    @Override
    protected String getEntityKey() {
        return this.jobId;
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
        try {
            JPAService jpaService = Services.get().get(JPAService.class);

            if (jpaService != null) {
                this.coordJob = jpaService.execute(new CoordJobGetCommand(jobId));
                check(this.coordJob, newEndTime, newConcurrency, newPauseTime);
            }
            else {
                getLog().error(ErrorCode.E0610);
            }
        }
        catch (XException ex) {
            throw new CommandException(ex);
        }
    }

    /**
     * Return the {link XLog} instance used by the command.
     * <p/>
     * The log instance belongs to the {link XCoordChangeCommand}.
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
