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
package org.apache.oozie;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.Date;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Lob;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;

import org.apache.hadoop.io.Writable;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.rest.JsonCoordinatorJob;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.WritableUtils;
import org.apache.openjpa.persistence.jdbc.Index;

@Entity
@NamedQueries( {
        @NamedQuery(name = "UPDATE_COORD_JOB", query = "update CoordinatorJobBean w set w.appName = :appName, w.appPath = :appPath, w.concurrency = :concurrency, w.conf = :conf, w.externalId = :externalId, w.frequency = :frequency, w.lastActionNumber = :lastActionNumber, w.timeOut = :timeOut, w.timeZone = :timeZone, w.authToken = :authToken, w.createdTimestamp = :createdTime, w.endTimestamp = :endTime, w.execution = :execution, w.jobXml = :jobXml, w.lastActionTimestamp = :lastAction, w.lastModifiedTimestamp = :lastModifiedTime, w.nextMaterializedTimestamp = :nextMaterializedTime, w.origJobXml = :origJobXml, w.slaXml=:slaXml, w.startTimestamp = :startTime, w.status = :status, w.timeUnitStr = :timeUnit where w.id = :id"),

        @NamedQuery(name = "UPDATE_COORD_JOB_STATUS", query = "update CoordinatorJobBean w set w.status = :status, w.lastModifiedTimestamp = :lastModifiedTime where w.id = :id"),

        @NamedQuery(name = "DELETE_COORD_JOB", query = "delete from CoordinatorJobBean w where w.id = :id"),

        @NamedQuery(name = "GET_COORD_JOBS", query = "select OBJECT(w) from CoordinatorJobBean w"),

        @NamedQuery(name = "GET_COORD_JOB", query = "select OBJECT(w) from CoordinatorJobBean w where w.id = :id"),

        @NamedQuery(name = "GET_COORD_JOBS_PENDING", query = "select OBJECT(w) from CoordinatorJobBean w where w.pending = 1 order by w.lastModifiedTimestamp"),

        @NamedQuery(name = "GET_COORD_JOBS_COUNT", query = "select count(w) from CoordinatorJobBean w"),

        @NamedQuery(name = "GET_COORD_JOBS_COLUMNS", query = "select w.id, w.appName, w.status, w.user, w.group, w.startTimestamp, w.endTimestamp, w.appPath, w.concurrency, w.frequency, w.lastActionTimestamp, w.nextMaterializedTimestamp, w.createdTimestamp, w.timeUnitStr, w.timeZone, w.timeOut from CoordinatorJobBean w order by w.createdTimestamp desc"),

        @NamedQuery(name = "GET_COORD_JOBS_OLDER_THAN", query = "select OBJECT(w) from CoordinatorJobBean w where w.startTimestamp <= :matTime AND (w.status = 'PREP' OR w.status = 'RUNNING') AND (w.nextMaterializedTimestamp < :matTime OR w.nextMaterializedTimestamp IS NULL) AND (w.nextMaterializedTimestamp IS NULL OR (w.endTimestamp > w.nextMaterializedTimestamp AND (w.pauseTimestamp IS NULL OR w.pauseTimestamp > w.nextMaterializedTimestamp))) order by w.lastModifiedTimestamp"),

        @NamedQuery(name = "GET_COORD_JOBS_OLDER_THAN_STATUS", query = "select OBJECT(w) from CoordinatorJobBean w where w.status = :status AND w.lastModifiedTimestamp <= :lastModTime order by w.lastModifiedTimestamp"),

        @NamedQuery(name = "GET_COMPLETED_COORD_JOBS_OLDER_THAN_STATUS", query = "select OBJECT(w) from CoordinatorJobBean w where ( w.status = 'SUCCEEDED' OR w.status = 'FAILED' or w.status = 'KILLED') AND w.lastModifiedTimestamp <= :lastModTime order by w.lastModifiedTimestamp"),

        @NamedQuery(name = "GET_COORD_JOBS_FOR_BUNDLE", query = "select OBJECT(w) from CoordinatorJobBean w where w.bundleId = :bundleId order by w.lastModifiedTimestamp") })
public class CoordinatorJobBean extends JsonCoordinatorJob implements Writable {

    @Basic
    @Index
    @Column(name = "status")
    private String status = CoordinatorJob.Status.PREP.toString();

    @Basic
    @Column(name = "auth_token")
    @Lob
    private String authToken = null;

    @Basic
    @Column(name = "start_time")
    private java.sql.Timestamp startTimestamp = null;

    @Basic
    @Column(name = "end_time")
    private java.sql.Timestamp endTimestamp = null;

    @Basic
    @Column(name = "pause_time")
    private java.sql.Timestamp pauseTimestamp = null;

    @Basic
    @Index
    @Column(name = "created_time")
    private java.sql.Timestamp createdTimestamp = null;

    @Basic
    @Column(name = "time_unit")
    private String timeUnitStr = CoordinatorJob.Timeunit.NONE.toString();

    @Basic
    @Column(name = "execution")
    private String execution = CoordinatorJob.Execution.FIFO.toString();

    @Basic
    @Column(name = "last_action")
    private java.sql.Timestamp lastActionTimestamp = null;

    @Basic
    @Index
    @Column(name = "next_matd_time")
    private java.sql.Timestamp nextMaterializedTimestamp = null;

    @Basic
    @Index
    @Column(name = "last_modified_time")
    private java.sql.Timestamp lastModifiedTimestamp = null;

    @Basic
    @Index
    @Column(name = "suspended_time")
    private java.sql.Timestamp suspendedTimestamp = null;

    @Column(name = "job_xml")
    @Lob
    private String jobXml = null;

    @Column(name = "orig_job_xml")
    @Lob
    private String origJobXml = null;

    @Column(name = "sla_xml")
    @Lob
    private String slaXml = null;

    @Basic
    @Column(name = "pending")
    private int pending = 0;

    /**
     * Set pending to true
     *
     * @param pending set pending to true
     */
    @Override
    public void setPending() {
        super.setPending();
        this.pending = 1;
    }

    /**
     * Set pending to false
     *
     * @param pending set pending to false
     */
    @Override
    public void resetPending() {
        super.resetPending();
        this.pending = 0;
    }

    /**
     * Return if the action is pending.
     *
     * @return if the action is pending.
     */
    public boolean isPending() {
        return pending == 1 ? true : false;
    }

    public java.sql.Timestamp getStartTimestamp() {
        return startTimestamp;
    }

    public void setStartTimestamp(java.sql.Timestamp startTimestamp) {
        super.setStartTime(DateUtils.toDate(startTimestamp));
        this.startTimestamp = startTimestamp;
    }

    public java.sql.Timestamp getEndTimestamp() {
        return endTimestamp;
    }

    public void setEndTimestamp(java.sql.Timestamp endTimestamp) {
        super.setEndTime(DateUtils.toDate(endTimestamp));
        this.endTimestamp = endTimestamp;
    }

    public Timestamp getNextMaterializedTimestamp() {
        return nextMaterializedTimestamp;
    }

    public void setNextMaterializedTimestamp(java.sql.Timestamp nextMaterializedTimestamp) {
        super.setNextMaterializedTime(DateUtils.toDate(nextMaterializedTimestamp));
        this.nextMaterializedTimestamp = nextMaterializedTimestamp;
    }

    public Timestamp getLastModifiedTimestamp() {
        return lastModifiedTimestamp;
    }

    public void setLastModifiedTimestamp(java.sql.Timestamp lastModifiedTimestamp) {
        this.lastModifiedTimestamp = lastModifiedTimestamp;
    }

    public Timestamp getSuspendedTimestamp() {
        return suspendedTimestamp;
    }

    public void setSuspendedTimestamp(java.sql.Timestamp suspendedTimestamp) {
        this.suspendedTimestamp = suspendedTimestamp;
    }

    public String getJobXml() {
        return jobXml;
    }

    public void setJobXml(String jobXml) {
        this.jobXml = jobXml;
    }

    public String getOrigJobXml() {
        return origJobXml;
    }

    public void setOrigJobXml(String origJobXml) {
        this.origJobXml = origJobXml;
    }

    public String getSlaXml() {
        return slaXml;
    }

    public void setSlaXml(String slaXml) {
        this.slaXml = slaXml;
    }

    @Override
    public void setTimeUnit(Timeunit timeUnit) {
        super.setTimeUnit(timeUnit);
        this.timeUnitStr = timeUnit.toString();
    }

    public void setLastActionTimestamp(java.sql.Timestamp lastActionTimestamp) {
        super.setLastActionTime(DateUtils.toDate(lastActionTimestamp));
        this.lastActionTimestamp = lastActionTimestamp;
    }

    public void setAuthToken(String authToken) {
        this.authToken = authToken;
    }

    public CoordinatorJobBean() {
    }

    /*
     * Serialize the coordinator bean to a data output. @param dataOutput data
     * output. @throws IOException thrown if the coordinator bean could not be
     * serialized.
     */
    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeStr(dataOutput, getAppPath());
        WritableUtils.writeStr(dataOutput, getAppName());
        WritableUtils.writeStr(dataOutput, getId());
        WritableUtils.writeStr(dataOutput, getConf());
        WritableUtils.writeStr(dataOutput, getStatusStr());
        dataOutput.writeInt(getFrequency());
        WritableUtils.writeStr(dataOutput, getTimeUnit().toString());
        WritableUtils.writeStr(dataOutput, getTimeZone());
        dataOutput.writeInt(getConcurrency());
        WritableUtils.writeStr(dataOutput, getExecutionOrder().toString());
        dataOutput.writeLong((getLastActionTime() != null) ? getLastActionTime().getTime() : -1);
        dataOutput.writeLong((getNextMaterializedTime() != null) ? getNextMaterializedTime().getTime() : -1);
        dataOutput.writeLong((getStartTime() != null) ? getStartTime().getTime() : -1);
        dataOutput.writeLong((getEndTime() != null) ? getEndTime().getTime() : -1);
        WritableUtils.writeStr(dataOutput, getUser());
        WritableUtils.writeStr(dataOutput, getGroup());
        WritableUtils.writeStr(dataOutput, getExternalId());
        dataOutput.writeInt(getTimeout());
        dataOutput.writeInt(getMatThrottling());
    }

    /**
     * Deserialize a coordinator bean from a data input.
     *
     * @param dataInput data input.
     * @throws IOException thrown if the workflow bean could not be deserialized.
     */
    public void readFields(DataInput dataInput) throws IOException {
        setAppPath(WritableUtils.readStr(dataInput));
        setAppName(WritableUtils.readStr(dataInput));
        setId(WritableUtils.readStr(dataInput));
        setConf(WritableUtils.readStr(dataInput));
        setStatus(CoordinatorJob.Status.valueOf(WritableUtils.readStr(dataInput)));
        setFrequency(dataInput.readInt());
        setTimeUnit(CoordinatorJob.Timeunit.valueOf(WritableUtils.readStr(dataInput)));
        setTimeZone(WritableUtils.readStr(dataInput));
        setConcurrency(dataInput.readInt());
        setExecutionOrder(Execution.valueOf(WritableUtils.readStr(dataInput)));

        long d = dataInput.readLong();
        if (d != -1) {
            setLastActionTime(new Date(d));
        }
        d = dataInput.readLong();
        if (d != -1) {
            setNextMaterializedTime(new Date(d));
        }
        d = dataInput.readLong();
        if (d != -1) {
            setStartTime(new Date(d));
        }

        d = dataInput.readLong();
        if (d != -1) {
            setEndTime(new Date(d));
        }
        setUser(WritableUtils.readStr(dataInput));
        setGroup(WritableUtils.readStr(dataInput));
        setExternalId(WritableUtils.readStr(dataInput));
        setTimeout(dataInput.readInt());
        setMatThrottling(dataInput.readInt());
    }

    @Override
    public Status getStatus() {
        return Status.valueOf(this.status);
    }

    public String getStatusStr() {
        return status;
    }

    @Override
    public void setStatus(Status val) {
        super.setStatus(val);
        this.status = val.toString();
    }

    public String getTimeUnitStr() {
        return timeUnitStr;
    }

    @Override
    public Timeunit getTimeUnit() {
        return Timeunit.valueOf(this.timeUnitStr);
    }

    public void setExecution(Execution order) {
        this.execution = order.toString();
        super.setExecutionOrder(order);
    }

    @Override
    public Execution getExecutionOrder() {
        return Execution.valueOf(this.execution);
    }

    public String getExecution() {
        return execution;
    }

    @Override
    public void setLastActionTime(Date lastAction) {
        this.lastActionTimestamp = DateUtils.convertDateToTimestamp(lastAction);
        super.setLastActionTime(lastAction);
    }

    @Override
    public Date getLastActionTime() {
        return DateUtils.toDate(lastActionTimestamp);
    }

    public Timestamp getLastActionTimestamp() {
        return lastActionTimestamp;
    }

    @Override
    public void setNextMaterializedTime(Date nextMaterializedTime) {
        super.setNextMaterializedTime(nextMaterializedTime);
        this.nextMaterializedTimestamp = DateUtils.convertDateToTimestamp(nextMaterializedTime);
    }

    @Override
    public Date getNextMaterializedTime() {
        return DateUtils.toDate(nextMaterializedTimestamp);
    }

    public void setLastModifiedTime(Date lastModifiedTime) {
        this.lastModifiedTimestamp = DateUtils.convertDateToTimestamp(lastModifiedTime);
    }

    public Date getLastModifiedTime() {
        return DateUtils.toDate(lastModifiedTimestamp);
    }

    public void setSuspendedTime(Date suspendedTime) {
        this.suspendedTimestamp = DateUtils.convertDateToTimestamp(suspendedTime);
    }

    public Date getSuspendedTime() {
        return DateUtils.toDate(suspendedTimestamp);
    }

    @Override
    public void setStartTime(Date startTime) {
        super.setStartTime(startTime);
        this.startTimestamp = DateUtils.convertDateToTimestamp(startTime);
    }

    @Override
    public Date getStartTime() {
        return DateUtils.toDate(startTimestamp);
    }

    @Override
    public void setEndTime(Date endTime) {
        super.setEndTime(endTime);
        this.endTimestamp = DateUtils.convertDateToTimestamp(endTime);
    }

    @Override
    public void setPauseTime(Date pauseTime) {
        super.setPauseTime(pauseTime);
        this.pauseTimestamp = DateUtils.convertDateToTimestamp(pauseTime);
    }

    @Override
    public Date getEndTime() {
        return DateUtils.toDate(endTimestamp);
    }

    @Override
    public Date getPauseTime() {
        return DateUtils.toDate(pauseTimestamp);
    }

    public void setCreatedTime(Date createTime) {
        this.createdTimestamp = DateUtils.convertDateToTimestamp(createTime);
    }

    public Date getCreatedTime() {
        return DateUtils.toDate(createdTimestamp);
    }

    public Timestamp getCreatedTimestamp() {
        return createdTimestamp;
    }

    public String getAuthToken() {
        // TODO Auto-generated method stub
        return this.authToken;
    }

}
