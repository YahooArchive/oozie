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
package org.apache.oozie.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.SLAEventBean;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.rest.JsonCoordinatorAction;
import org.apache.oozie.client.rest.JsonCoordinatorJob;
import org.apache.oozie.client.rest.JsonSLAEvent;
import org.apache.oozie.client.rest.JsonWorkflowAction;
import org.apache.oozie.client.rest.JsonWorkflowJob;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.jpa.JPACommand;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.Instrumentable;
import org.apache.oozie.util.Instrumentation;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.db.ValidateConnectionBean;
import org.apache.openjpa.persistence.OpenJPAEntityManagerFactorySPI;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.Properties;

/**
 * Service that manages JPA  and executes {@link JPACommand}.
 */
public class JPAService implements Service, Instrumentable {
    private static final String INSTRUMENTATION_GROUP = "jpa";

    public static final String CONF_DB_SCHEMA = "oozie.db.schema.name";

    public static final String CONF_PREFIX = Service.CONF_PREFIX + "JPAService.";
    public static final String CONF_URL = CONF_PREFIX + "jdbc.url";
    public static final String CONF_DRIVER = CONF_PREFIX + "jdbc.driver";
    public static final String CONF_USERNAME = CONF_PREFIX + "jdbc.username";
    public static final String CONF_PASSWORD = CONF_PREFIX + "jdbc.password";
    public static final String CONF_MAX_ACTIVE_CONN = CONF_PREFIX + "pool.max.active.conn";
    public static final String CONF_CREATE_DB_SCHEMA = CONF_PREFIX + "create.db.schema";
    public static final String CONF_VALIDATE_DB_CONN = CONF_PREFIX + "validate.db.connection";

    private EntityManagerFactory factory;
    private Instrumentation instr;

    private static XLog LOG;

    /**
     * Return the public interface of the service.
     *
     * @return {@link JPAService}.
     */
    public Class<? extends Service> getInterface() {
        return JPAService.class;
    }

    @Override
    public void instrument(Instrumentation instr) {
        this.instr = instr;
    }

    /**
     * Initializes the {@link JPAService}.
     *
     * @param services services instance.
     */
    public void init(Services services) throws ServiceException {
        LOG = XLog.getLog(JPAService.class);
        Configuration conf = services.getConf();
        String dbSchema = conf.get(CONF_DB_SCHEMA, "oozie");
        String url = conf.get(CONF_URL, "jdbc:hsqldb:mem:oozie;create=true");
        String driver = conf.get(CONF_DRIVER, "org.hsqldb.jdbcDriver");
        String user = conf.get(CONF_USERNAME, "sa");
        String password = conf.get(CONF_PASSWORD, "").trim();
        String maxConn = conf.get(CONF_MAX_ACTIVE_CONN, "10").trim();
        boolean autoSchemaCreation = conf.getBoolean(CONF_CREATE_DB_SCHEMA, true);
        boolean validateDbConn = conf.getBoolean(CONF_VALIDATE_DB_CONN, false);

        if (!url.startsWith("jdbc:")) {
            throw new ServiceException(ErrorCode.E0608, url, "invalid JDBC URL, must start with 'jdbc:'");
        }
        String dbType = url.substring("jdbc:".length());
        if (dbType.indexOf(":") <= 0) {
            throw new ServiceException(ErrorCode.E0608, url, "invalid JDBC URL, missing vendor 'jdbc:[VENDOR]:...'");
        }
        dbType = dbType.substring(0, dbType.indexOf(":"));

        String persistentUnit = "oozie-" + dbType;

        //Checking existince of ORM file for DB type
        String ormFile = "META-INF/" + persistentUnit + "-orm.xml";
        try {
            IOUtils.getResourceAsStream(ormFile, -1);
        }
        catch (IOException ex) {
            throw new ServiceException(ErrorCode.E0609, dbType, ormFile);
        }

        String connProps = "DriverClassName={0},Url={1},Username={2},Password={3},MaxActive={4}";
        connProps = MessageFormat.format(connProps, driver, url, user, password, maxConn);
        Properties props = new Properties();
        if (autoSchemaCreation) {
            props.setProperty("openjpa.jdbc.SynchronizeMappings", "buildSchema(ForeignKeys=true)");
        }
        else if (validateDbConn) {
            //validation can be done only if the schema already exist, else a connection cannot
            //be obtained to create the schema.
            connProps += ",TestOnBorrow=true,TestOnReturn=false,TestWhileIdle=false";
            connProps += ",ValidationQuery=select count(*) from VALIDATE_CONN";
            connProps = MessageFormat.format(connProps, dbSchema);
        }
        props.setProperty("openjpa.ConnectionProperties", connProps);

        factory = Persistence.createEntityManagerFactory(persistentUnit, props);

        EntityManager entityManager = factory.createEntityManager();
        entityManager.find(WorkflowActionBean.class, 1);
        entityManager.find(WorkflowJobBean.class, 1);
        entityManager.find(CoordinatorActionBean.class, 1);
        entityManager.find(CoordinatorJobBean.class, 1);
        entityManager.find(JsonWorkflowAction.class, 1);
        entityManager.find(JsonWorkflowJob.class, 1);
        entityManager.find(JsonCoordinatorAction.class, 1);
        entityManager.find(JsonCoordinatorJob.class, 1);
        entityManager.find(SLAEventBean.class, 1);
        entityManager.find(JsonSLAEvent.class, 1);
        entityManager.find(ValidateConnectionBean.class, 1);

        LOG.info(XLog.STD, "All entities initialized");
        // need to use a pseudo no-op transaction so all entities, datasource
        // and connection pool are initialized
        // one time only
        entityManager.getTransaction().begin();
        entityManager.getTransaction().commit();
        entityManager.close();

        OpenJPAEntityManagerFactorySPI spi = (OpenJPAEntityManagerFactorySPI) factory;
        LOG.info("JPA configuration: {0}", spi.getConfiguration().getConnectionProperties());
    }

    /**
     * Destroy the StoreService
     */
    public void destroy() {
        factory.close();
    }

    /**
     * Execute a {@link JPACommand}.
     *
     * @param command JPACommand to execute.
     * @return return value of the JPACommand.
     * @throws CommandException
     */
    public <T> T execute(JPACommand<T> command) throws CommandException {
        EntityManager em = factory.createEntityManager();
        Instrumentation.Cron cron = new Instrumentation.Cron();
        try {
            LOG.trace("Executing JPACommand [{0}]", command.getName());
            if (instr != null) {
                instr.incr(INSTRUMENTATION_GROUP, command.getName(), 1);
            }
            cron.start();
            em.getTransaction().begin();
            T t = command.execute(em);
            if (em.getTransaction().isActive()) {
                em.getTransaction().commit();
            }
            return t;
        }
        finally {
            cron.stop();
            if (instr != null) {
                instr.addCron(INSTRUMENTATION_GROUP, command.getName(), cron);
            }
            try {
                if (em.getTransaction().isActive()) {
                    LOG.warn("JPACommand [{0}] ended with an active transaction, rolling back", command.getName());
                    em.getTransaction().rollback();
                }
            }
            catch (Exception ex) {
                LOG.warn("Could not check/rollback transaction after JPACommand [{0}], {1}",
                         command.getName(), ex.getMessage(), ex);
            }
            try {
                if (em.isOpen()) {
                    em.close();
                }
                else {
                    LOG.warn("JPACommand [{0}] closed the EntityManager, it should not!", command.getName());
                }
            }
            catch (Exception ex) {
                LOG.warn("Could not close EntityManager after JPACommand [{0}], {1}",
                         command.getName(), ex.getMessage(), ex);
            }
        }
    }

    /**
     * TODO
     * Return an EntityManager. Used by the StoreService. Once the StoreService is removed this method must be removed.
     *
     * @return an entity manager
     */
    EntityManager getEntityManager() {
        return factory.createEntityManager();
    }

}
