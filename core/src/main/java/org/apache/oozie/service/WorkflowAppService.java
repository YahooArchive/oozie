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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.workflow.WorkflowApp;
import org.apache.oozie.workflow.WorkflowException;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XLog;
import org.apache.oozie.ErrorCode;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Service that provides application workflow definition reading from the path and creation of the proto configuration.
 */
public abstract class WorkflowAppService implements Service {

    public static final String CONF_PREFIX = Service.CONF_PREFIX + "WorkflowAppService.";

    public static final String SYSTEM_LIB_PATH = CONF_PREFIX + "system.libpath";

    public static final String APP_LIB_PATH_LIST = "oozie.wf.application.lib";

    public static final String HADOOP_UGI = "hadoop.job.ugi";

    public static final String HADOOP_USER = "user.name";

    public static final String HADOOP_JT_KERBEROS_NAME = "mapreduce.jobtracker.kerberos.principal";

    public static final String HADOOP_NN_KERBEROS_NAME = "dfs.namenode.kerberos.principal";

    private Path systemLibPath;

    /**
     * Initialize the workflow application service.
     *
     * @param services services instance.
     */
    public void init(Services services) {
        String path = services.getConf().get(SYSTEM_LIB_PATH, " ");
        if (path.trim().length() > 0) {
            systemLibPath = new Path(path.trim());
        }
    }

    /**
     * Destroy the workflow application service.
     */
    public void destroy() {
    }

    /**
     * Return the public interface for workflow application service.
     *
     * @return {@link WorkflowAppService}.
     */
    public Class<? extends Service> getInterface() {
        return WorkflowAppService.class;
    }

    /**
     * Read workflow definition.
     *
     * @param appPath application path.
     * @param user user name.
     * @param group group name.
     * @param autToken authentication token.
     * @return workflow definition.
     * @throws WorkflowException thrown if the definition could not be read.
     */
    protected String readDefinition(String appPath, String user, String group, String autToken)
            throws WorkflowException {
        try {
            URI uri = new URI(appPath);
            FileSystem fs = Services.get().get(HadoopAccessorService.class).
                    createFileSystem(user, group, uri, new Configuration());
            Reader reader = new InputStreamReader(fs.open(new Path(uri.getPath())));
            StringWriter writer = new StringWriter();
            IOUtils.copyCharStream(reader, writer);
            return writer.toString();

        }
        catch (IOException ex) {
            throw new WorkflowException(ErrorCode.E0710, ex.getMessage(), ex);
        }
        catch (URISyntaxException ex) {
            throw new WorkflowException(ErrorCode.E0711, appPath, ex.getMessage(), ex);
        }
        catch (HadoopAccessorException ex) {
            throw new WorkflowException(ex);
        }
        catch (Exception ex) {
            throw new WorkflowException(ErrorCode.E0710, ex.getMessage(), ex);
        }
    }

    /**
     * Create proto configuration. <p/> The proto configuration includes the user,group and the paths which need to be
     * added to distributed cache. These paths include .jar,.so and the resource file paths.
     *
     * @param jobConf job configuration.
     * @param authToken authentication token.
     * @param isWorkflowJob indicates if the job is a workflow job or not.
     * @return proto configuration.
     * @throws WorkflowException thrown if the proto action configuration could not be created.
     */
    public XConfiguration createProtoActionConf(Configuration jobConf, String authToken, boolean isWorkflowJob)
            throws WorkflowException {
        XConfiguration conf = new XConfiguration();
        try {
            String user = jobConf.get(OozieClient.USER_NAME);
            String group = jobConf.get(OozieClient.GROUP_NAME);
            String hadoopUgi = user + "," + group;

            conf.set(OozieClient.USER_NAME, user);
            conf.set(OozieClient.GROUP_NAME, group);
            conf.set(HADOOP_UGI, hadoopUgi);

            conf.set(HADOOP_JT_KERBEROS_NAME, jobConf.get(HADOOP_JT_KERBEROS_NAME));
            conf.set(HADOOP_NN_KERBEROS_NAME, jobConf.get(HADOOP_NN_KERBEROS_NAME));

            URI uri = new URI(jobConf.get(OozieClient.APP_PATH));

            FileSystem fs = Services.get().get(HadoopAccessorService.class).createFileSystem(user, group, uri, conf);

            Path appPath = new Path(uri.getPath());
            XLog.getLog(getClass()).debug("jobConf.libPath = " + jobConf.get(OozieClient.LIBPATH));
            XLog.getLog(getClass()).debug("jobConf.appPath = " + appPath);

            List<String> filePaths;
            if (isWorkflowJob) {
                filePaths = getLibFiles(fs, new Path(appPath.getParent(), "lib"));
            }
            else {
                filePaths = new ArrayList<String>();
            }

            String[] libPaths = jobConf.getStrings(OozieClient.LIBPATH);
            if (libPaths != null && libPaths.length > 0) {
                for (int i=0; i<libPaths.length; i++) {
                    if (libPaths[i].trim().length() > 0) {
                        Path libPath = new Path(libPaths[i].trim());
                        List<String> libFilePaths = getLibFiles(fs, libPath);
                        filePaths.addAll(libFilePaths);
                    }
                }
            }

            if (systemLibPath != null && jobConf.getBoolean(OozieClient.USE_SYSTEM_LIBPATH, false)) {
                List<String> libFilePaths = getLibFiles(fs, systemLibPath);
                filePaths.addAll(libFilePaths);
            }

            conf.setStrings(APP_LIB_PATH_LIST, filePaths.toArray(new String[filePaths.size()]));

            //Add all properties start with 'oozie.'
            for (Map.Entry<String, String> entry : jobConf) {
                if (entry.getKey().startsWith("oozie.")) {
                    String name = entry.getKey();
                    String value = entry.getValue();
                    conf.set(name, value);
                }
            }
            return conf;
        }
        catch (IOException ex) {
            throw new WorkflowException(ErrorCode.E0712, jobConf.get(OozieClient.APP_PATH), ex.getMessage(), ex);
        }
        catch (URISyntaxException ex) {
            throw new WorkflowException(ErrorCode.E0711, jobConf.get(OozieClient.APP_PATH), ex.getMessage(), ex);
        }
        catch (HadoopAccessorException ex) {
            throw new WorkflowException(ex);
        }
        catch (Exception ex) {
            throw new WorkflowException(ErrorCode.E0712, jobConf.get(OozieClient.APP_PATH),
                                        ex.getMessage(), ex);
        }
    }

    /**
     * Parse workflow definition.
     *
     * @param jobConf job configuration.
     * @param authToken authentication token.
     * @return workflow application.
     * @throws WorkflowException thrown if the workflow application could not be parsed.
     */
    public abstract WorkflowApp parseDef(Configuration jobConf, String authToken) throws WorkflowException;

    /**
     * Parse workflow definition.
     * @param wfXml workflow.
     * @return workflow application.
     * @throws WorkflowException thrown if the workflow application could not be parsed.
     */
    public abstract WorkflowApp parseDef(String wfXml) throws WorkflowException;

    /**
     * Get all library paths.
     *
     * @param fs file system object.
     * @param libPath hdfs library path.
     * @return list of paths.
     * @throws IOException thrown if the lib paths could not be obtained.
     */
    private List<String> getLibFiles(FileSystem fs, Path libPath) throws IOException {
        List<String> libPaths = new ArrayList<String>();
        if (fs.exists(libPath)) {
            FileStatus[] files = fs.listStatus(libPath, new NoPathFilter());

            for (FileStatus file : files) {
                libPaths.add(file.getPath().toUri().getPath().trim());
            }
        }
        else {
            XLog.getLog(getClass()).warn("libpath [{0}] does not exists", libPath);
        }
        return libPaths;
    }

    /*
     * Filter class doing no filtering.
     * We dont need define this class, but seems fs.listStatus() is not working properly without this.
     * So providing this dummy no filtering Filter class.
     */
    private class NoPathFilter implements PathFilter {
        @Override
        public boolean accept(Path path) {
            return true;
        }
    }
}
