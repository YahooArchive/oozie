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
package org.apache.oozie.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.service.HadoopAccessorException;
import org.apache.oozie.service.HadoopAccessorService;

import java.io.IOException;
import java.net.URI;

/**
 * Base JUnit <code>TestCase</code> subclass used by all Oozie testcases that need Hadoop FS access. <p/> As part of its
 * setup, this testcase class creates a unique test working directory per test method in the FS. <p/> The URI of the FS
 * namenode must be specified via the {@link XTestCase#OOZIE_TEST_NAME_NODE} system property. The default value is
 * 'hdfs://localhost:9000'.
 *
 * The test working directory is created in the specified FS URI, under the current user name home directory, under the
 * subdirectory name specified wit the system property {@link XTestCase#OOZIE_TEST_DIR}. The default value is '/tmp'.
 * <p/> The path of the test working directory is: '$FS_URI/user/$USER/$OOZIE_TEST_DIR/oozietest/$TEST_CASE_CLASS/$TEST_CASE_METHOD/'
 * <p/> For example: 'hdfs://localhost:9000/user/tucu/tmp/oozietest/org.apache.oozie.service.TestELService/testEL/'
 */
public abstract class XFsTestCase extends XTestCase {
    private static HadoopAccessorService has;
    private FileSystem fileSystem;
    private Path fsTestDir;

    /**
     * Set up the testcase.
     *
     * @throws Exception thrown if the test case could no be set up.
     */
    protected void setUp() throws Exception {
        super.setUp();
        Configuration conf = new Configuration();
        conf.setBoolean("oozie.service.HadoopAccessorService.kerberos.enabled",
                        System.getProperty("oozie.test.hadoop.security", "simple").equals("kerberos"));
        conf.set("oozie.service.HadoopAccessorService.keytab.file", getKeytabFile());
        conf.set("oozie.service.HadoopAccessorService.kerberos.principal", getOoziePrincipal());
        conf.set("local.realm", getRealm());
        injectKerberosInfo(conf);

        Class hasClass;

        //TODO change this for a hardcoded instantiation when we only compile 20.100 onwards
        if (System.getProperty("hadoop20", "false").toLowerCase().equals("false")) {
            hasClass = Class.forName("org.apache.oozie.service.KerberosHadoopAccessorService");
        }
        else {
            hasClass = HadoopAccessorService.class;
        }

        has = (HadoopAccessorService) hasClass.newInstance();
        has.init(conf);
        fileSystem = has.createFileSystem(getTestUser(), getTestGroup(), new URI(getNameNodeUri()), conf);
        Path path = new Path(fileSystem.getWorkingDirectory(), getTestCaseDir().substring(1));
        fsTestDir = fileSystem.makeQualified(path);
        System.out.println(XLog.format("Setting FS testcase work dir[{0}]", fsTestDir));
        if (fileSystem.exists(fsTestDir)) {
            setAllPermissions(fileSystem, fsTestDir);
        }
        fileSystem.delete(fsTestDir, true);
        if (!fileSystem.mkdirs(path)) {
            throw new IOException(XLog.format("Could not create FS testcase dir [{0}]", fsTestDir));
        }
        fileSystem.setOwner(fsTestDir, getTestUser(), getTestGroup());
        fileSystem.setPermission(fsTestDir, FsPermission.valueOf("-rwxrwx--x"));
    }

    private void setAllPermissions(FileSystem fileSystem, Path path) throws IOException {
        FsPermission fsPermission = new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE);
        try {
            fileSystem.setPermission(path, fsPermission);
        }
        catch (IOException ex) {
            //NOP
        }
        FileStatus fileStatus = fileSystem.getFileStatus(path);
        if (fileStatus.isDir()) {
            for (FileStatus status : fileSystem.listStatus(path)) {
                setAllPermissions(fileSystem, status.getPath());
            }
        }
    }

    /**
     * Tear down the testcase.
     */
    protected void tearDown() throws Exception {
        fileSystem = null;
        fsTestDir = null;
        super.tearDown();
    }

    /**
     * Return the file system used by the tescase.
     *
     * @return the file system used by the tescase.
     */
    protected FileSystem getFileSystem() {
        return fileSystem;
    }

    /**
     * Return the FS test working directory. The directory name is the full class name of the test plus the test method
     * name.
     *
     * @return the test working directory path, it is always an full and absolute path.
     */
    protected Path getFsTestCaseDir() {
        return fsTestDir;
    }

    /**
     * Return a JobClient to the test JobTracker.
     *
     * @return a JobClient to the test JobTracker.
     * @throws HadoopAccessorException thrown if the JobClient could not be obtained.
     */
    protected JobClient createJobClient() throws HadoopAccessorException {
        JobConf conf = new JobConf();
        conf.set("mapred.job.tracker", getJobTrackerUri());
        conf.set("fs.default.name", getNameNodeUri());
        injectKerberosInfo(conf);
        return has.createJobClient(getTestUser(), getTestGroup(), conf);
    }

}
