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
package org.apache.hadoop.http.authentication.test;

import java.security.PrivilegedExceptionAction;

import javax.servlet.http.HttpServletRequest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.http.authentication.web.FileSystemEvictorCallback;
import org.apache.hadoop.http.authentication.web.ProxyUGICacheManager;
import org.apache.hadoop.security.UserGroupInformation;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ProxyUGICacheManagerTest {

    @Test
    public void testEviction() throws Exception {
        Configuration conf = new Configuration(true);
        FileSystemEvictorCallback callback = new FileSystemEvictorCallback();
        ProxyUGICacheManager manager = new ProxyUGICacheManager(20, 50, callback);
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        UserGroupInformation ugi = manager.getUGI("nobody", request);
        FileSystem fs = getFileSystem(ugi, conf);
        FileSystem fs1 = getFileSystem(ugi, conf);
        if (fs != fs1)
            Assert.fail();
        manager.removeRequest("nobody", request);
        Thread.sleep(90);
        FileSystem fs2 = getFileSystem(ugi, conf);
        if (fs == fs2)
            Assert.fail();
        manager.destroy();
    }

    private FileSystem getFileSystem(UserGroupInformation ugi, final Configuration conf) throws Exception {
        return ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
            @Override
            public FileSystem run() throws Exception {
                FileSystem fs = FileSystem.get(conf);
                return fs;
            }
        });
    }

}
