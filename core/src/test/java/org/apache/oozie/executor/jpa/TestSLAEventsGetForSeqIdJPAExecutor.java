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
package org.apache.oozie.executor.jpa;

import java.util.Date;
import java.util.List;

import org.apache.oozie.SLAEventBean;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;

public class TestSLAEventsGetForSeqIdJPAExecutor extends XDataTestCase {
    Services services;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        services.init();
        cleanUpDBTables();
    }

    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    public void testSLAEventsGetForSeqId() throws Exception {
        final String wfId = "0000000-" + new Date().getTime() + "-TestSLAEventsGetForSeqIdCommand-W";
        addRecordToSLAEventTable(wfId);
        addRecordToSLAEventTable(wfId);
        addRecordToSLAEventTable(wfId);
        _testGetSLAEventsForSeqId(wfId);
    }

    private void _testGetSLAEventsForSeqId(String jobId) throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        SLAEventsGetForSeqIdJPAExecutor slaEventsGetCmd = new SLAEventsGetForSeqIdJPAExecutor(0, 10, new long[1]);
        List<SLAEventBean> list = jpaService.execute(slaEventsGetCmd);
        assertNotNull(list);
        assertEquals(2, list.size());
    }

}