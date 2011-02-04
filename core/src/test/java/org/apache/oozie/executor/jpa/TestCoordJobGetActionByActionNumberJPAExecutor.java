package org.apache.oozie.executor.jpa;

import java.util.List;

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.local.LocalOozie;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;

public class TestCoordJobGetActionByActionNumberJPAExecutor extends XDataTestCase {
    Services services;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        services.init();
        cleanUpDBTables();
        LocalOozie.start();
    }

    @Override
    protected void tearDown() throws Exception {
        LocalOozie.stop();
        services.destroy();
        super.tearDown();
    }


    public void testCoordActionsGetByActionNumber() throws Exception {
        int actionNum = 1;
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING);
        CoordinatorActionBean action = addRecordToCoordActionTable(job.getId(), actionNum, CoordinatorAction.Status.WAITING, "coord-action-get.xml");
        _testCoordActionsGetByActionNumber(job, action);
    }

    private void _testCoordActionsGetByActionNumber(CoordinatorJobBean job, CoordinatorActionBean action) throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        CoordJobGetActionByActionNumberJPAExecutor coordGetCmd = new CoordJobGetActionByActionNumberJPAExecutor(job.getId(), action.getActionNumber());
        CoordinatorActionBean ret = jpaService.execute(coordGetCmd);
        assertNotNull(ret);
        assertEquals(ret.getId(), action.getId());
    }

}
