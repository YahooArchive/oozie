package org.apache.oozie.store;

import java.util.Date;

import org.apache.oozie.BundleJobBean;
import org.apache.oozie.client.BundleJob;
import org.apache.oozie.service.BundleStoreService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XTestCase;

public class TestBundleStore extends XTestCase {
    Services services;
    BundleStore store;
    BundleJobBean bundleBean;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        cleanUpDB(services.getConf());
        services.init();
        store = Services.get().get(BundleStoreService.class).create();
    }

    @Override
    protected void tearDown() throws Exception {
        // dropSchema(dbName, conn);
        services.destroy();
        super.tearDown();
    }

    public void testBundleStore() throws StoreException {
        String jobId = "00000-" + new Date().getTime() + "-TestBundleStore-C";
        try {
            _testInsertJob(jobId);
            _testGetJob(jobId);
        }
        finally {
            // store.closeTrx();
        }
    }

    private void _testInsertJob(String jobId) throws StoreException {
        BundleJobBean job = createBundleJob(jobId);
        store.beginTrx();
        try {
            store.insertBundleJob(job);
            store.commitTrx();
        }
        catch (Exception ex) {
            store.rollbackTrx();
            ex.printStackTrace();
            fail("Unable to insert a record into Bundle Job ");
        }
    }

    private BundleJobBean createBundleJob(String jobId) {
        BundleJobBean bundleJob = new BundleJobBean();
        bundleJob.setId(jobId);
        bundleJob.setBundleName("testBundleName");
        bundleJob.setBundlePath("testBundlePath");
        bundleJob.setStatus(BundleJob.Status.PREP);
        bundleJob.setCreatedTime(new Date());
        bundleJob.setUser("testUser");
        bundleJob.setGroup("testGroup");
        String confStr = "<configuration></configuration>";
        bundleJob.setConf(confStr);
        String appXml = "<bundle-app name='NAME' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' xmlns='uri:oozie:bundle:0.1'>";
        appXml += "<controls>";
        appXml += "<kick-off-time>2009-02-02T00:00Z</kick-off-time>";
        appXml += "<coordinator>";
        appXml += "<app-path>hdfs://localhost:9001/tmp/examples/coordinator/coordinator.xml</app-path>";
        appXml += "<configuration>";
        appXml += "<property>";
        appXml += "<name>START_TIME</name> ";
        appXml += "<value>2009-02-01T00:00Z</value>";
        appXml += "</property>";
        appXml += "</configuration>";
        appXml += "</coordinator>";
        appXml += "</bundle-app>";
        bundleJob.setJobXml(appXml);
        Date curr = new Date();
        bundleJob.setLastModifiedTime(curr);
        bundleJob.setEndTime(new Date(curr.getTime() + 86400000));
        bundleJob.setKickoffTime(new Date(curr.getTime() - 86400000));
        return bundleJob;
    }

    private void _testGetJob(String jobId) throws StoreException {
        store.beginTrx();
        try {
            BundleJobBean job = store.getBundleJob(jobId, false);
            assertEquals(jobId, job.getId());
            assertEquals(job.getStatus(), BundleJob.Status.PREP);
            store.commitTrx();
        }
        catch (Exception ex) {
            store.rollbackTrx();
            ex.printStackTrace();
            fail("Unable to GET a record for Bundle Job. jobId =" + jobId);
        }
    }
}
