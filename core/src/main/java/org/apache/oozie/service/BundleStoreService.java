package org.apache.oozie.service;

import org.apache.oozie.store.StoreException;
import org.apache.oozie.service.Service;
import org.apache.oozie.store.Store;
import org.apache.oozie.store.BundleStore;
import org.apache.oozie.ErrorCode;

public class BundleStoreService implements Service{
	
	public final static String TRANSIENT_VAR_PREFIX = "oozie.bundle.";
    public static final String BUNDLE_BEAN = TRANSIENT_VAR_PREFIX
            + "bundle.bean";
    
	 /**
     * Return the public interface of the service.
     *
     * @return {@link BundleStoreService}.
     */
	@Override
	public Class<? extends Service> getInterface() {
		return BundleStoreService.class;
	}

	/**
     * Return a Bundle store instance with a fresh transaction. <p/> The bundle store has to be committed and then
     * closed to commit changes, if only close it rolls back.
     *
     * @return a bundle store.
     * @throws StoreException thrown if the bundle store could not be created.
     */
    public BundleStore create() throws StoreException {
        try {
            return new BundleStore(false);
        }
        catch (Exception ex) {
            throw new StoreException(ErrorCode.E0600, ex.getMessage(), ex);
        }
    }
    
    /**
     * Return a bundle store instance with an existing transaction. <p/> The bundle store has to be committed and then
     * closed to commit changes, if only close it rolls back.
     *
     * @return a bundle store.
     * @throws StoreException thrown if the bundle store could not be created.
     */
    // to do this method can be abstract or should be overridden
    public <S extends Store> BundleStore create(S store)
            throws StoreException {
        try {
            return new BundleStore(store, false);
        }
        catch (Exception ex) {
            throw new StoreException(ErrorCode.E0600, ex.getMessage(), ex);
        }
    }
    
	@Override
	public void init(Services services) throws ServiceException {
	}
	
	@Override
	public void destroy() {		
	}	
}
