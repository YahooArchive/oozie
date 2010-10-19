package org.apache.oozie.command.bundle;

import org.apache.oozie.command.Command;
import org.apache.oozie.store.BundleStore;
import org.apache.oozie.store.Store;

public abstract class BundleCommand<T> extends Command <T, BundleStore>{
	
    public BundleCommand(String name, String type, int priority, int logMask) {
        super(name, type, priority, logMask);
    }

    public BundleCommand(String name, String type, int priority, int logMask,
                              boolean dryrun) {
        super(name, type, priority, logMask, (dryrun) ? false : true, dryrun);
    }

    /**
     * Return the public interface of the Bundle Store.
     *
     * @return {@link BundleStore}
     */
    public Class<? extends Store> getStoreClass() {
        return BundleStore.class;
    }
}
