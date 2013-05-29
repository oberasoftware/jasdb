package nl.renarj.jasdb.core.dummy;

import nl.renarj.jasdb.core.RemoteService;
import nl.renarj.jasdb.core.exceptions.ServiceException;
import nl.renarj.jasdb.core.locator.ServiceInformation;

import java.util.HashMap;

public class DummyRemoteService implements RemoteService {

	@Override
	public void startService() throws ServiceException {
	}

	@Override
	public void stopService() throws ServiceException {
	}

    @Override
    public ServiceInformation getServiceInformation() {
        return new ServiceInformation("none", new HashMap<String, String>());
    }
}
