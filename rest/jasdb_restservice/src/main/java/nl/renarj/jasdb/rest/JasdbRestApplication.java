package nl.renarj.jasdb.rest;

import nl.renarj.jasdb.rest.providers.JasdbInfoService;

import javax.ws.rs.core.Application;
import java.util.HashSet;
import java.util.Set;

public class JasdbRestApplication extends Application {

	@Override
	public Set<Class<?>> getClasses() {
		Set<Class<?>> resources = new HashSet<Class<?>>();
		resources.add(JasdbInfoService.class);
		
		return resources;
	}

}
