package sd2223.trab1.servers.rest;

import java.util.logging.Logger;

import org.glassfish.jersey.server.ResourceConfig;

import sd2223.trab1.api.java.Users;
import sd2223.trab1.servers.Domain;
import utils.Args;


public class RestUsersServer extends AbstractRestServer {
	public static final int PORT = 3456;
	
	private static Logger Log = Logger.getLogger(RestUsersServer.class.getName());

	RestUsersServer() {
		super( Log, Users.SERVICENAME, PORT);
	}
	
	
	@Override
    protected void registerResources(ResourceConfig config) {
		String secret = Args.valueOf("-secret", "EMPTY");
		config.register( new RestUsersResource(secret));
//		config.register(new GenericExceptionMapper());
//		config.register(new CustomLoggingFilter());
	}
	
	
	public static void main(String[] args) throws Exception {
		Domain.set( args[0], Long.valueOf(args[1]));
		new RestUsersServer().start();
	}	
}