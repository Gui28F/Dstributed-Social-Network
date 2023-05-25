package sd2223.trab1.servers.rest;

import java.net.URI;
import java.security.NoSuchAlgorithmException;
import java.util.logging.Logger;

import org.glassfish.jersey.jdkhttp.JdkHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import sd2223.trab1.discovery.Discovery;
import sd2223.trab1.servers.java.AbstractServer;
import utils.IP;

import javax.net.ssl.SSLContext;


public abstract class AbstractRestServer extends AbstractServer {

    private static final String REST_CTX = "/rest";

    protected AbstractRestServer(Logger log, String service, int port) {
        super(log, service, String.format(SERVER_BASE_URI, IP.hostAddress(), port, REST_CTX));
    }


    protected void start() {
        try {
            ResourceConfig config = new ResourceConfig();
            registerResources(config);
            JdkHttpServerFactory.createHttpServer(URI.create(serverURI.replace(IP.hostAddress(), INETADDR_ANY)), config, SSLContext.getDefault());
            Discovery.getInstance().announce(service, super.serverURI);
            Log.info(String.format("%s Server ready @ %s\n", service, serverURI));
        } catch (Exception e) {
            Log.severe(e.getMessage());
        }
    }

    protected abstract void registerResources(ResourceConfig config);
}
