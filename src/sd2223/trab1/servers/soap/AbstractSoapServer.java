package sd2223.trab1.servers.soap;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;
import jakarta.xml.ws.Endpoint;
import sd2223.trab1.discovery.Discovery;
import sd2223.trab1.servers.java.AbstractServer;
import utils.IP;

import javax.net.ssl.SSLContext;

public class AbstractSoapServer<T> extends AbstractServer {
    private static final String SOAP_CTX = "/soap";
    private HttpsServer server;
    final T webservice;

    protected AbstractSoapServer(boolean enableSoapDebug, Logger log, String service, int port, T webservice) {
        super(log, service, String.format(SERVER_BASE_URI, IP.hostAddress(), port, SOAP_CTX));
        this.webservice = webservice;
        try {
            this.server = HttpsServer.create(new InetSocketAddress(IP.hostAddress(), port), 0);
        } catch (Exception e) {
            Log.severe(e.getMessage());
        }
        if (enableSoapDebug) {
            System.setProperty("com.sun.xml.ws.transport.http.client.HttpTransportPipe.dump", "true");
            System.setProperty("com.sun.xml.internal.ws.transport.http.client.HttpTransportPipe.dump", "true");
            System.setProperty("com.sun.xml.ws.transport.http.HttpAdapter.dump", "true");
            System.setProperty("com.sun.xml.internal.ws.transport.http.HttpAdapter.dump", "true");
        }
    }

    protected void start() {
        try {
            server.setExecutor(Executors.newCachedThreadPool());
            server.setHttpsConfigurator(new HttpsConfigurator(SSLContext.getDefault()));
            Endpoint.publish(serverURI.replace(IP.hostAddress(), INETADDR_ANY), webservice);
            Discovery.getInstance().announce(service, serverURI);
            Log.info(String.format("%s Soap Server ready @ %s\n", service, serverURI));
        } catch (Exception e) {
            Log.severe(e.getMessage());
        }
    }
}
