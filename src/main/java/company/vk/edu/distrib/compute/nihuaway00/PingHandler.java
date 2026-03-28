package company.vk.edu.distrib.compute.nihuaway00;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.io.OutputStream;

public class PingHandler implements HttpHandler {

    private final EntityDao entityDao;

    PingHandler(EntityDao dao) {
        this.entityDao = dao;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        var daoAvailable = entityDao.available();

        if (daoAvailable) {
            exchange.sendResponseHeaders(200, 0);
            OutputStream os = exchange.getResponseBody();
            os.write("{\"status\": \"ok\"}".getBytes());
            os.close();
            exchange.close();
        } else {
            exchange.sendResponseHeaders(503, 0);
            OutputStream os = exchange.getResponseBody();
            os.write("{\"status\": \"not available\", \"desc\":\"entity dao not available\"}".getBytes());
            os.close();
            exchange.close();
        }

    }
}
