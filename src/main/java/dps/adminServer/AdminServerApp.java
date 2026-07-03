package dps.adminServer;

import dps.adminServer.service.ProductionLineRegistry;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class AdminServerApp {

    public static void main(String[] args) {
        SpringApplication.run(AdminServerApp.class, args);
    }

    /** Declare the registry as a Singleton managed by Spring,
     * so that it can be injected into each (REST) AdminServerController constructor.
     * **/
    @Bean // Bean = a system object created, managed, and held in memory by Spring itself.
    public ProductionLineRegistry productionLineRegistry() {
        return new ProductionLineRegistry();
    }
}
