package dps.adminServer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class AdminServerApp {

    public static void main(String[] args) {
        SpringApplication.run(AdminServerApp.class, args);
    }

    @Bean
    public ProductionLineRegistry productionLineRegistry() {
        return new ProductionLineRegistry();
    }
}
