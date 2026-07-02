package dps.adminClient;

import dps.common.model.ProductionLineStatus;
import org.springframework.web.client.RestTemplate;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;

/**
 * Administration Client process that communicates with the Admin Server via REST.
 */
public class AdminClientApp {

    private final String serverUrl;
    private final RestTemplate restTemplate;

    public AdminClientApp(String serverUrl) {
        this.serverUrl = serverUrl;
        this.restTemplate = new RestTemplate();
    }

    /**
     * Queries the Admin Server for the list of registered production lines and their states.
     */
    public List<ProductionLineStatus> getLinesStatus() {
        String url = serverUrl + "/production-lines";
        try {
            ProductionLineStatus[] response = restTemplate.getForObject(url, ProductionLineStatus[].class);
            if (response != null) {
                return Arrays.asList(response);
            }
        } catch (Exception e) {
            System.err.println("[ADMIN_CLIENT] ❌ Error fetching lines status: " + e.getMessage());
        }
        return new ArrayList<>();
    }

    public static void main(String[] args) {
        String serverUrl = args.length > 0 ? args[0] : "http://localhost:8080";
        AdminClientApp client = new AdminClientApp(serverUrl);
        
        System.out.println("[ADMIN_CLIENT] Booting client...");
        List<ProductionLineStatus> statuses = client.getLinesStatus();
        System.out.println("[ADMIN_CLIENT] Found " + statuses.size() + " active node(s) registered on Admin Server.");
    }
}
