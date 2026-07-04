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

    /**
     * Queries the Admin Server for the average vibration level of a specific production line
     * between timestamp t1 and timestamp t2.
     */
    public double getAverageVibration(int id, long t1, long t2) {
        String url = serverUrl + "/production-lines/{id}/stats?t1={t1}&t2={t2}";
        try {
            Double response = restTemplate.getForObject(url, Double.class, id, t1, t2);
            if (response != null) {
                return response;
            }
        } catch (Exception e) {
            System.err.println("[ADMIN_CLIENT] ❌ Error fetching average vibration for node " + id + ": " + e.getMessage());
        }
        return 0.0;
    }

    public static void main(String[] args) {
        String serverUrl = args.length > 0 ? args[0] : "http://localhost:8080";
        AdminClientApp client = new AdminClientApp(serverUrl);
        client.startConsoleMenu();
    }

    /**
     * Starts the interactive command-line interface menu loop.
     */
    public void startConsoleMenu() {
        java.util.Scanner scanner = new java.util.Scanner(System.in);
        java.text.SimpleDateFormat dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        dateFormat.setLenient(false);

        System.out.println("=================================================");
        System.out.println("   SMARTFAB ADMINISTRATION CLIENT CONSOLE MENU   ");
        System.out.println("=================================================");

        boolean exit = false;
        while (!exit) {
            System.out.println("\nSelect an operation:");
            System.out.println("1) View registered production lines and their states");
            System.out.println("2) Get average vibration level for a specific line");
            System.out.println("3) Exit");
            System.out.print("Choice: ");

            String input = scanner.nextLine().trim();
            switch (input) {
                case "1":
                    handleViewLines();
                    break;
                case "2":
                    handleGetStats(scanner, dateFormat);
                    break;
                case "3":
                    System.out.println("Exiting Administration Client. Goodbye!");
                    exit = true;
                    break;
                default:
                    System.out.println("❌ Invalid choice. Please enter 1, 2, or 3.");
            }
        }
    }

    /**
     * Helper to fetch and print the statuses of all production lines.
     */
    private void handleViewLines() {
        System.out.println("\nRetrieving production lines status...");
        List<ProductionLineStatus> statuses = getLinesStatus();
        if (statuses.isEmpty()) {
            System.out.println("No active production lines registered in the system.");
            return;
        }
        System.out.println("+---------+-------------------------+---------------------------+");
        System.out.println("| Node ID | Network Address         | Current State             |");
        System.out.println("+---------+-------------------------+---------------------------+");
        for (ProductionLineStatus status : statuses) {
            String address = status.line().ip() + ":" + status.line().port();
            System.out.printf("| %-7d | %-23s | %-25s |\n", 
                    status.line().id(), 
                    address, 
                    status.state());
        }
        System.out.println("+---------+-------------------------+---------------------------+");
    }

    /**
     * Helper to prompt parameters and fetch average vibration stats for a node.
     */
    private void handleGetStats(java.util.Scanner scanner, java.text.SimpleDateFormat dateFormat) {
        System.out.print("Enter Production Line ID: ");
        int id;
        try {
            id = Integer.parseInt(scanner.nextLine().trim());
        } catch (NumberFormatException e) {
            System.out.println("❌ Invalid ID format. Must be an integer.");
            return;
        }

        System.out.print("Enter start date (format: yyyy-MM-dd HH:mm:ss, e.g. 2026-07-01 12:00:00): ");
        String startDateStr = scanner.nextLine().trim();
        long t1;
        try {
            t1 = dateFormat.parse(startDateStr).getTime();
        } catch (java.text.ParseException e) {
            System.out.println("❌ Invalid date format. Please use 'yyyy-MM-dd HH:mm:ss'.");
            return;
        }

        System.out.print("Enter end date (format: yyyy-MM-dd HH:mm:ss): ");
        String endDateStr = scanner.nextLine().trim();
        long t2;
        try {
            t2 = dateFormat.parse(endDateStr).getTime();
        } catch (java.text.ParseException e) {
            System.out.println("❌ Invalid date format. Please use 'yyyy-MM-dd HH:mm:ss'.");
            return;
        }

        if (t1 > t2) {
            System.out.println("❌ Start date cannot be after end date.");
            return;
        }

        System.out.println("\nQuerying average vibration statistics from server...");
        double avg = getAverageVibration(id, t1, t2);
        System.out.printf(">> Average vibration for Node %d: %.4f\n", id, avg);
    }
}
