package dps.adminServer;

import dps.common.model.OperationalState;
import dps.common.model.ProductionLine;
import dps.common.model.ProductionLineStatus;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.NoSuchElementException;

/** Declares a Spring Boot REST controller mapped to the path "/production-lines" **/
@RestController
@RequestMapping("/production-lines")
public class AdminServerController {

    private final ProductionLineRegistry registry;

    public AdminServerController(ProductionLineRegistry registry) {
        this.registry = registry;
    }

    /**
     * POST endpoint for registration.
     * It converts the JSON body into a ProductionLine object, adds it to the registry, and returns the list of peers (200 OK)
     * or a 409 Conflict error if the ID already exists.
     **/
    @PostMapping
    public ResponseEntity<?> register(@RequestBody ProductionLine line) {
        try {
            List<ProductionLine> peers = registry.register(line);
            return ResponseEntity.ok(peers);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.status(HttpStatus.CONFLICT).body(e.getMessage());
        }
    }

    /**
     * GET endpoint to retrieve the current status of all machines.
     **/
    @GetMapping
    public List<ProductionLineStatus> getLinesStatus() {
        return registry.getLinesStatus();
    }

    /**
     * GET endpoint to calculate the average vibration between two timestamps,
     * returning 404 Not Found if the specified ID is not found.
     **/
    @GetMapping("/{id}/stats")
    public ResponseEntity<?> getStats(
            @PathVariable int id,
            @RequestParam long t1,
            @RequestParam long t2) {
        try {
            double avg = registry.getAverageVibration(id, t1, t2);
            return ResponseEntity.ok(avg);
        } catch (NoSuchElementException e) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(e.getMessage());
        }
    }
}
