package dps.adminServer;

import dps.common.model.OperationalState;
import dps.common.model.ProductionLine;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/production-lines")
public class AdminServerController {

    private final ProductionLineRegistry registry;

    public AdminServerController(ProductionLineRegistry registry) {
        this.registry = registry;
    }

    @PostMapping
    public ResponseEntity<?> register(@RequestBody ProductionLine line) {
        try {
            List<ProductionLine> peers = registry.register(line);
            return ResponseEntity.ok(peers);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.status(HttpStatus.CONFLICT).body(e.getMessage());
        }
    }

    @GetMapping
    public List<ProductionLine> getLines() {
        return registry.getAllLines();
    }

    @GetMapping("/states")
    public Map<Integer, OperationalState> getStates() {
        return registry.getCurrentStates();
    }

    @GetMapping("/{id}/stats")
    public ResponseEntity<Double> getStats(
            @PathVariable int id,
            @RequestParam long t1,
            @RequestParam long t2) {
        double avg = registry.getAverageVibration(id, t1, t2);
        return ResponseEntity.ok(avg);
    }
}
