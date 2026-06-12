package dps.adminServer;

import dps.common.model.OperationalState;
import dps.common.model.ProductionLine;
import dps.common.model.ProductionLineStatus;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.NoSuchElementException;

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
    public List<ProductionLineStatus> getLinesStatus() {
        return registry.getLinesStatus();
    }

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
