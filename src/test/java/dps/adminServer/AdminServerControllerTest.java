package dps.adminServer;

import com.fasterxml.jackson.databind.ObjectMapper;
import dps.common.model.ProductionLine;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
import static org.hamcrest.Matchers.*;

@SpringBootTest
@AutoConfigureMockMvc
public class AdminServerControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    public void testRegisterAndGetLines() throws Exception {
        ProductionLine line1 = new ProductionLine(1, "127.0.0.1", 5001);
        ProductionLine line2 = new ProductionLine(2, "127.0.0.1", 5002);

        // Register first line
        mockMvc.perform(post("/production-lines")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(line1)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(0)));

        // Register second line, should return first line as peer
        mockMvc.perform(post("/production-lines")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(line2)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(1)))
                .andExpect(jsonPath("$[0].id", is(1)));

        // Get all lines
        mockMvc.perform(get("/production-lines"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(2)));
    }

    @Test
    public void testDuplicateRegistration() throws Exception {
        ProductionLine line3 = new ProductionLine(3, "127.0.0.1", 5003);

        mockMvc.perform(post("/production-lines")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(line3)))
                .andExpect(status().isOk());

        mockMvc.perform(post("/production-lines")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(line3)))
                .andExpect(status().isConflict());
    }
}
