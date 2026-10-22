package com.example.demo.controller;

import com.example.demo.service.RabbitAdminService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/queues")
public class RabbitAdminController {

    @Autowired
    private RabbitAdminService adminService;

    @PostMapping
    public ResponseEntity<Map<String, String>> createQueue(@RequestBody Map<String, Object> body) {
        String name = (String) body.get("name");
        Boolean durable = body.get("durable") != null ? (Boolean) body.get("durable") : true;
        String dlxExchange = (String) body.get("dlxExchange");
        String dlxRoutingKey = (String) body.get("dlxRoutingKey");

        adminService.createQueue(name, durable, dlxExchange, dlxRoutingKey);

        Map<String, String> response = new HashMap<>();
        response.put("message", "Cola '" + name + "' creada exitosamente.");
        return ResponseEntity.ok(response);
    }

    @DeleteMapping("/{name}")
    public ResponseEntity<Map<String, String>> deleteQueue(@PathVariable String name) {
        boolean deleted = adminService.deleteQueue(name);
        Map<String, String> response = new HashMap<>();
        if (deleted) {
            response.put("message", "Cola '" + name + "' eliminada exitosamente.");
            return ResponseEntity.ok(response);
        } else {
            response.put("message", "No se encontró la cola '" + name + "'.");
            return ResponseEntity.badRequest().body(response);
        }
    }
}