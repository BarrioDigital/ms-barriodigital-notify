package com.example.demo.consumer;

import com.example.demo.model.Notification;
import com.example.demo.repository.NotificationRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class NotificationConsumers {

    @Autowired
    private NotificationRepository notificationRepository;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @RabbitListener(queues = "${rabbitmq.queues.email}")
    public void consumeEmailMessage(String message, Channel channel, @Header(AmqpHeaders.DELIVERY_TAG) long tag) throws IOException {
        try {
            System.out.println("[ms-notify] [EMAIL] Procesando notificación: " + message);
            if (message != null && message.contains("REJECT_TEST")) {
                throw new IllegalArgumentException("Error no recuperable forzado");
            }

            saveNotificationFromMessage(message);

            channel.basicAck(tag, false);
            System.out.println("[ms-notify] [EMAIL] ACK confirmado para tag: " + tag);
        } catch (IllegalArgumentException e) {
            System.err.println("[ms-notify] [EMAIL] Error no recuperable. Enviando a DLQ: " + e.getMessage());
            channel.basicNack(tag, false, false);
        } catch (Exception e) {
            System.err.println("[ms-notify] [EMAIL] Error al guardar/procesar mensaje: " + e.getMessage());
            channel.basicNack(tag, false, false);
        }
    }

    @RabbitListener(queues = "${rabbitmq.queues.crew}")
    public void consumeCrewMessage(String message, Channel channel, @Header(AmqpHeaders.DELIVERY_TAG) long tag) throws IOException {
        try {
            System.out.println("[ms-notify] [CREW] Generando orden de trabajo en terreno: " + message);
            if (message != null && message.contains("REJECT_TEST")) {
                throw new IllegalArgumentException("Error no recuperable forzado");
            }

            // Guardar notificación del evento de asignación a cuadrilla en la base de datos
            saveNotificationFromMessage(message);

            channel.basicAck(tag, false);
            System.out.println("[ms-notify] [CREW] ACK confirmado para tag: " + tag);
        } catch (Exception e) {
            System.err.println("[ms-notify] [CREW] Error al procesar mensaje de cuadrilla: " + e.getMessage());
            channel.basicNack(tag, false, false);
        }
    }

    @RabbitListener(queues = "${rabbitmq.queues.certificate}")
    public void consumeCertificateMessage(String message, Channel channel, @Header(AmqpHeaders.DELIVERY_TAG) long tag) throws IOException {
        try {
            System.out.println("[ms-notify] [CERTIFICATE] Generando comprobante PDF: " + message);
            if (message != null && message.contains("REJECT_TEST")) {
                throw new IllegalArgumentException("Error no recuperable forzado");
            }

            saveNotificationFromMessage(message);

            channel.basicAck(tag, false);
            System.out.println("[ms-notify] [CERTIFICATE] ACK confirmado para tag: " + tag);
        } catch (Exception e) {
            System.err.println("[ms-notify] [CERTIFICATE] Error procesando PDF: " + e.getMessage());
            channel.basicNack(tag, false, false);
        }
    }

    private void saveNotificationFromMessage(String message) {
        try {
            Long requestId = null;
            String citizenId = "N/A";
            String status = "INFO";
            String taskDetails = message;

            // Intento 1: Parsear como JSON estándar
            try {
                JsonNode jsonNode = objectMapper.readTree(message);
                JsonNode dataNode = jsonNode.has("data") ? jsonNode.get("data") : jsonNode;

                if (dataNode.has("requestId")) requestId = dataNode.get("requestId").asLong();
                if (dataNode.has("citizenId")) citizenId = dataNode.get("citizenId").asText();
                if (dataNode.has("status")) status = dataNode.get("status").asText();
                if (dataNode.has("taskDetails")) taskDetails = dataNode.get("taskDetails").asText();
            } catch (Exception jsonEx) {
                // Intento 2: Parsear mapa de texto Java tipo {key=value, key2=value2}
                Map<String, String> parsedMap = parseJavaMapString(message);
                if (parsedMap.containsKey("requestId")) {
                    try { requestId = Long.parseLong(parsedMap.get("requestId")); } catch (Exception ignored) {}
                }
                if (parsedMap.containsKey("citizenId")) citizenId = parsedMap.get("citizenId");
                if (parsedMap.containsKey("status")) status = parsedMap.get("status");
                if (parsedMap.containsKey("taskDetails")) taskDetails = parsedMap.get("taskDetails");
            }

            Notification notification = Notification.builder()
                    .requestId(requestId)
                    .citizenId(citizenId)
                    .status(status)
                    .message(taskDetails)
                    .createdAt(LocalDateTime.now())
                    .build();

            notificationRepository.save(notification);
            System.out.println("[ms-notify] Notificación guardada exitosamente en BD para RUT: " + citizenId);
        } catch (Exception e) {
            System.err.println("[ms-notify] Error grave al procesar mensaje: " + e.getMessage());
        }
    }

    private Map<String, String> parseJavaMapString(String input) {
        Map<String, String> map = new HashMap<>();
        Pattern pattern = Pattern.compile("([a-zA-Z0-9]+)=([^,}]+)");
        Matcher matcher = pattern.matcher(input);
        while (matcher.find()) {
            map.put(matcher.group(1).trim(), matcher.group(2).trim());
        }
        return map;
    }

    // Consumidores DLQ
    @RabbitListener(queues = "${rabbitmq.queues.email}.dlq")
    public void consumeEmailDlq(String message, Channel channel, @Header(AmqpHeaders.DELIVERY_TAG) long tag) throws IOException {
        System.err.println("[DLQ AUDIT] [EMAIL] Mensaje en DLQ: " + message);
        channel.basicAck(tag, false);
    }

    @RabbitListener(queues = "${rabbitmq.queues.crew}.dlq")
    public void consumeCrewDlq(String message, Channel channel, @Header(AmqpHeaders.DELIVERY_TAG) long tag) throws IOException {
        System.err.println("[DLQ AUDIT] [CREW] Mensaje en DLQ: " + message);
        channel.basicAck(tag, false);
    }

    @RabbitListener(queues = "${rabbitmq.queues.certificate}.dlq")
    public void consumeCertificateDlq(String message, Channel channel, @Header(AmqpHeaders.DELIVERY_TAG) long tag) throws IOException {
        System.err.println("[DLQ AUDIT] [CERTIFICATE] Mensaje en DLQ: " + message);
        channel.basicAck(tag, false);
    }
}