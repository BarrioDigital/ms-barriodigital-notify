package com.example.demo.consumer;

import com.rabbitmq.client.Channel;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class NotificationConsumers {

    // =======================================================
    // CONSUMIDORES PRINCIPALES DE NOTIFICACIONES
    // =======================================================

    @RabbitListener(queues = "${rabbitmq.queues.email}")
    public void consumeEmailMessage(String message, Channel channel, @Header(AmqpHeaders.DELIVERY_TAG) long tag) throws IOException {
        try {
            System.out.println("[ms-notify] [EMAIL] Procesando notificación: " + message);
            if (message != null && message.contains("REJECT_TEST")) {
                throw new IllegalArgumentException("Error no recuperable forzado");
            }
            channel.basicAck(tag, false);
            System.out.println("[ms-notify] [EMAIL] ACK confirmado para tag: " + tag);
        } catch (IllegalArgumentException e) {
            System.err.println("[ms-notify] [EMAIL] Error no recuperable. Enviando a DLQ: " + e.getMessage());
            channel.basicNack(tag, false, false);
        } catch (Exception e) {
            System.err.println("[ms-notify] [EMAIL] Error recuperable. Rechazando mensaje: " + e.getMessage());
            channel.basicNack(tag, false, false);
        }
    }

    @RabbitListener(queues = "${rabbitmq.queues.crew}")
    public void consumeCrewMessage(String message, Channel channel, @Header(AmqpHeaders.DELIVERY_TAG) long tag) throws IOException {
        try {
            System.out.println("[ms-notify] [CREW] Generando ticket de visita: " + message);
            if (message != null && message.contains("REJECT_TEST")) {
                throw new IllegalArgumentException("Error no recuperable forzado");
            }
            channel.basicAck(tag, false);
            System.out.println("[ms-notify] [CREW] ACK confirmado para tag: " + tag);
        } catch (IllegalArgumentException e) {
            System.err.println("[ms-notify] [CREW] Error no recuperable. Enviando a DLQ: " + e.getMessage());
            channel.basicNack(tag, false, false);
        } catch (Exception e) {
            System.err.println("[ms-notify] [CREW] Error al procesar ticket. Enviando a DLQ: " + e.getMessage());
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
            channel.basicAck(tag, false);
            System.out.println("[ms-notify] [CERTIFICATE] ACK confirmado para tag: " + tag);
        } catch (IllegalArgumentException e) {
            System.err.println("[ms-notify] [CERTIFICATE] Error no recuperable. Enviando a DLQ: " + e.getMessage());
            channel.basicNack(tag, false, false);
        } catch (Exception e) {
            System.err.println("[ms-notify] [CERTIFICATE] Error procesando PDF. Enviando a DLQ: " + e.getMessage());
            channel.basicNack(tag, false, false);
        }
    }

    // =======================================================
    // CONSUMIDORES DE DEAD LETTER QUEUES (DLQ)
    // =======================================================

    @RabbitListener(queues = "${rabbitmq.queues.email}.dlq")
    public void consumeEmailDlq(String message, Channel channel, @Header(AmqpHeaders.DELIVERY_TAG) long tag) throws IOException {
        try {
            System.err.println("[DLQ AUDIT] [EMAIL] Mensaje no entregado procesado desde DLQ: " + message);
            channel.basicAck(tag, false);
        } catch (Exception e) {
            System.err.println("[DLQ ERROR] Error al registrar log de la DLQ Email: " + e.getMessage());
            channel.basicNack(tag, false, false);
        }
    }

    @RabbitListener(queues = "${rabbitmq.queues.crew}.dlq")
    public void consumeCrewDlq(String message, Channel channel, @Header(AmqpHeaders.DELIVERY_TAG) long tag) throws IOException {
        try {
            System.err.println("[DLQ AUDIT] [CREW] Mensaje no entregado procesado desde DLQ: " + message);
            channel.basicAck(tag, false);
        } catch (Exception e) {
            System.err.println("[DLQ ERROR] Error al registrar log de la DLQ Crew: " + e.getMessage());
            channel.basicNack(tag, false, false);
        }
    }

    @RabbitListener(queues = "${rabbitmq.queues.certificate}.dlq")
    public void consumeCertificateDlq(String message, Channel channel, @Header(AmqpHeaders.DELIVERY_TAG) long tag) throws IOException {
        try {
            System.err.println("[DLQ AUDIT] [CERTIFICATE] Mensaje no entregado procesado desde DLQ: " + message);
            channel.basicAck(tag, false);
        } catch (Exception e) {
            System.err.println("[DLQ ERROR] Error al registrar log de la DLQ Certificate: " + e.getMessage());
            channel.basicNack(tag, false, false);
        }
    }
}