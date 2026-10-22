package com.example.demo.service;

import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class RabbitAdminService {

    @Autowired
    private RabbitAdmin rabbitAdmin;

    public void createQueue(String queueName, boolean durable, String dlxExchange, String dlxRoutingKey) {
        if (queueName == null || queueName.trim().isEmpty()) {
            throw new IllegalArgumentException("El nombre de la cola no puede estar vacío.");
        }
        QueueBuilder builder = durable ? QueueBuilder.durable(queueName) : QueueBuilder.nonDurable(queueName);
        if (dlxExchange != null && !dlxExchange.trim().isEmpty()) {
            builder.withArgument("x-dead-letter-exchange", dlxExchange);
        }
        if (dlxRoutingKey != null && !dlxRoutingKey.trim().isEmpty()) {
            builder.withArgument("x-dead-letter-routing-key", dlxRoutingKey);
        }
        rabbitAdmin.declareQueue(builder.build());
    }

    public boolean deleteQueue(String queueName) {
        if (queueName == null || queueName.trim().isEmpty()) {
            throw new IllegalArgumentException("El nombre de la cola no puede estar vacío.");
        }
        return rabbitAdmin.deleteQueue(queueName);
    }
}