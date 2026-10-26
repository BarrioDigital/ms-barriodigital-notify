package com.example.demo.config;

import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitMQConfig {

    @Bean
    public RabbitAdmin rabbitAdmin(ConnectionFactory connectionFactory) {
        return new RabbitAdmin(connectionFactory);
    }

    @Value("${rabbitmq.queues.email}")
    private String emailQueueName;

    @Value("${rabbitmq.queues.crew}")
    private String crewQueueName;

    @Value("${rabbitmq.queues.certificate}")
    private String certificateQueueName;

    @Value("${rabbitmq.exchanges.direct}")
    private String directExchangeName;

    @Value("${rabbitmq.exchanges.topic}")
    private String topicExchangeName;

    @Value("${rabbitmq.exchanges.dlx}")
    private String dlxExchangeName;

    @Bean
    public DirectExchange directExchange() {
        return new DirectExchange(directExchangeName, true, false);
    }

    @Bean
    public TopicExchange topicExchange() {
        return new TopicExchange(topicExchangeName, true, false);
    }

    @Bean
    public DirectExchange dlxExchange() {
        return new DirectExchange(dlxExchangeName, true, false);
    }

    @Bean
    public Queue emailQueue() {
        return QueueBuilder.durable(emailQueueName)
                .withArgument("x-dead-letter-exchange", dlxExchangeName)
                .withArgument("x-dead-letter-routing-key", "email.dead")
                .withArgument("x-max-length", 1000)
                .withArgument("x-message-ttl", 86400000)
                .build();
    }

    @Bean
    public Queue emailDlq() {
        return QueueBuilder.durable(emailQueueName + ".dlq").build();
    }

    @Bean
    public Binding emailBindingDirect(Queue emailQueue, DirectExchange directExchange) {
        return BindingBuilder.bind(emailQueue).to(directExchange).with("email.send");
    }

    @Bean
    public Binding emailBindingTopic(Queue emailQueue, TopicExchange topicExchange) {
        return BindingBuilder.bind(emailQueue).to(topicExchange).with("email.*");
    }

    @Bean
    public Binding emailDlqBinding(Queue emailDlq, DirectExchange dlxExchange) {
        return BindingBuilder.bind(emailDlq).to(dlxExchange).with("email.dead");
    }

    @Bean
    public Queue crewQueue() {
        return QueueBuilder.durable(crewQueueName)
                .withArgument("x-dead-letter-exchange", dlxExchangeName)
                .withArgument("x-dead-letter-routing-key", "crew.dead")
                .withArgument("x-max-length", 1000)
                .withArgument("x-message-ttl", 86400000)
                .build();
    }

    @Bean
    public Queue crewDlq() {
        return QueueBuilder.durable(crewQueueName + ".dlq").build();
    }

    @Bean
    public Binding crewBindingDirect(Queue crewQueue, DirectExchange directExchange) {
        return BindingBuilder.bind(crewQueue).to(directExchange).with("crew.ticket");
    }

    @Bean
    public Binding crewBindingTopic(Queue crewQueue, TopicExchange topicExchange) {
        return BindingBuilder.bind(crewQueue).to(topicExchange).with("crew.#");
    }

    @Bean
    public Binding crewDlqBinding(Queue crewDlq, DirectExchange dlxExchange) {
        return BindingBuilder.bind(crewDlq).to(dlxExchange).with("crew.dead");
    }

    @Bean
    public Queue certificateQueue() {
        return QueueBuilder.durable(certificateQueueName)
                .withArgument("x-dead-letter-exchange", dlxExchangeName)
                .withArgument("x-dead-letter-routing-key", "certificate.dead")
                .withArgument("x-max-length", 1000)
                .withArgument("x-message-ttl", 86400000)
                .build();
    }

    @Bean
    public Queue certificateDlq() {
        return QueueBuilder.durable(certificateQueueName + ".dlq").build();
    }

    @Bean
    public Binding certificateBindingDirect(Queue certificateQueue, DirectExchange directExchange) {
        return BindingBuilder.bind(certificateQueue).to(directExchange).with("certificate.gen");
    }

    @Bean
    public Binding certificateBindingTopic(Queue certificateQueue, TopicExchange topicExchange) {
        return BindingBuilder.bind(certificateQueue).to(topicExchange).with("certificate.*");
    }

    @Bean
    public Binding certificateDlqBinding(Queue certificateDlq, DirectExchange dlxExchange) {
        return BindingBuilder.bind(certificateDlq).to(dlxExchange).with("certificate.dead");
    }
}