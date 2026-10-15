package com.barriodigital.requests.config;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitMQConfig {
    public static final String EXCHANGE_NAME = "cmd.direct";
    public static final String QUEUE_EMAIL = "q.cmd.email";
    public static final String QUEUE_CREW = "q.cmd.crew";
    public static final String QUEUE_CERTIFICATE = "q.cmd.certificate";
    public static final String ROUTING_KEY_EMAIL = "email.send";
    public static final String ROUTING_KEY_CREW = "crew.assign";
    public static final String ROUTING_KEY_CERTIFICATE = "certificate.issue";

    @Bean
    public DirectExchange exchange() {
        return new DirectExchange(EXCHANGE_NAME, true, false);
    }

    @Bean
    public Queue emailQueue() {
        return new Queue(QUEUE_EMAIL, true);
    }

    @Bean
    public Queue crewQueue() {
        return new Queue(QUEUE_CREW, true);
    }

    @Bean
    public Queue certificateQueue() {
        return new Queue(QUEUE_CERTIFICATE, true);
    }

    @Bean
    public Binding bindingEmail(Queue emailQueue, DirectExchange exchange) {
        return BindingBuilder.bind(emailQueue).to(exchange).with(ROUTING_KEY_EMAIL);
    }

    @Bean
    public Binding bindingCrew(Queue crewQueue, DirectExchange exchange) {
        return BindingBuilder.bind(crewQueue).to(exchange).with(ROUTING_KEY_CREW);
    }

    @Bean
    public Binding bindingCertificate(Queue certificateQueue, DirectExchange exchange) {
        return BindingBuilder.bind(certificateQueue).to(exchange).with(ROUTING_KEY_CERTIFICATE);
    }

    @Bean
    public MessageConverter jsonMessageConverter() {
        return new Jackson2JsonMessageConverter();
    }

    @Bean
    public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory) {
        RabbitTemplate template = new RabbitTemplate(connectionFactory);
        template.setMessageConverter(jsonMessageConverter());
        return template;
    }
}
