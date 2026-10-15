function parseMessageEnvelope(msg) {
  const content = JSON.parse(msg.content.toString());
  const payload = content.data || content;
  const metadata = {
    eventId: content.eventId || 'N/A',
    traceId: content.traceId || 'N/A',
    timestamp: content.timestamp || new Date().toISOString()
  };

  return { payload, metadata };
}

async function startNotificationConsumers(channel) {
  const exchangeName = 'cmd.direct';
  await channel.assertExchange(exchangeName, 'direct', { durable: true });
  const queueName = process.env.QUEUE_EMAIL || 'q.cmd.email';
  await channel.assertQueue(queueName, { durable: true });
  await channel.bindQueue(queueName, exchangeName, 'email.send');

  channel.consume(queueName, (msg) => {
    if (msg !== null) {
      const content = JSON.parse(msg.content.toString());
      console.log('[Notify] Mensaje recibido desde RabbitMQ:', content);
      channel.ack(msg);
    }
  });
}

async function startNotificationConsumers(channel) {
  const exchangeName = 'cmd.direct';
  await channel.assertExchange(exchangeName, 'direct', { durable: true });

  const queues = [
    { name: process.env.QUEUE_EMAIL || 'q.cmd.email', routingKey: 'email.send', type: 'EMAIL / PUSH' },
    { name: process.env.QUEUE_CREW || 'q.cmd.crew', routingKey: 'crew.assign', type: 'TICKET CUADRILLA' },
    { name: process.env.QUEUE_CERTIFICATE || 'q.cmd.certificate', routingKey: 'certificate.issue', type: 'CERTIFICADO' }
  ];

  for (const q of queues) {
    await channel.assertQueue(q.name, { durable: true });
    await channel.bindQueue(q.name, exchangeName, q.routingKey);
    console.log(`[*] Escuchando cola ${q.name} con routing key [${q.routingKey}]`);

    channel.consume(q.name, (msg) => {
      if (msg !== null) {
        try {
          const { payload, metadata } = parseMessageEnvelope(msg);
          const { requestId, status, citizenId, email, address, taskDetails } = payload;

          console.log('\n==================================================');
          console.log(` SIMULACIÓN EVENTO ASÍNCRONO [${q.type}]`);
          console.log(` EventID     : ${metadata.eventId}`);
          console.log(` TraceID     : ${metadata.traceId}`);
          console.log(` Timestamp   : ${metadata.timestamp}`);
          console.log('--------------------------------------------------');
          console.log(` RequestID   : ${requestId || 'N/A'}`);
          console.log(` Status      : ${status || 'N/A'}`);
          console.log(` CitizenID   : ${citizenId || 'N/A'}`);
          
          if (q.name.includes('email')) {
            console.log(` Destino     : ${email || 'vecino@barriodigital.cl'}`);
          } else if (q.name.includes('crew')) {
            console.log(` Dirección   : ${address || 'Dirección comunal registrada'}`);
            console.log(` Tarea       : ${taskDetails || 'Inspección en terreno'}`);
          }
          console.log('==================================================\n');

          channel.ack(msg);
        } catch (error) {
          console.error(`[Consumer Error] Fallo al procesar mensaje en ${q.name}:`, error.message);
          channel.nack(msg, false, false);
        }
      }
    });
  }
}

module.exports = { startNotificationConsumers };
