const amqp = require('amqplib');

let connection = null;
let channel = null;

async function connectRabbitMQ() {
  const rabbitUrl = process.env.RABBITMQ_URL || 'amqp://guest:guest@localhost:5672';
  try {
    connection = await amqp.connect(rabbitUrl);
    channel = await connection.createChannel();
    console.log('[RabbitMQ] Conexión establecida exitosamente con el broker.');
    return { connection, channel };
  } catch (error) {
    console.error('[RabbitMQ] Error de conexión:', error.message);
    console.log('[RabbitMQ] Reintentando conexión en 5 segundos...');
    await new Promise((resolve) => setTimeout(resolve, 5000));
    return connectRabbitMQ();
  }
}

module.exports = { connectRabbitMQ };