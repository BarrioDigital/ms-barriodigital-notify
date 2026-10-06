const express = require('express');
const { connectRabbitMQ } = require('./src/config/rabbitmq');
const { startNotificationConsumers } = require('./src/consumers/notificationConsumer');
const app = express();
app.use(express.json());
const PORT = process.env.PORT || 8083;
app.get('/health', (req, res) => {
  res.status(200).json({ status: 'OK', service: 'ms-barriodigital-notify' });
});

async function main() {
  try {
    const { connection, channel } = await connectRabbitMQ();

    await startNotificationConsumers(channel);

    app.listen(PORT, () => {
      console.log(`[ms-barriodigital-notify] Servidor iniciado en puerto ${PORT}`);
    });
  } catch (error) {
    console.error('[ms-barriodigital-notify] Error crítico al iniciar:', error);
    process.exit(1);
  }
}

main();