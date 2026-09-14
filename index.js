const express = require('express');
const amqp = require('amqplib');

const app = express();
app.use(express.json());

const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://localhost';
const QUEUE_NAME = 'cola_notificaciones_barrio';

app.post('/api/reportar-incidente', async (req, res) => {
  const { usuarioId, tipoIncidente, ubicacion, descripcion } = req.body;

  try {
    const incidenteId = 'INC-' + Math.floor(Math.random() * 10000);
    const connection = await amqp.connect(RABBITMQ_URL);
    const channel = await connection.createChannel();
    await channel.assertQueue(QUEUE_NAME, { durable: true });

    const mensaje = {
      incidenteId,
      usuarioId,
      tipoIncidente,
      ubicacion,
      descripcion,
      fecha: new Date().toISOString()
    };

    channel.sendToQueue(QUEUE_NAME, Buffer.from(JSON.stringify(mensaje)), { persistent: true });
    console.log(`[BFF] 🚀 Reporte #${incidenteId} enviado a RabbitMQ`);

    await channel.close();
    await connection.close();

    return res.status(201).json({ exito: true, mensaje: 'Reporte registrado', incidenteId });
  } catch (error) {
    console.error('[BFF] Error:', error);
    return res.status(500).json({ exito: false, error: 'Error interno en el BFF' });
  }
});

app.listen(3000, () => console.log('[BFF] Listo en http://localhost:3000'));