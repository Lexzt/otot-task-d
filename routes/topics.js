var express = require('express');
var kafka = require('kafka-node');

var router = express.Router();

var Producer = kafka.Producer;
var Client = kafka.KafkaClient;
var client = new Client({ kafkaHost: "localhost:9091" });
var topic = 'my-topic-three';

var producer = new Producer(client, { requireAcks: 1 });
var canSend = false;
producer.on('ready', function () {
  canSend = true;

  
});
producer.on('error', function (err) {
  console.log('error', err);
});


/* POST topics to kafka. */
router.post('/post', async (req, res) => {
  if (!canSend){
    res.sendStatus(500);
  }

  if (!req.body.message) {
    res.sendStatus(500);
  }

  await client.refreshMetadata(
    [topic],
    async (err) => {
      if (err) {
        console.error(err);
        throw err;
      }

      console.log(`Sending message to ${topic}: ${req.body.message}`);
      await producer.send(
        [{ topic, messages: [req.body.message] }],
        (err, result) => {
          console.log(err || result);
        }
      );
    }
  );
  
  // await producer.send([{ topic: topic, messages: [req.body.message] }], function (
  //   err,
  //   result
  // ) {
  //   console.log(err || result);
  // });
  
  res.sendStatus(200);
});

module.exports = router;
