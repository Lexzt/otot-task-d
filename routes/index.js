var express = require('express');
var router = express.Router();

var kafka = require('kafka-node');
var ConsumerGroup = kafka.ConsumerGroup;

const messages = [];
const topics =  ['my-topic-three'];

var consumerOptions = {
  kafkaHost: '127.0.0.1:9091',
  groupId: 'ExampleTestGroup',
  sessionTimeout: 15000,
  protocol: ['roundrobin'],
  fromOffset: 'earliest' 
};

const onError = (error) => (data) => {
  console.error(data);
  console.error(error);
}

function onMessage (message) {
  console.log(
    '%s read msg Topic="%s" Partition=%s Offset=%d',
    this.client.clientId,
    message.topic,
    message.partition,
    message.offset,
    message
  );

  messages.push(message.value);
}

var consumerGroup = new ConsumerGroup(Object.assign({ id: 'consumer1' }, consumerOptions), topics);
consumerGroup.on('error', onError("consumer 1"));
consumerGroup.on('message', onMessage);
consumerGroup.on('connect', function () {
  console.log('Consumer1 Started');
});

var consumerGroup2 = new ConsumerGroup(Object.assign({ id: 'consumer2' }, consumerOptions), topics);
consumerGroup2.on('error', onError("consumer 2"));
consumerGroup2.on('message', onMessage);
consumerGroup2.on('connect', function () {
  console.log('Consumer2 Started');
});

var consumerGroup3 = new ConsumerGroup(Object.assign({ id: 'consumer3' }, consumerOptions), topics);
consumerGroup3.on('error', onError("consumer 3"));
consumerGroup3.on('message', onMessage);
consumerGroup3.on('connect', function () {
  console.log('Consumer3 Started');
});

/* GET home page. */
router.get('/', function(req, res, next) {
  res.render('index', { title: 'CS3219 OTOT Task D', messages });
});

module.exports = router;
