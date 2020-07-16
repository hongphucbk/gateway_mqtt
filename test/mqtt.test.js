var mqtt = require('mqtt')
var client = mqtt.connect('mqtt://127.0.0.1')
 
client.on('connect', function () {
  console.log('Client connected MQTT')
  try{
    client.subscribe("MY1")
  }catch(error){
    console.log('MQTT ERROR ' + error.message)
  }
  

  client.subscribe('presence', function (err) {
    if (!err) {
      client.publish('presence', 'Hello mqtt')
    }
  })
})
 
client.on('message', function (topic, message) {
  // message is Buffer
  console.log(message.toString())
  client.end()
})

client.on('reconnect', function () {
  // message is Buffer
  console.log('Reconnect')
  //client.end()
})

client.on('disconnect', function () {
  // message is Buffer
  console.log('Disconnect')
  //client.end()
})

client.on('end', function () {
  // message is Buffer
  console.log('end')
  client.reconnect()
})