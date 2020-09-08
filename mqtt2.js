require('dotenv').config()

var mqtt = require('mqtt')
var client = mqtt.connect(process.env.MQTT_URL)

var client1 = mqtt.connect('mqtt://127.0.0.1:1888')

client.on('connect', function () {
  console.log('Client connected MQTT')
  try{
    client.subscribe("Flexy/Data1")
  }catch(error){
    console.log('MQTT ERROR ' + error.message)
  }
  

  
})
 
client.on('message', function (topic, message) {
  // message is Buffer
  let a = message.toString();

  let b = JSON.parse(a)
  let c = JSON.stringify(b)
  console.log(c)
  client1.publish("Flexy/Data1", c)
  
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
  
})