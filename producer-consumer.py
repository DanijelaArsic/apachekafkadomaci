from kafka import KafkaConsumer, KafkaProducer
import requests, json

bootstrap_servers = 'localhost:9092'
input_topic = 'topic1'
output_topic = 'topic2'

consumer = KafkaConsumer(input_topic,bootstrap_servers=bootstrap_servers,value_deserializer=lambda x: json.loads(x.decode('utf-8')))
producer = KafkaProducer(bootstrap_servers=bootstrap_servers,value_serializer=lambda x: json.dumps(x).encode('utf-8'))


for message in consumer:

    json_data = message.value
    
   
    kelvin = float(json_data['main']['temp']) 
    celsius = kelvin - 273.15
    

    processed_data = {
       
        'temperature_celsius': round(temperature_celsius,2),
        'dt': json_data.get('dt','error no date')

    }

    producer.send(output_topic, value=processed_data)
    producer.flush()


consumer.close()
producer.close()