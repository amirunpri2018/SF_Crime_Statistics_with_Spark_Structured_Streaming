from kafka import KafkaProducer
import json
import time
from json import loads, dumps

class ProducerServer(KafkaProducer):

    def __init__(self, input_file, topic, bootstrap_servers, client_id, **kwargs):
        super().__init__(**kwargs)
        self.input_file = input_file
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.client_id = client_id
        self.producer = KafkaProducer(
            bootstrap_servers = self.bootstrap_servers            
        )
  
    def read_file(self) -> json:
        with open(self.input_file, 'r') as f:
            data = json.load(f)
        return data
    
    def generate_data(self):
        data = self.read_file()
        for i in data:
            message = self.dict_to_binary(i)
            self.producer.send(self.topic, value=message)
            time.sleep(1)
    
    def dict_to_binary(self, json_dict):
        return dumps(json_dict).encode("utf-8")
        