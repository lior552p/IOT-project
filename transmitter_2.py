from awscrt import io, mqtt, auth, http
from awsiot import mqtt_connection_builder
import time as t
import json
import random


ENDPOINT = "a24j1oip4brp38-ats.iot.eu-central-1.amazonaws.com"
CLIENT_ID = "iotconsole-liorcatlitterboxtester"
PATH_TO_CERTIFICATE = "certs/cert_2.pem.crt"  
PATH_TO_PRIVATE_KEY = "certs/priv_key_2.key"  
PATH_TO_AMAZON_ROOT_CA_1 = "certs/AmazonRootCA1.pem"  
USER = "lior2"
TOPIC_HUM = 'cat/litterbox/'+USER+'/humidity'
TOPIC_enter = 'cat/litterbox/'+USER+'/entry'
RANGE = 20


event_loop_group = io.EventLoopGroup(1)
host_resolver = io.DefaultHostResolver(event_loop_group)
client_bootstrap = io.ClientBootstrap(event_loop_group, host_resolver)
mqtt_connection = mqtt_connection_builder.mtls_from_path(
            endpoint=ENDPOINT,
            cert_filepath=PATH_TO_CERTIFICATE,
            pri_key_filepath=PATH_TO_PRIVATE_KEY,
            client_bootstrap=client_bootstrap,
            ca_filepath=PATH_TO_AMAZON_ROOT_CA_1,
            client_id=CLIENT_ID,
            clean_session=False,
            keep_alive_secs=30
            )
print("Connecting to {} with client ID '{}'...".format(
        ENDPOINT, CLIENT_ID))
# Make the connect() call
connect_future = mqtt_connection.connect()
x = connect_future.result()
print(x)            # ensure to us that the server is connected via text print and if not it needs to be restarted

if (x == False):
    exit(1)
t.sleep(5)
# Publish message to server desired number of times.
print('Begin Publish')
for i in range (RANGE):
    Humidity = {"Humidity" + " " + USER: str(random.randint(25,48)) + "%"}
    entry = {"cat_in" + " " + USER: 1}
    #print(json.dumps(Humidity))
    mqtt_connection.publish(topic=TOPIC_HUM, payload=json.dumps(Humidity), qos=mqtt.QoS.AT_LEAST_ONCE)
    print("Published: '" + json.dumps(Humidity) + "' to the topic: " + TOPIC_HUM)
    if(i%3 == 0):
        mqtt_connection.publish(topic=TOPIC_enter, payload=json.dumps(entry), qos=mqtt.QoS.AT_LEAST_ONCE)
        print("Published: '" + json.dumps(entry) + "' to the topic: " + TOPIC_enter)

    t.sleep(3)
print('Publish End')
disconnect_future = mqtt_connection.disconnect()
disconnect_future.result()



#'''