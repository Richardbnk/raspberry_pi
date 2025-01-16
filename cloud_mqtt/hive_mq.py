"""
This script demonstrates the use of the HiveMQ public MQTT broker for publishing and subscribing to messages using the Paho MQTT client library. The script connects to the broker, subscribes to a topic, and publishes data.

1. **Broker Configuration:**
   - **Broker Address:** `broker.hivemq.com` (HiveMQ public broker).
   - **Port:** 1883 (default MQTT port).
   - **Client ID:** A unique identifier for the MQTT client (`clientId-...`).

2. **Receiving Messages:**
   - The `on_message` callback is triggered whenever a message is received on a subscribed topic.
   - It prints the topic name and the message payload.

3. **Subscribing to a Topic:**
   - Subscribes to the topic `"streaming/OMIoT/team/valores_medios"`.
   - This allows the client to receive messages published to this topic.

4. **Publishing Messages:**
   - Publishes numbers (`1` to `99`) as strings to the topic `"streaming/OMIoT/team/valores_instantaneos"`.
   - Uses a loop to send messages at 1-second intervals.

5. **MQTT Client Lifecycle:**
   - **`connect`:** Connects to the broker.
   - **`loop_start`:** Starts the MQTT network loop in a separate thread.
   - **`disconnect`:** Disconnects from the broker after publishing is complete.

6. **Use Case:**
   - This script can be used for testing MQTT communication or for IoT applications where data is published and subscribed to in real time.

**Dependencies:**
   - Install the Paho MQTT client library if not already installed:
     ```bash
     pip install paho-mqtt
     ```

**Example Usage:**
   - Run the script to observe messages being published to and received from the broker.
   - Use another MQTT client to subscribe to the topics and see the published messages.

**Notes:**
   - Replace `"clientId-..."` with a unique client ID to avoid connection conflicts.
   - The HiveMQ public broker is for testing purposes; do not use it for sensitive or critical data.
"""

#https://www.hivemq.com/public-mqtt-broker/

import paho.mqtt.client as mqtt
import time

client_id = "clientId-..."

port = 1883
broker_address = "broker.hivemq.com"

client = mqtt.Client(client_id)  # clientId- add 10 random characters
client.connect(broker_address, port)

# Receives data from any subscription on the server
def on_message(client, userdata, msg):
    print("TOPIC: '{}'   MSG:'{}'".format(msg.topic, msg.payload.decode()))

# Prepares to subscribe to a channel
client.on_message = on_message
client.loop_start()
client.subscribe("streaming/OMIoT/team/valores_medios")

# Performs 10 publish operations to the server's topic
for i in range(1, 100):
    # v1/username/things/clientID/data/channel
    client.publish("streaming/OMIoT/team/valores_instantaneos", str(i))
    print(i)
    time.sleep(1)

client.disconnect()
