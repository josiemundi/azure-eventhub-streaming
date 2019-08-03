import os
import sys
import logging
import time
from azure import eventhub
from azure.eventhub import EventHubClient, Receiver, Offset, EventData
from websocket import create_connection

 
ADDRESS = "amqps://<namespace.servicebus.windows.net/<eventhubname>"
USER = "<policy name>"
KEY = "<primary key>"
CONSUMER_GROUP = "$default"
OFFSET = Offset("-1")
PARTITION = "0"

ws = create_connection("wss://ws.blockchain.info/inv")
ws.send('{"op":"unconfirmed_sub"}')

try:
    if not ADDRESS:
        raise ValueError("No EventHubs URL supplied.")
 
    # Create Event Hubs client
    client = EventHubClient(ADDRESS, debug=False, username=USER, password=KEY)
    sender = client.add_sender(partition="0")
    client.run()
    
    i = 0
    
    start_time = time.time()
    try:
        while True:
            sender.send(EventData(ws.recv()))
            print(i)
            if i > 10:
                break
            i = i + 1
    except:
        raise
    finally:
        end_time = time.time()
        client.stop()
        run_time = end_time - start_time

except KeyboardInterrupt:
    pass
