import os
import sys
import logging
import time
from azure.eventhub import EventHubClient, Receiver, Offset

ADDRESS = "amqps://<namespace>.servicebus.windows.net/<eventhub>"
USER = "<policyname>"
KEY = "<primarykey>"
CONSUMER_GROUP = "$default"
OFFSET = Offset("-1")
PARTITION = "0"

total = 0
client = EventHubClient(ADDRESS, debug=False, username=USER, password=KEY)
try:
    receiver = client.add_receiver(CONSUMER_GROUP, PARTITION, prefetch=5000, offset=OFFSET)
    client.run()
    start_time = time.time()
    batch = receiver.receive(timeout=5000)
    while batch:
        for event_data in batch:
            print(event_data.message)#body_as_str())
            total += 1
        batch = receiver.receive(timeout=5000)

    end_time = time.time()
    client.stop()
    run_time = end_time - start_time

except KeyboardInterrupt:
    pass
finally:
    client.stop()
