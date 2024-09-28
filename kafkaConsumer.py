from kafka import KafkaConsumer
from time import sleep
from json import dumps, loads
import json
from s3fs import S3FileSystem


import six
import sys
if sys.version_info >= (3, 12, 0):
    sys.modules['kafka.vendor.six.moves'] = six.moves


consumer = KafkaConsumer(
    'demo_test',
    bootstrap_servers=['{Enter IP here}:9092'],
    value_deserializer=lambda x: loads(x.decode('utf-8')) if x else None
)

s3 = S3FileSystem()

for count, message in enumerate(consumer):
    # Check if the message value is not None or empty
    if message.value:
        # Print the raw message for debugging
        print(f"Raw message: {message.value}")
        
        # Save to S3
        with s3.open(f"s3://{Enter your S3 Buckete}/stock_market_{count}.json", 'w') as file:
            json.dump(message.value, file)  # Use message.value instead of i.value
    else:
        print(f"Received empty message at count {count}")
