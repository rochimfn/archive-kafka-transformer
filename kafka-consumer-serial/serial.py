from confluent_kafka import Consumer, Producer
from confluent_kafka.error import KafkaError, KafkaException
import hashlib
import json
import sys
import logging

logging.basicConfig(level=logging.INFO)

def consume_loop(consumer, broker, topics, destination):
    logging.info('prepare producer')
    conf = {'bootstrap.servers': broker}
    producer = Producer(conf)

    def acked(err, msg):
        if err is not None:
            logging.info("Failed to deliver message: %s: %s" % (str(msg), str(err)))
        else:
            logging.info("Message produced: %s" % (str(msg)))

    logging.info('start consume')
    try:
        consumer.subscribe(topics)

        while True:
            logging.info('pollling')
            # consuming
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                    (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                # tranformation
                key = msg.key()
                value = json.loads(msg.value())
                value['hash'] = hashlib.md5(msg.value()).hexdigest()
                # producer
                producer.produce(destination, key=key, value=json.dumps(value), callback=acked)
                producer.poll(1)

    except KeyboardInterrupt:
        logging.info('receive interrupt')
    finally:
        consumer.close()



def main():
    topic = 'random_topics'
    destination_topic = 'random_topics_serial_transformed'
    broker = "localhost:9092"
    conf = {'bootstrap.servers': broker,
            'group.id': "consumer_serial"}
    consumer = Consumer(conf)
    consume_loop(consumer=consumer, broker=broker, topics=[topic], destination=destination_topic)


if __name__ == '__main__':
    main()