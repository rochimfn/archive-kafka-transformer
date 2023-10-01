from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
from multiprocessing import Process, Queue
import logging
import socket
import sys
import json
import hashlib
import signal

logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.INFO)


class Switch():
    is_active = True
    def __init__(self):
        signal.signal(signal.SIGINT, self.turn_off)
        signal.signal(signal.SIGTERM, self.turn_off)

    def turn_off(self, *_):
        self.is_active = False


def consumer(q: Queue, switch: Switch):
    logging.info("consumer started")
    consumer_topic = 'random_topics'
    conf = {'bootstrap.servers': 'localhost:9092',
                'group.id': 'consumer_parallel'}
    consumer = Consumer(conf)
    consumer.subscribe([consumer_topic])
    
    while switch.is_active:
        msg = consumer.poll(1.0)
        if msg is None: continue
        
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                sys.stderr.write('%% %s [%d] reached end at offset %d\n' % (msg.topic(), msg.partition(), msg.offset()))
            elif msg.error(): raise KafkaException(msg.error())
        else:
            q.put((msg.key(), msg.value()))

    logging.info("receiving signal")
    consumer.close()
    q.put(signal.SIGINT)


def transformer(consumer_q: Queue, producer_q: Queue):
    logging.info('transformer started')
    while True:
        # tranformation
        logging.info("transformer poll")
        item = consumer_q.get()
        if item == signal.SIGINT:
            producer_q.put(signal.SIGINT)
            return
        
        key = item[0]
        value = json.loads(item[1])
        value['hash'] = hashlib.md5(item[1]).hexdigest()
        producer_q.put((key, value))
    

def producer_acked(err, msg):
    if err is not None:
        logging.info('Failed to deliver message: %s: %s' % (str(msg), str(err)))
    else:
        logging.info('Message produced: %s' % (str(msg)))


def producer(producer_q: Queue):
    logging.info("producer started")
    producer_topic = 'random_topics_parallel_transformed'
    conf = {'bootstrap.servers': 'localhost:9092',
    'client.id': socket.gethostname()}
    producer = Producer(conf)

    while True:
        logging.info("producer poll") 
        item = producer_q.get()
        if item == signal.SIGINT:
            unflushed = producer.flush()
            logging.info(f"unflushed: {unflushed}")
            return
        # producer
        producer.produce(producer_topic, key=item[0], value=json.dumps(item[1]), callback=producer_acked)
        producer.flush()


def main():
    s = Switch()
    consumer_q = Queue()
    producer_q = Queue()

    consumer_process = Process(target=consumer, args=(consumer_q,s,))
    transformer_process = Process(target=transformer, args=(consumer_q, producer_q,))
    producer_process = Process(target=producer, args=(producer_q,))

    consumer_process.start()
    transformer_process.start()
    producer_process.start()

    consumer_process.join()
    transformer_process.join()
    producer_process.join()

    consumer_process.close()
    transformer_process.close()
    producer_process.close()

    logging.info('all done')


if __name__ == '__main__':
    main()