import os
import abc
import json

import six
import pika

@six.add_metaclass(abc.ABCMeta)
class RabbitMQ(object):
    RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', '127.0.0.1')
    RABBITMQ_PORT = os.environ.get('RABBITMQ_PORT', 5672)
    RABBITMQ_USER = os.environ.get('RABBITMQ_USER','guest')
    RABBITMQ_PASS = os.environ.get('RABBITMQ_PASS','guest')
    PREFIX_QUEUE_ERROR_DELAY = '{queue}_error_delay_{time}'
    PREFIX_QUEUE_ERROR = '{queue}_error'
    TTL_DELAY = 60000
    MAX_RETRY = 5

    def __init__(self, queue):
      self._connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=self.RABBITMQ_HOST,
                    port=self.RABBITMQ_PORT,
                    credentials=pika.PlainCredentials(self.RABBITMQ_USER, self.RABBITMQ_PASS)
                )
      )

      self._queue = queue
      self._queue_error = self._get_queue_error_name()
      self._queue_delay = self._get_queue_error_delay_name()

      # queue error
      self._channel_error = self._get_channel()
      self._channel_error.exchange_declare(self._queue_error, exchange_type='direct',durable=True, auto_delete=True)
      # create queue error if not exists, and mark durable
      self._channel_error.queue_declare(
            queue=self._queue_error, 
            durable=True,
            arguments={
                'x-dead-letter-exchange' : self._queue_delay, # Exchange used to transfer the message from A to B.
                'x-dead-letter-routing-key' : self._queue_delay # Name of the queue we want the message transferred to.
            }
      )

      # queue delay
      self._channel_delay = self._get_channel()
      self._channel_delay.queue_declare(
            queue=self._queue_delay, 
            durable=True,
            arguments={
                'x-message-ttl' : self.TTL_DELAY,
                'x-dead-letter-exchange' : self._queue_error,
                'x-dead-letter-routing-key' : self._queue_error
            }
      )

      # queue main
      self._channel = self._get_channel()
      self._channel.exchange_declare(self._queue_delay, exchange_type='direct',durable=True, auto_delete=True)
      # create queue main if not exists, and mark durable 
      self._channel.queue_declare(
            queue=self._queue,
            durable=True,
            arguments={
                'x-dead-letter-exchange' : self._queue_delay,
                'x-dead-letter-routing-key' : self._queue_delay
            }
      )

      # bind queue for works delay
      self._channel_error.queue_bind(exchange=self._queue_error, queue=self._queue_error)
      self._channel.queue_bind(exchange=self._queue_delay, queue=self._queue_delay)

    def _get_channel(self):
      channel = self._connection.channel()
      channel.confirm_delivery()
      return channel

    def __del__(self):
      self._channel.close()
      self._channel_error.close()
      self._channel_delay.close()
      self._connection.close()

    def post_msg(self, message):
      self._channel.basic_publish(
            exchange='',
            routing_key=self._queue,
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=2,
                content_type='application/json'
            )
      )

    def _get_queue_error_name(self):
      return self.PREFIX_QUEUE_ERROR.format(queue=self._queue)

    def _get_queue_error_delay_name(self):
      return self.PREFIX_QUEUE_ERROR_DELAY.format(queue=self._queue, time=self.TTL_DELAY)

    def send_msg_queue_error_delay(self, message): 
      self._channel_delay.basic_publish(
            exchange='',
            routing_key=self._queue_delay,
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=2,
                content_type='application/json'
            )
      )

    @abc.abstractmethod 
    def callback(self, ch, method, properties, body):
      """ This function is abstract method, please extend and implement"""
      print(" [x] Example: Received %r" % body)
      # set ack after success execute this line for remove message from queue
      # ch.basic_ack(delivery_tag = method.delivery_tag)
      print(" [x] Example: Done")

    def consumer(self):
      self._channel.basic_qos(prefetch_count=1)
      self._channel.basic_consume(self.callback, queue=self._queue)
      print(' [*] Waiting for messages. To exit press CTRL+C')
      try:
         self._channel.start_consuming()
      except:
         print(' [x] Terminited consumer queue...')

    def consumer_error(self):
      self._channel_error.basic_qos(prefetch_count=1)
      self._channel_error.basic_consume(self.callback, queue=self._queue_error)
      print(' [*] Waiting for messages in queue error. To exit press CTRL+C')
      try:
         self._channel_error.start_consuming()
      except:
         print(' [x] Terminited consumer queue error...')


# vim: set ft=python et ts=4 sw=4 :
