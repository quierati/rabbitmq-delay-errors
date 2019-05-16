import json

from rabbitmq import RabbitMQ


class OrdersQueue(RabbitMQ):
    TTL_DELAY = 60000
    MAX_RETRY = 5

    def __init__(self, queue='orders'):
        self.msg = {}
        super(OrdersQueue, self).__init__(queue)

    def callback(self, ch, method, properties, body):
        #load message as object
        self.msg = json.loads(body)
        print(" [*] Initializing job processing...")
        print(" [x] Received job: %r" % body)

        # 
        if isinstance(properties.headers, dict):
            for item in properties.headers['x-death']:
                if item.get('queue') == self._queue_error:
                    if item['count'] > self.MAX_RETRY:
                        ch.basic_ack(delivery_tag = method.delivery_tag)
                        print(" [#] Stopping processing message, maximum attempts reached")
                        return

        #
        # ... implement block code here
        # check on database status of job in begining end ending before run and save #fix duplicate, re-run only when msg.get('force') is true
        #

        #simulate error delay
        if self.msg.get('job_id') >= 7000000:
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            print(" [x] Message rescheduled in delay queue")
        else:
            # release job from queue
            ch.basic_ack(delivery_tag = method.delivery_tag)
            print(" [x] Done")

