#!/usr/bin/env python
import sys
import random
from datetime import datetime

from orders_queue import OrdersQueue

if __name__ == '__main__':
    number_of_messages = ' '.join(sys.argv[1:]) or 100 
    job = OrdersQueue()

    for i in range(int(number_of_messages)):
        # create order id
        order_id = random.randint(1000000, 9999999)

        # template create msg
        msg = {'job_type': 'orders', 'job_id': order_id, 'created_at': datetime.utcnow().isoformat(), 'error': False, 'retry': 0, 'force': False}

        # send msg to queue
        job.post_msg(msg)

