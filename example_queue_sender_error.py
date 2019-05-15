#!/usr/bin/env python
import sys
import random
from datetime import datetime

from orders_queue import OrdersQueue

if __name__ == '__main__':
    number_of_messages = ' '.join(sys.argv[1:]) or 100 

    job = OrdersQueue()

    # create multiples jobs
    for i in range(int(number_of_messages)):
        # create order id
        order_id = random.randint(1000000, 9999999)

        # template create msg
        msg = {'job_type': 'orders', 'job_id': order_id, 'created_at': datetime.utcnow().isoformat(), 'error': True, 'retry': 0, 'force': False}

        # send job queue delay
        job.send_msg_queue_error_delay(msg, job.MIN_TTL_DELAY)  # delay 10sec
