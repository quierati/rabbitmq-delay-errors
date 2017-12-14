#!/usr/bin/env python
import sys
import random

from orders_queue import OrdersQueue

number_of_messages = ' '.join(sys.argv[1:]) or 100 

job = OrdersQueue()

for i in range(int(number_of_messages)):
   # create order id
   order_id = random.randint(1000000, 9999999)

   # create msg
   msg = {'job_type': 'orders', 'job_id': order_id, 'created_at': '2017-01-01 00:00:00', 'error': False, 'retry': 0, 'force': False}

   # send msg to queue
   job.post_msg(msg)
