#!/usr/bin/env python

from orders_queue import OrdersQueue

if __name__ == '__main__':
   job = OrdersQueue()
   job.consumer()
