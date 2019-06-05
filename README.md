PYTHON RABBITMQ QUEUE DELAYED
-----------------------------

### prepare enviroment
```sh
(yum|apt) install python3 python3-pip
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### populate rabbitmq with msg
```sh
python example_queue_sender.py
pythor example_queue_sender_error.py
```

### consumer msg on rabbitmq
```sh
python example_queue_consumer.py
python example_queue_consumer_error.py
```

