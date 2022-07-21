import pika
from utils import get_logger


"""
Code responsible for creating a connection to an indicated RabbitMQ server and passing data
Modified from https://medium.com/@rahulsamant_2674/python-rabbitmq-8c1c3b79ab3d
"""


class RabbitPublisher:
    def __init__(self, config):
        self.logger = get_logger(self.__class__.__name__)
        self.config = config
        self._routing_key = config.get("routing_key")
        self._channel = self.create_channel()

    def publish(self, message, routing_key=None):
        if not routing_key:
            if not self._routing_key:
                raise ValueError("A routing key needs to be set either in config or in publish")
            routing_key = self._routing_key

        # Publishes message to the exchange with the given routing key
        self._channel.basic_publish(exchange=self.config["exchange"], routing_key=routing_key, body=message)
        self.logger.debug("Sent message {} on routing key {}".format(message, routing_key))

    def create_connection(self):
        param = pika.ConnectionParameters(host=self.config["host"],
                                          port=self.config["port"],
                                          credentials=pika.PlainCredentials(self.config["user"],
                                                                            self.config["password"]),
                                          heartbeat=0)  # no timeout, possibly an issue
        return pika.BlockingConnection(param)

    def create_channel(self):
        connection = self.create_connection()

        # Create a new channel with the next available channel number
        # or pass in a channel number to use
        channel = connection.channel()

        # Creates an exchange if it does not already exist, and if
        # the exchange exists,
        # verifies that it is of the correct and expected class.
        channel.exchange_declare(exchange=self.config["exchange"],
                                 exchange_type="topic",
                                 durable=True,
                                 auto_delete=False)
        return channel
