import os
import json
import asyncio

from web3environment import Web3Interface, Blockchains
from utils import get_logger
from distribution import RabbitPublisher


class EventScraper:

    POLLING_INTERVAL_SECONDS = 1
    INPUT_FILE_NAME = "contract-watchlist.json"

    def __init__(self):
        self.logger = get_logger(self.__class__.__name__)
        self.w3i = Web3Interface(blockchain=Blockchains.ETHEREUM,
                                 endpoint=os.environ['RPC_ENDPOINT_HTTPS_URL'])
        self.targeted_events_data = self.load_events_filter()
        self.logger.info("Loaded contract watchlist")
        self.background_tasks = set()

        config = {
            "host": os.environ["RABBIT_HOST_URL"],
            "port": int(os.environ["RABBIT_HOST_PORT"]),
            "exchange": os.environ["RABBIT_EXCHANGE"],
            "routing_key": os.environ["RABBIT_ROUTING_KEY"],
            "user": os.environ["RABBIT_USER"],
            "password": os.environ["RABBIT_PASSWORD"]
        }

        self.publisher = RabbitPublisher(config)

    @staticmethod
    def load_events_filter():
        with open(EventScraper.INPUT_FILE_NAME, "rt") as fin:
            return json.load(fin)

    def setup_filters(self):

        loop = asyncio.get_event_loop()

        for contract_data in self.targeted_events_data['contracts']:
            address = self.w3i.web3.toChecksumAddress(contract_data['address'])
            blockchain = contract_data['blockchain']
            if blockchain != self.w3i.blockchain:
                raise ValueError("Chain {} is not supported for smart contract {}!".format(
                    blockchain, address))
            abi = contract_data['abi']

            # this is actually used dynamically, do not delete, look down at the eval function
            contract = self.w3i.web3.eth.contract(address=address, abi=abi)

            events_to_listen = contract_data['events_to_listen']
            for event_name, event_data in events_to_listen.items():
                argument_filters = event_data['argument_filters']

                function_string = self.compose_filter_creation_execution_string(event_name, argument_filters)

                # this is a hack and the use of eval in general code should be discouraged
                # this also brings a very bad security risk if the passed argument can be controlled by a 3rd party
                # in this case it is not, it is composed of the content in contract-watchlist.json which we control
                event_filter = eval(function_string)

                self.logger.info("For contract {} created event filter: {}:{}".format(
                    address, event_name, argument_filters))

                task = loop.create_task(self.filter_loop(event_filter,
                                                         self.POLLING_INTERVAL_SECONDS,
                                                         event_name,
                                                         argument_filters))
                self.background_tasks.add(task)
                task.add_done_callback(self.background_tasks.discard)

        try:
            loop.run_until_complete(
                asyncio.wait(self.background_tasks)
            )
        finally:
            loop.close()

    @staticmethod
    def compose_filter_creation_execution_string(event_name, argument_filters):
        normalised_args = ""
        for argument, value in argument_filters.items():
            normalised_args += '{}="{}", '.format(argument, value)
        normalised_args = normalised_args[:-2]
        function_string = "contract.events.{}.createFilter({})".format(event_name, normalised_args)
        return function_string

    async def filter_loop(self, event_filter, polling_interval, event_name, filter_arguments):
        self.logger.info("Starting asyncio filter routine {} {} with arguments: {}".format(
            event_name, event_filter, filter_arguments))
        while True:
            for event_data in event_filter.get_new_entries():
                self.handle_event(event_data, filter_arguments, event_name)
            await asyncio.sleep(polling_interval)

    def handle_event(self, event, filter_arguments, event_name):
        data = {
            "event_name": event_name,
            "filter_arguments": filter_arguments,
            "event_data": json.loads(self.w3i.web3.toJSON(event))
        }
        self.logger.info("Publishing event {} data to routing key".format(event_name))
        self.publisher.publish(json.dumps(data))
        self.logger.info("Done publishing event {} data to routing key".format(event_name))


def main():
    event_scraper = EventScraper()
    event_scraper.setup_filters()


if __name__ == '__main__':
    main()
