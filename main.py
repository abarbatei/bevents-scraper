import os
import json
import asyncio

from web3environment import Web3Interface, Blockchains
from utils import get_logger


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

            # this is actually used dynamically, do not delete
            contract = self.w3i.web3.eth.contract(address=address, abi=abi)

            events_to_listen = contract_data['events_to_listen']
            for event_name, event_data in events_to_listen.items():
                argument_filters = event_data['argument_filters']

                normalised_args = ""
                for argument, value in argument_filters.items():
                    normalised_args += '{}="{}", '.format(argument, value)
                normalised_args = normalised_args[:-2]
                function_string = "contract.events.{}.createFilter({})".format(event_name, normalised_args)
                event_filter = eval(function_string)

                self.logger.info("For contract {} created event filter: {}:{}".format(
                    address, event_name, normalised_args))

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
        self.logger.info(json.dumps(data, indent=4))


def main():
    event_scraper = EventScraper()
    event_scraper.setup_filters()


if __name__ == '__main__':
    main()