import os
import json
import asyncio
from web3.types import LogReceipt
from web3.contract import LogFilter

from web3environment import Web3Interface, Blockchains
from utils import get_logger
from distribution import RabbitPublisher
import myweb3encoding


class EventScraper:

    POLLING_INTERVAL_SECONDS = 1

    def __init__(self, contract_watchlist, rabbitmq_config, endpoint):
        self.logger = get_logger(self.__class__.__name__)
        self.w3i = Web3Interface(blockchain=Blockchains.ETHEREUM,
                                 endpoint=endpoint)
        self.contract_watchlist = contract_watchlist
        self.logger.info("Loaded contract watchlist")

        # this is needed to keep a strong reference on the coroutines so that the GC doesn't collect them.
        # as per documentation indicated
        self.background_tasks = set()
        self.is_test_run = False
        self.publisher = self.get_rabbit_connection(rabbitmq_config)

    @staticmethod
    def get_rabbit_connection(rabbitmq_config):
        return RabbitPublisher(rabbitmq_config)

    def setup_filters(self):
        """
        Function is the entry point to the main logic of the code.
        All business logic related code is executed from here.
        It sets up an asyncio coroutine for each smart contract filter and monitors it.
            On each event will publish the event data on a previously created RabbitMQ server
        @return: nothing
        """
        loop = asyncio.get_event_loop()

        for contract_data in self.contract_watchlist['contracts']:
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
                # TODO: there is actually a better way using web3.eth.filter:
                #  https://web3py.readthedocs.io/en/latest/filters.html#event-log-filters
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
    def compose_filter_creation_execution_string(event_name: str, argument_filters: dict) -> str:
        """
        Will combine the given parameters to form a python execution string for setting up a new filter.
        This will be removed, but for the purpose of this POC, it will stay and be properly documented.

        Example:
        event_name: "PairCreated"
        argument_filters: {"fromBlock": "latest"}
        return: 'contract.events.PairCreated.createFilter(fromBlock="latest")'

        @param event_name: the contract event name on which the filter will be added
        @param argument_filters: the "filter_params" that will be passed to createFilter function, as
        defined in https://web3py.readthedocs.io/en/stable/web3.eth.html#filters
        @return: the string composing the function to be executed
        """
        normalised_args = ""
        for argument, value in argument_filters.items():
            try:
                extra = '{}={}, '.format(argument, int(value))
            except ValueError:
                extra = '{}="{}", '.format(argument, value)
            normalised_args += extra
        normalised_args = normalised_args[:-2]
        function_string = "contract.events.{}.createFilter({})".format(event_name, normalised_args)
        return function_string

    async def filter_loop(self, event_filter: LogFilter, polling_interval: int, event_name: str, filter_arguments: dict):
        """
        Entry point for the async functions. Passes arguments to handle_event function and polls polling_interval
        seconds for new events to pass
        """
        self.logger.info("Starting asyncio filter routine {} {} with arguments: {}".format(
            event_name, event_filter, filter_arguments))
        while True:
            for event_data in event_filter.get_new_entries():
                self.handle_event(event_data, filter_arguments, event_name)
                if self.is_test_run:
                    # helper part for testing purpose
                    return
            await asyncio.sleep(polling_interval)

    def handle_event(self, event: LogReceipt, filter_arguments: dict, event_name: str):
        """
        Function handles the received event from the smart contract, the filter arguments used and the event name.
        Will group the data and publish it on the indicated RabbitMQ routing key
        @param event: the blockchain event as returned by the API
        @param filter_arguments: the dict with the filter parameters used
        @param event_name: the Event name
        @return: nothing, it publishes to the message broker new events
        """
        try:
            data = {
                "event_name": event_name,
                "filter_arguments": filter_arguments,
                # TODO: do a pull request with the extension to web3.toJSON to support bytes then resume using it here
                # "event_data": json.loads(self.w3i.web3.toJSON(event))
                "event_data": json.loads(myweb3encoding.to_json(event))
            }
            self.logger.info("Publishing event {} data to routing key".format(event_name))
            self.publisher.publish(json.dumps(data))
            self.logger.info("Done publishing event {} data to routing key".format(event_name))
        except Exception:
            self.logger.exception("Unknown problem publishing event {}:{}".format(event_name, event))


def load_events_filter() -> dict:
    """
    Reads the content of input_file_name (json file) and returns it. Simple helper function
    @return: the targeted contract events and filters
    """
    input_file_name = os.path.join(os.path.dirname(os.path.abspath(__file__)), "contract-watchlist.json")
    with open(input_file_name, "rt") as fin:
        return json.load(fin)


def main():
    endpoint = os.environ['RPC_ENDPOINT_HTTPS_URL']
    rabbitmq_config = {
        "host": os.environ["RABBIT_HOST_URL"],
        "port": int(os.environ["RABBIT_HOST_PORT"]),
        "exchange": os.environ["RABBIT_EXCHANGE"],
        "routing_key": os.environ["RABBIT_ROUTING_KEY"],
        "user": os.environ["RABBIT_USER"],
        "password": os.environ["RABBIT_PASSWORD"]
    }

    contract_watchlist = load_events_filter()
    event_scraper = EventScraper(contract_watchlist=contract_watchlist,
                                 rabbitmq_config=rabbitmq_config,
                                 endpoint=endpoint)
    event_scraper.setup_filters()


if __name__ == '__main__':
    main()
