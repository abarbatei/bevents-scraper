from web3 import Web3
from web3.middleware import geth_poa_middleware  # Needed for Binance Smart Chain
from utils import get_logger, endpoint_issue_retry, jsonrpc_issue_retry


class Blockchains:
    ETHEREUM = 'ethereum'
    BINANCE_SMART_CHAIN = 'bsc'
    FANTOM = 'ftm'
    CRONOS = 'cronos'
    AVALANCHE = 'avalanche'

    BINANCE_SMART_CHAIN_TESTNET = "bsc-testnet"
    ETHEREUM_RINKEBY_TESTNET = 'ethereum-rinkey-testnet'


class Web3Interface:

    SUPPORTED_BLOCKCHAINS = [Blockchains.ETHEREUM,
                             # Blockchains.BINANCE_SMART_CHAIN,
                             # Blockchains.FANTOM,
                             # Blockchains.CRONOS,
                             # Blockchains.AVALANCHE
                             ]

    def __init__(self, blockchain, endpoint, is_test_net=False):
        if blockchain not in Web3Interface.SUPPORTED_BLOCKCHAINS:
            raise ValueError("Blockchain {} not supported. Currently supported are: {}".format(
                blockchain, Web3Interface.SUPPORTED_BLOCKCHAINS))

        self.logger = get_logger(self.__class__.__name__)
        self.is_test_net = is_test_net
        self.blockchain = blockchain

        self.endpoint = endpoint

        if self.endpoint.startswith("wss"):
            self.web3 = Web3(Web3.WebsocketProvider(self.endpoint))
        else:
            self.web3 = Web3(Web3.HTTPProvider(self.endpoint))

        if blockchain == Blockchains.BINANCE_SMART_CHAIN:
            self.web3.middleware_onion.inject(geth_poa_middleware, layer=0)

        self.web3.eth.handleRevert = True
        self.check_connection()

    @jsonrpc_issue_retry
    @endpoint_issue_retry
    def check_connection(self, max_attempts=10):
        attempt = 1
        while attempt <= max_attempts:
            if self.web3.isConnected():
                self.logger.debug("Successfully connected to endpoint: {}".format(self.endpoint))
                return True
            self.logger.debug("Problem connecting to endpoint {}! attempt {}/{}".format(
                self.endpoint, attempt, max_attempts))
            attempt += 1

        message = "Problem connecting to endpoint {}! aborting after {} tries".format(self.endpoint, max_attempts)
        self.logger.error(message)

        raise Exception(message)
