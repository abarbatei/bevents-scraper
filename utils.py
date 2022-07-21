import logging
import sys
import web3
import json
import requests

from colorama import Fore, Style, init as colorama_init
colorama_init()


class CustomFormatter(logging.Formatter):

    print_format = "%(asctime)s | %(message)s"

    FORMATS = {
        logging.DEBUG: Fore.WHITE + print_format + Style.RESET_ALL,
        logging.INFO: Fore.LIGHTWHITE_EX + print_format + Style.RESET_ALL,
        logging.WARNING: Fore.LIGHTYELLOW_EX + print_format + Style.RESET_ALL,
        logging.ERROR: Fore.LIGHTRED_EX + print_format + Style.RESET_ALL,
        logging.CRITICAL: Fore.LIGHTRED_EX + print_format + Style.RESET_ALL
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)


def get_logger(name, file_name=None, use_file_logger=True):

    if not file_name:
        file_name = 'debug_log.txt'
    logger = logging.getLogger(name)

    if logger.hasHandlers():
        return logger

    logger.setLevel(logging.DEBUG)

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.INFO)
    stdout_handler.setFormatter(CustomFormatter())

    if use_file_logger:
        formatter = logging.Formatter('%(asctime)s [%(levelname)7s][%(name)s]: %(message)s')
        file_handler = logging.FileHandler(file_name, encoding='utf8')
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)

        logger.addHandler(file_handler)

    logger.addHandler(stdout_handler)

    return logger


def validate_address(input_address):
    try:
        web3.Web3.toChecksumAddress(input_address)
        return True
    except:
        return False


def endpoint_issue_retry(original_function):
    # this issue appeared only on Fantom chain, probably will appear on others. I think it has to do with
    # the quality of the RCP endpoint. But for the default ones, this kills
    # new one
    # requests.exceptions.HTTPError: 503 Server Error: Service Temporarily Unavailable for url: https://rpcapi.fantom.network/
    def wrapped_function(self, *args, **kwargs):
        max_tries = 7
        attempted_tries = 1
        last_exception = None
        while attempted_tries < max_tries:
            try:
                return original_function(self, *args, **kwargs)
            except json.decoder.JSONDecodeError as e:
                last_exception = e
                attempted_tries += 1
                self.logger.debug("json.decoder.JSONDecodeError exception: {} when calling"
                                  " function {}. Retrying attempt {}/{}".format(e.msg, original_function.__name__,
                                                                                attempted_tries, max_tries))
            except requests.exceptions.HTTPError as e:
                last_exception = e
                attempted_tries += 1
                if "503" in e.args[0]:
                    self.logger.debug("requests.exceptions.HTTPError exception: {} when calling"
                                      " function {}. Retrying attempt {}/{}".format(
                                       e.args, original_function.__name__, attempted_tries, max_tries))
            except ValueError as e:
                last_exception = e
                # ValueError: {'code': -32602, 'message': 'invalid method params'}
                self.logger.debug("Possible random issue with RPC: {} when calling function {}. "
                                  "Retrying attempt {}/{}".format(e.args, original_function.__name__,
                                                                  attempted_tries, max_tries))
                attempted_tries += 1
        raise last_exception
    return wrapped_function


def jsonrpc_issue_retry(original_function):
    # new one
    def wrapped_function(self, *args, **kwargs):
        max_tries = 7
        attempted_tries = 0

        while attempted_tries < max_tries:
            try:
                return original_function(self, *args, **kwargs)
            except KeyError as e:
                attempted_tries += 1
                if "jsonrpc" in e.args[0]:
                    self.logger.debug("KeyError: 'jsonrpc' when calling function {}. Retrying attempt {}/{}".format(
                        original_function.__name__, attempted_tries, max_tries))
                else:
                    raise e
    return wrapped_function
