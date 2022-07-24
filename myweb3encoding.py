import json
import web3

"""
This code is taken strategy from web3._utils.encoding and extended the to_json (toJSON) method 
so that it supports decoding bytes also. That's all.

Easelly testable with the event OrderFulfilled 
from OpenSea: Seaport 1.1 contract 0x00000000006c3852cbef3e08e8df289169ede581

Will do a pull request on the official web3py repo, if accepted, will remove this code.  
"""

from typing import (
    Any,
    Dict,
    Union,
)

from eth_typing import (
    HexStr,
)

from hexbytes import (
    HexBytes,
)

from web3.datastructures import (
    AttributeDict,
)


class MyWeb3JsonEncoder(json.JSONEncoder):
    def default(self, obj: Any) -> Union[Dict[Any, Any], HexStr]:
        if isinstance(obj, AttributeDict):
            return {k: v for k, v in obj.items()}
        if isinstance(obj, HexBytes):
            return HexStr(obj.hex())
        # just added the next 2 lines
        if isinstance(obj, bytes):
            return HexStr(HexBytes(obj).hex())
        return json.JSONEncoder.default(self, obj)


def to_json(obj: Dict[Any, Any]) -> str:
    '''
    Convert a complex object (like a transaction object) to a JSON string
    '''
    return web3._utils.encoding.FriendlyJsonSerde().json_encode(obj, cls=MyWeb3JsonEncoder)
