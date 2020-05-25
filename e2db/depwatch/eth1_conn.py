import json
from eth_typing import Address
import importlib.resources as pkg_resources

from web3 import Web3
from web3.middleware import geth_poa_middleware  # For Goerli

from e2db.depwatch.monitor import DepositMonitor

from e2db.depwatch import res

deposit_contract_json = pkg_resources.read_text(res, "deposit_abi.json")

deposit_contract_abi = json.loads(deposit_contract_json)["abi"]


def create_monitor(eth1_rpc: str, dep_contract_addr: str) -> DepositMonitor:
    w3prov = Web3.HTTPProvider(eth1_rpc) if eth1_rpc.startswith("http") else Web3.WebsocketProvider(eth1_rpc)
    w3: Web3 = Web3(w3prov)

    # Handle POA Goerli style "extraData" in Web3
    # inject the poa compatibility middleware to the innermost layer
    w3.middleware_onion.inject(geth_poa_middleware, layer=0)

    contract_addr = Address(bytes.fromhex(dep_contract_addr.replace("0x", "")))
    return DepositMonitor(w3, contract_addr, deposit_contract_abi)
