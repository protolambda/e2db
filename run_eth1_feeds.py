import trio
from eth_typing import BlockNumber
from sqlalchemy.orm import sessionmaker

from e2db.depwatch import eth1_conn
from e2db.depwatch.monitor import DepositMonitor
from e2db.feedproc.dep_log_feed import ev_deposit_batch_loop
from e2db.feedproc.eth1_block_feed import ev_eth1_block_loop
from e2db.models import DepositTx, Base
from sqlalchemy import func

# Topaz
#DEPOSIT_CONTRACT_ADDRESS = "0x5cA1e00004366Ac85f492887AAab12d0e6418876"
#DEPOSIT_CONTRACT_DEPLOY_BLOCK = 2523557

# Schlesi
# DEPOSIT_CONTRACT_ADDRESS = "0xA15554BF93a052669B511ae29EA21f3581677ac5"
# DEPOSIT_CONTRACT_DEPLOY_BLOCK = 2596126

# Witti
DEPOSIT_CONTRACT_ADDRESS = "0x9eED6A5741e3D071d70817beD551D0078e9a2706"
DEPOSIT_CONTRACT_DEPLOY_BLOCK = 2740461

# Number of blocks to start backfill, from before latest known block
BACKFILL_REPEAT_DISTANCE = 100

ETH1_RPC = "https://goerli.infura.io/v3/caf2e67f3cec4926827e5b4d17dc5167"
# ETH1_RPC = "wss://goerli.prylabs.net/websocket"


async def run_eth1_feeds(eth1mon: DepositMonitor, start_backfill: BlockNumber, start_watch: BlockNumber):
    async with trio.open_nursery() as nursery:
        send, recv = trio.open_memory_channel(max_buffer_size=100)
        nursery.start_soon(eth1mon.backfill_logs, start_backfill, start_watch, send)
        nursery.start_soon(eth1mon.watch_logs, start_watch, send)
        nursery.start_soon(ev_deposit_batch_loop, recv)

        send, recv = trio.open_memory_channel(max_buffer_size=100)
        nursery.start_soon(eth1mon.backfill_blocks, start_backfill, start_watch, send)
        nursery.start_soon(eth1mon.watch_blocks, start_watch, send)
        nursery.start_soon(ev_eth1_block_loop, recv)


async def main():
    # Look for latest existing block, then adjust starting point from there.
    start_block_num = DEPOSIT_CONTRACT_DEPLOY_BLOCK
    max_block_number_res = session.query(func.max(DepositTx.block_num)).first()[0]
    if max_block_number_res is not None:
        start_block_num = max(DEPOSIT_CONTRACT_DEPLOY_BLOCK, max_block_number_res - BACKFILL_REPEAT_DISTANCE)

    eth1mon = eth1_conn.create_monitor(ETH1_RPC, DEPOSIT_CONTRACT_ADDRESS)
    current_block_num = eth1mon.get_block('latest').number

    print(f"start at block {start_block_num}, backfill up to {current_block_num} and watch for more")
    await run_eth1_feeds(eth1mon, BlockNumber(start_block_num), current_block_num)


if __name__ == '__main__':
    from sqlalchemy import create_engine
    engine = create_engine('sqlite:///testing.db')

    Base.metadata.create_all(engine, checkfirst=True)

    Session = sessionmaker(bind=engine)
    session = Session()

    trio.run(main)
