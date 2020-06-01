import trio
from eth_typing import BlockNumber
from sqlalchemy.orm import sessionmaker, Session

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
DEPOSIT_CONTRACT_ADDRESS = "0x42cc0FcEB02015F145105Cf6f19F90e9BEa76558"
DEPOSIT_CONTRACT_DEPLOY_BLOCK = 2758066

# Number of blocks to start backfill, from before latest known block
BACKFILL_REPEAT_DISTANCE = 100

ETH1_RPC = "wss://goerli.infura.io/ws/v3/caf2e67f3cec4926827e5b4d17dc5167"


async def run_eth1_feeds(session: Session, eth1mon: DepositMonitor, start_backfill: BlockNumber, end_backfill: BlockNumber):
    async with trio.open_nursery() as nursery:
        send, recv = trio.open_memory_channel(max_buffer_size=100)
        nursery.start_soon(eth1mon.backfill_logs, start_backfill, end_backfill, send)
        nursery.start_soon(eth1mon.watch_logs, end_backfill, send)
        nursery.start_soon(ev_deposit_batch_loop, session, recv)

        send, recv = trio.open_memory_channel(max_buffer_size=100)
        nursery.start_soon(eth1mon.backfill_blocks, start_backfill, end_backfill, send)
        nursery.start_soon(eth1mon.watch_blocks, send)
        nursery.start_soon(ev_eth1_block_loop, session, recv)


async def main(session: Session):
    # Look for latest existing block, then adjust starting point from there.
    start_block_num = DEPOSIT_CONTRACT_DEPLOY_BLOCK
    max_block_number_res = session.query(func.max(DepositTx.block_num)).first()[0]
    if max_block_number_res is not None:
        start_block_num = max(DEPOSIT_CONTRACT_DEPLOY_BLOCK, max_block_number_res - BACKFILL_REPEAT_DISTANCE)

    eth1mon = eth1_conn.create_monitor(ETH1_RPC, DEPOSIT_CONTRACT_ADDRESS)
    current_block_num = eth1mon.get_block('latest').number

    print(f"start at block {start_block_num}, backfill up to {current_block_num} and watch for more")
    await run_eth1_feeds(session, eth1mon, BlockNumber(start_block_num), current_block_num)


if __name__ == '__main__':
    from sqlalchemy import create_engine
    engine = create_engine('sqlite:///testing_eth1_2.db')

    Base.metadata.create_all(engine, checkfirst=True)

    Session = sessionmaker(bind=engine)
    session = Session()

    trio.run(main, session)
