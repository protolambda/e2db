import argparse
import trio
from eth_typing import BlockNumber
from sqlalchemy.orm import sessionmaker, Session

from e2db.depwatch import eth1_conn
from e2db.depwatch.monitor import DepositMonitor
from e2db.feedproc.dep_log_feed import ev_deposit_batch_loop
from e2db.feedproc.eth1_block_feed import ev_eth1_block_loop
from e2db.models import DepositTx, Base
from sqlalchemy import func


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


async def main(session: Session, eth1_rpc: str, dep_contract_addr: str, contract_deploy_block: int, backfill_repeat: int):
    # Look for latest existing block, then adjust starting point from there.
    start_block_num = contract_deploy_block
    max_block_number_res = session.query(func.max(DepositTx.block_num)).first()[0]
    if max_block_number_res is not None:
        start_block_num = max(contract_deploy_block, max_block_number_res - backfill_repeat)

    eth1mon = eth1_conn.create_monitor(eth1_rpc, dep_contract_addr)
    current_block_num = eth1mon.get_block('latest').number

    print(f"start at block {start_block_num}, backfill up to {current_block_num} and watch for more")
    await run_eth1_feeds(session, eth1mon, BlockNumber(start_block_num), current_block_num)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run eth2 indexing.')
    parser.add_argument('--db-addr', dest='db_addr', default='sqlite:///testing.db',
                        help='the DB string to use for a connection')
    parser.add_argument('--eth1-addr', dest='eth1_addr', default='http://localhost:8545/',
                        help='the HTTP API address of the Eth1 node to extract data from (ws or http)')

    parser.add_argument('--contract-deploy-block', dest='contract_deploy_block', type=int, default=2758066, help='The contract deployment block')
    parser.add_argument('--backfill-repeat', dest='backfill_repeat', type=int, default=100, help='The distance to re-do from the latest entry.')
    parser.add_argument('--contract-addr', dest='contract_addr', default='0x42cc0FcEB02015F145105Cf6f19F90e9BEa76558', help='Contract address.')

    args = parser.parse_args()

    print(f"Connecting to DB at {args.db_addr}")
    from sqlalchemy import create_engine
    engine = create_engine(args.db_addr)
    engine.connect()

    print("Creating/checking tables")
    Base.metadata.create_all(engine, checkfirst=True)

    print("Creating session")
    Session = sessionmaker(bind=engine)
    session = Session()

    trio.run(main, session, args.eth1_addr, args.contract_addr, args.contract_deploy_block, args.backfill_repeat)
