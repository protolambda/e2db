import argparse
import trio

from eth2spec.config import config_util
config_util.config['GENESIS_FORK_VERSION'] = '0x00000113'

from eth2.core import ContentType
from eth2.models import lighthouse
from eth2.providers.http import Eth2HttpClient, Eth2HttpOptions
from eth2spec.phase0 import spec
from sqlalchemy.orm import sessionmaker

from e2db.eth2watch.monitor import Eth2Monitor
from e2db.feedproc.eth2_state_feed import ev_eth2_state_loop
from e2db.models import Base

import httpx


async def run_eth2_feeds(eth2mon: Eth2Monitor, watch: bool, backfill: bool, start_backfill: int, end_backfill: int):
    async with trio.open_nursery() as nursery:
        send, recv = trio.open_memory_channel(max_buffer_size=5)
        nursery.start_soon(ev_eth2_state_loop, session, recv)
        if watch:
            print("watching eth2 chain")
            nursery.start_soon(eth2mon.watch_hot_chain, send)
        if backfill:
            end_backfill_slot: spec.Slot
            if end_backfill < 0:
                # Backfill all the way up to the head. If not canonical, it will be re-orged by the watcher anyway.
                while True:
                    try:
                        head_info = await eth2mon.api.beacon.head()
                        end_backfill_slot = head_info.slot
                        break
                    except:
                        print("could not fetch head slot, retrying")
                        await trio.sleep(1)
            else:
                end_backfill_slot = spec.Slot(end_backfill)

            print(f"backfilling eth2 chain, slots: {start_backfill} to {end_backfill}")
            await eth2mon.backfill_cold_chain(spec.Slot(start_backfill), end_backfill_slot, send)


async def main(eth2_rpc: str, watch: bool, backfill: bool, backfill_start: int, backfill_end: int):
    # Temporary hack, would be better on per-function call basis.
    # Beacon states take long to fetch, but are rarely fetched.
    timeout = httpx.Timeout(connect_timeout=25.0,
                            read_timeout=30.0,
                            write_timeout=5.0,
                            pool_timeout=10.0)
    async with Eth2HttpClient(options=Eth2HttpOptions(
            api_base_url=eth2_rpc,
            default_req_type=ContentType.json,
            default_resp_type=ContentType.ssz, default_timeout=timeout)) as prov:
        api = prov.extended_api(lighthouse.Eth2API)
        eth2mon = Eth2Monitor(api)
        await run_eth2_feeds(eth2mon, watch, backfill, backfill_start, backfill_end)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run eth2 indexing.')
    parser.add_argument('--db-addr', dest='db_addr', default='sqlite:///testing.db',
                        help='the DB string to use for a connection')
    parser.add_argument('--eth2-addr', dest='eth2_addr', default='http://localhost:5052/',
                        help='the HTTP API address of the Eth2 node to extract data from (currently only Lighthouse)')
    parser.add_argument('-w', '--watch', action='store_true', help='Watch for live blocks')
    parser.add_argument('-b', '--backfill', action='store_true', help='Backfill blocks')
    parser.add_argument('--backfill-start', dest='backfill_start', type=int, default=0, help='Backfill start slot')
    parser.add_argument('--backfill-end', dest='backfill_end', type=int, default=-1, help='Backfill end slot, -1 to backfill until head')
    # TODO: config option. Maybe also a genesis-state option.

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

    print(f"Extracting data from eth2 API: {args.eth2_addr}")
    trio.run(main, args.eth2_addr, args.watch, args.backfill, args.backfill_start, args.backfill_end)

