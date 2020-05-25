import trio
from eth2.core import ContentType
from eth2.models import lighthouse
from eth2.providers.http import Eth2HttpClient, Eth2HttpOptions
from eth2spec.phase0 import spec
from sqlalchemy.orm import sessionmaker

from e2db.eth2watch.monitor import Eth2Monitor
from e2db.feedproc.eth2_state_feed import ev_eth2_state_loop
from e2db.models import Base


async def run_eth2_feeds(eth2mon: Eth2Monitor, start_backfill: spec.Slot, start_watch: spec.Slot):
    async with trio.open_nursery() as nursery:
        send, recv = trio.open_memory_channel(max_buffer_size=100)
        nursery.start_soon(eth2mon.backfill_blocks_and_states, start_backfill, start_watch, send)
        nursery.start_soon(eth2mon.watch_blocks_and_states, start_watch, send)
        nursery.start_soon(ev_eth2_state_loop, recv)


async def main(eth2_rpc: str):
    async with Eth2HttpClient(options=Eth2HttpOptions(
            api_base_url=eth2_rpc,
            default_req_type=ContentType.json,
            default_resp_type=ContentType.ssz)) as prov:
        api = prov.extended_api(lighthouse.Eth2API)
        eth2mon = Eth2Monitor(api)
        # TODO
        start_backfill = spec.Slot(123)
        start_watch = spec.Slot(456)
        await run_eth2_feeds(eth2mon, start_backfill, start_watch)


if __name__ == '__main__':
    from sqlalchemy import create_engine
    engine = create_engine('sqlite:///testing.db')

    Base.metadata.create_all(engine, checkfirst=True)

    Session = sessionmaker(bind=engine)
    session = Session()

    trio.run(main)

