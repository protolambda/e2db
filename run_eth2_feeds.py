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


async def run_eth2_feeds(eth2mon: Eth2Monitor, start_backfill: spec.Slot):
    async with trio.open_nursery() as nursery:
        send, recv = trio.open_memory_channel(max_buffer_size=5)
        nursery.start_soon(ev_eth2_state_loop, session, recv)

        head_info = await eth2mon.api.beacon.head()
        # Backfill all the way up to the head. If not canonical, it will be re-orged by the watcher anyway.
        await eth2mon.backfill_cold_chain(start_backfill, head_info.slot, send)
        # After completing the back-fill, start watching for new hot blocks.
        # nursery.start_soon(eth2mon.watch_hot_chain, send)


async def main(eth2_rpc: str):
    async with Eth2HttpClient(options=Eth2HttpOptions(
            api_base_url=eth2_rpc,
            default_req_type=ContentType.json,
            default_resp_type=ContentType.ssz)) as prov:
        api = prov.extended_api(lighthouse.Eth2API)
        eth2mon = Eth2Monitor(api)
        # TODO, continue from old progress
        start_backfill = spec.Slot(590)
        await run_eth2_feeds(eth2mon, start_backfill)


if __name__ == '__main__':
    from sqlalchemy import create_engine
    engine = create_engine('sqlite:///testing.db')

    Base.metadata.create_all(engine, checkfirst=True)

    Session = sessionmaker(bind=engine)
    session = Session()

    trio.run(main, "http://ec2-18-232-73-77.compute-1.amazonaws.com:4000")

