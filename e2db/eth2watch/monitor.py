import trio
from eth2.models.lighthouse import Eth2API
from eth2spec.phase0 import spec


class Eth2Monitor(object):
    api: Eth2API

    def __init__(self, api: Eth2API) -> None:
        self.api = api

    async def watch_blocks_and_states(self, start_block_root: spec.Root, slot: spec.Slot, dest: trio.MemorySendChannel, poll_interval: float = 2.0):
        last_block_root = start_block_root
        while True:
            # TODO: use heads endpoint to get data for every fork
            # Poll the head
            head = await self.api.beacon.head()
            if head.block_root == last_block_root:
                await trio.sleep(poll_interval)
                continue
            block = await self.api.beacon.block(root=head.block_root)

            # TODO send (pre, post, block) to the channel
            await dest.send()

    async def backfill_blocks_and_states(self, from_slot: spec.Slot, to_slot: spec.Slot,
                            dest: trio.MemorySendChannel, step_slowdown: float = 0.5):
        pass  # TODO send (pre, post, block) to the channel

    # TODO: watch network, forkchoice, gossip, etc.
