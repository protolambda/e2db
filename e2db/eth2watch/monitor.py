import trio
from typing import Dict, Set, Optional
from eth2.models.lighthouse import Eth2API, APIBlock
from eth2spec.phase0 import spec
from lru import LRU


def unchecked_state_transition(state: spec.BeaconState, signed_block: spec.SignedBeaconBlock) -> spec.BeaconState:
    block = signed_block.message
    # Process slots (including those with no blocks) since block
    if block.slot > state.slot:
        spec.process_slots(state, block.slot)
    # Process block
    spec.process_block(state, block)
    # Return post-state
    return state


class Eth2Monitor(object):
    api: Eth2API
    state_cache_dict = LRU(size=10)

    def __init__(self, api: Eth2API) -> None:
        self.api = api

    async def get_state(self, state_root: spec.Root) -> spec.BeaconState:
        if state_root not in self.state_cache_dict:
            api_state = await self.api.beacon.state(root=state_root)
            self.state_cache_dict[spec.Root(api_state.root)] = api_state.beacon_state
            return api_state.beacon_state
        return self.state_cache_dict[state_root]

    async def _fetch_state_and_process_block(self, block: APIBlock, parent_state_root: spec.Root, dest: trio.MemorySendChannel) -> Optional[spec.BeaconState]:
        pre_state = await self.get_state(parent_state_root)
        state = pre_state.copy()
        try:
            unchecked_state_transition(state, block.block)
        except:
            print(f"WARNING: {block.block_root.hex()} (slot {block.block.message.slot}) failed state transition")
            return None
        if block.block.message.state_root != state.hash_tree_root():
            print(f"WARNING: {block.block_root.hex()} (slot {block.block.message.slot}) state root ({block.block.message.state_root}) does not match computed state root ({state.hash_tree_root()})")
            return None

        await dest.send((pre_state, state, block))
        return state

    async def _fetch_state_empty_slots(self, state: spec.BeaconState, delta_slots: int, dest: trio.MemorySendChannel):
        to_slot = state.slot + delta_slots
        for slot in range(state.slot + 1, to_slot):
            pre_state = state.copy()
            spec.process_slots(state, spec.Slot(slot))
            await dest.send((pre_state, state, None))

    async def watch_hot_chain(self, dest: trio.MemorySendChannel, poll_interval: float = 2.0):
        last_hot_nodes = set()
        while True:
            # Poll the forkchoice data
            hot = await self.api.advanced.fork_choice()
            # Get the new set of nodes
            new_block_roots = set(hot.indices.keys()) - last_hot_nodes
            new_nodes = list(map(lambda r: hot.nodes[hot.indices[r]], new_block_roots))

            if len(new_nodes) == 0:
                await trio.sleep(poll_interval)
                continue

            # Block root, mapped to all following empty slots that lead up to a later block
            # Including the slot of the later block itself, before the later block was applied.
            empty_slots: Dict[spec.Root, Set[spec.Slot]] = dict()
            for node in new_nodes:
                if node.parent is not None:
                    parent = hot.nodes[node.parent]
                    for slot in range(parent.slot + 1, node.slot+1):
                        empty_slots[node.root].add(slot)

            # Process all new blocks, sorted by increasing slot.
            sorted_nodes = sorted(new_nodes, key=lambda x: x.slot)
            # Whenever any empty slots are known, process those in-between.
            for node in sorted_nodes:
                # TODO: genesis case is not handled
                if node.parent is None:
                    print("warning: skipping over block due to missing parent info (it was likely already processed though)")
                    continue

                parent = hot.nodes[node.parent]
                block = await self.api.beacon.block(root=node.root)
                state = await self._fetch_state_and_process_block(
                    block=block, parent_state_root=parent.state_root, dest=dest)

                if node.root in empty_slots:
                    await self._fetch_state_empty_slots(state=state, delta_slots=len(empty_slots[node.root]), dest=dest)

    async def backfill_cold_chain(self, from_slot: spec.Slot, to_slot: spec.Slot,
                                  dest: trio.MemorySendChannel, step_slowdown: float = 0.5):
        if from_slot == 0:
            # TODO handle genesis case
            from_slot = 1
        state = await self.api.beacon.state(slot=spec.Slot(from_slot-1))
        prev_state_root = spec.Root(state.hash_tree_root())
        for slot in range(from_slot, to_slot):
            block = await self.api.beacon.block(slot=spec.Slot(slot))
            state = await self._fetch_state_and_process_block(
                block=block, parent_state_root=prev_state_root, dest=dest)
            prev_state_root = spec.Root(state.hash_tree_root())
            # Don't spam the serving side with requests too much, pause a little
            await trio.sleep(step_slowdown)

    # TODO: watch network, forkchoice, gossip, etc.
