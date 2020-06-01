import trio
from typing import Dict, Set, Optional
from eth2.models.lighthouse import Eth2API, APIBlock, ForkchoiceData
from eth2spec.phase0 import spec
from lru import LRU
import traceback


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
        """May fail if the state is being fetched from API"""
        if state_root not in self.state_cache_dict:
            print("FETCHING STATE")
            api_state = await self.api.beacon.state(root=state_root)
            self.state_cache_dict[spec.Root(api_state.root)] = api_state.beacon_state
            return api_state.beacon_state
        return self.state_cache_dict[state_root]

    async def cache_state(self, state: spec.BeaconState):
        state_root = spec.Root(state.hash_tree_root())
        self.state_cache_dict[state_root] = state.copy()

    async def _fetch_state_and_process_block(self, api_block: Optional[APIBlock], parent_state_root: spec.Root, dest: trio.MemorySendChannel) -> Optional[spec.BeaconState]:
        pre_state: spec.BeaconState
        try:
            pre_state = await self.get_state(parent_state_root)
        except Exception as e:
            print(f"Failed to fetch state: {e}")
            return None
        state = pre_state.copy()
        try:
            print(f"processing state transition of block {api_block.root} (slot {api_block.beacon_block.message.slot})")
            unchecked_state_transition(state, api_block.beacon_block)
        except Exception as e:
            print(f"WARNING: {api_block.root.hex()} (slot {api_block.beacon_block.message.slot}) failed state transition: {e}")
            traceback.print_exc()
            return None
        if api_block.beacon_block.message.state_root != state.hash_tree_root():
            print(f"WARNING: {api_block.beacon_block.message.hash_tree_root().hex()} (slot {api_block.beacon_block.message.slot}) state root ({api_block.beacon_block.message.state_root.hex()}) does not match computed state root ({state.hash_tree_root().hex()})")
            expected_state = await self.api.beacon.state(slot=spec.Slot(api_block.beacon_block.message.slot))
            print(f"expected state root (root from API provider): {expected_state.root.hex()}")
            return None
        await self.cache_state(state)
        await dest.send((pre_state, state, api_block.beacon_block))
        return state

    async def _fetch_state_empty_slots(self, state: spec.BeaconState, delta_slots: int, dest: trio.MemorySendChannel) -> spec.BeaconState:
        to_slot = state.slot + delta_slots
        for slot in range(state.slot + 1, to_slot + 1):
            pre_state = state.copy()
            print(f"processing state transition of empty slot {slot} on pre-state {pre_state.hash_tree_root().hex()})")
            spec.process_slots(state, spec.Slot(slot))
            await self.cache_state(state)
            await dest.send((pre_state, state, None))
        return state

    async def watch_hot_chain(self, dest: trio.MemorySendChannel, poll_interval: float = 2.0):
        last_hot_node_roots = set()
        while True:
            # Poll the forkchoice data
            hot: ForkchoiceData
            try:
                hot = await self.api.advanced.fork_choice()
            except Exception as e:
                print(f"Failed to fetch fork choice data, error: {e}")
                await trio.sleep(poll_interval)
                continue

            # Get the new set of nodes
            hot_roots = set(hot.indices.keys())
            new_block_roots = hot_roots - last_hot_node_roots
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
                    if parent.root not in empty_slots:
                        empty_slots[parent.root] = set()
                    for slot in range(parent.slot + 1, node.slot+1):
                        empty_slots[parent.root].add(slot)

            # Process all new blocks, sorted by increasing slot.
            sorted_nodes = sorted(hot.nodes, key=lambda x: x.slot)
            processed_node_roots = set()

            for node in sorted_nodes:

                # If the block is new, process it.
                if node.root in new_block_roots:
                    # Genesis case is handled as part of the backfill.
                    # (TODO: maybe inject genesis state ahead of chain start?)
                    if node.parent is None:
                        print("warning: skipping over block due to missing parent info (it was likely already processed though)")
                        continue

                    parent = hot.nodes[node.parent]

                    try:
                        print(f"eth2 watcher is fetching block {node.root.hex()}")
                        block = await self.api.beacon.block(root=node.root)
                    except Exception as e:
                        print(f"Failed to fetch block for by root {node.root.hex()}: {e}")

                        await trio.sleep(poll_interval)
                        continue

                    out_state = await self._fetch_state_and_process_block(
                        api_block=block, parent_state_root=parent.state_root, dest=dest)
                    if out_state is None:
                        print(f"state transition/fetch error! slot {node.slot}, pre-state: {parent.state_root.hex()}")
                        continue

                # If the node is a start of a gap to a later unprocessed node, then process the empty slots
                if node.root in empty_slots:
                    pre_state: spec.BeaconState
                    try:
                        pre_state = await self.get_state(node.state_root)
                    except Exception as e:
                        print(f"Failed to fetch state: {e}")
                        continue

                    await self._fetch_state_empty_slots(state=pre_state, delta_slots=len(empty_slots[node.root]), dest=dest)

                # If all processing succeeds, then keep remember the node to not re-do work for it next round
                processed_node_roots.add(node.root)

            # Add all processed nodes
            last_hot_node_roots += processed_node_roots
            # Remove nodes that are no longer hot
            last_hot_node_roots &= hot_roots

    async def backfill_cold_chain(self, from_slot: spec.Slot, to_slot: spec.Slot,
                                  dest: trio.MemorySendChannel, step_slowdown: float = 0.5):
        genesis = False
        if from_slot == 0:
            genesis = True
            from_slot = 1
        api_state = await self.api.beacon.state(slot=spec.Slot(from_slot-1))
        await self.cache_state(api_state.beacon_state)
        if genesis:
            await dest.send((None, api_state.beacon_state, None))
        prev_state_root = api_state.root
        print(f"latest header: {api_state.beacon_state.latest_block_header}")
        for slot in range(from_slot, to_slot):
            try:
                block = await self.api.beacon.block(slot=spec.Slot(slot))
            except Exception as e:
                print(f"Failed to fetch block for slot {slot}: {e}")
                await trio.sleep(step_slowdown)
                continue
            out_state: spec.BeaconState
            if block.beacon_block.message.slot < slot:
                print(f"empty slot {slot}")
                # We got the same block again, it's an empty slot.
                pre_state = await self.get_state(prev_state_root)
                out_state = await self._fetch_state_empty_slots(pre_state, 1, dest)
                print("completed filling empty slot data")
            else:
                print(f"block state root {block.beacon_block.message.state_root.hex()}")
                out_state = await self._fetch_state_and_process_block(
                    api_block=block, parent_state_root=prev_state_root, dest=dest)
                if out_state is None:
                    print(f"state transition/fetch error! slot {slot}, pre-state: {prev_state_root.hex()}")
                    continue
                print("completed filling filled slot data")
            print(f"latest header: {out_state.latest_block_header}")
            prev_state_root = spec.Root(out_state.hash_tree_root())
            print(f"new prev root: {prev_state_root.hex()}")
            # Don't spam the serving side with requests too much, pause a little
            await trio.sleep(step_slowdown)

    # TODO: watch network, forkchoice, gossip, etc.
