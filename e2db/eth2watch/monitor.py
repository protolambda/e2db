import trio
from typing import Dict, Set, Tuple, Optional
from eth2.models.lighthouse import Eth2API, APIBlock, ForkchoiceData, HeadInfo
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

    state_by_block_slot_cache_dict = LRU(size=10)

    def __init__(self, api: Eth2API) -> None:
        self.api = api

    async def get_state_by_block_and_slot(self, block_root: spec.Root, slot: spec.Slot) -> spec.BeaconState:
        """May fail if the state is being fetched from API"""
        key = (block_root, slot)
        if key not in self.state_by_block_slot_cache_dict:
            print(f"key: ({block_root.hex()}, {slot})")
            print(f"cached: " + ', '.join(f"({b.hex()}, {s})" for b,s in self.state_by_block_slot_cache_dict.keys()))
            print(f"fetching block {block_root.hex()}")
            api_block = await self.api.beacon.block(root=block_root)
            pre_state_root = api_block.beacon_block.message.state_root
            print(f"fetching state {pre_state_root.hex()}")
            api_state = await self.api.beacon.state(root=pre_state_root)
            state = api_state.beacon_state
            if state.slot < slot:
                spec.process_slots(state, slot)
            print(f"computed state {state.hash_tree_root().hex()}")
            await self.cache_state(block_root, state)
            return state.copy()

        out = self.state_by_block_slot_cache_dict[key]
        print(f"out: {out.hash_tree_root().hex()} key: ({block_root.hex()}, {slot})")
        return out.copy()

    async def cache_state(self, block_root: spec.Root, state: spec.BeaconState):
        print(f"caching state (last block: {block_root.hex()}, slot: {state.latest_block_header.slot}) "
              f"{state.hash_tree_root().hex()} (state slot {state.slot})")
        cached = state.copy()
        self.state_by_block_slot_cache_dict[(block_root, state.slot)] = cached

    async def _fetch_state_and_process_block(self, signed_block: Optional[spec.SignedBeaconBlock], dest: trio.MemorySendChannel, is_canon: bool) -> bool:
        block = signed_block.message
        block_root = spec.Root(block.hash_tree_root())
        prev_slot = spec.Slot(block.slot - 1)
        pre_state: spec.BeaconState
        try:
            pre_state = await self.get_state_by_block_and_slot(block.parent_root, prev_slot)
        except Exception as e:
            print(f"Failed to fetch state: {e}")
            return False
        state = pre_state.copy()

        print(f"processing state transition of block {block_root} (slot {block.slot}) "
              f"on state {state.hash_tree_root().hex()} (slot {state.slot}) "
              f"(asked for state of parent {block.parent_root.hex()}, at slot {prev_slot})")

        try:
            unchecked_state_transition(state, signed_block)
        except Exception as e:
            print(f"WARNING: {block_root.hex()} (slot {block.slot}) failed state transition: {e}")
            traceback.print_exc()
            return False
        if block.state_root != state.hash_tree_root():
            print(f"WARNING: {block.hash_tree_root().hex()} (slot {block.slot}) state root ({block.state_root.hex()})"
                  f" does not match computed state root ({state.hash_tree_root().hex()})")
            return False
        await self.cache_state(block_root, state)
        await dest.send((pre_state, state.copy(), signed_block, is_canon))
        return True

    async def _fetch_state_empty_slots(self, state: spec.BeaconState, delta_slots: int, dest: trio.MemorySendChannel, deltas_canon: int) -> spec.BeaconState:
        state = state.copy()
        start_slot = state.slot + 1
        to_slot = start_slot + delta_slots
        # If deltas_canon == 0, then none of the slots are canon
        canon_to_slot = start_slot + deltas_canon
        for slot in range(start_slot, to_slot):
            pre_state = state.copy()
            print(f"processing state transition of empty slot {slot} on pre-state {pre_state.hash_tree_root().hex()})")
            spec.process_slots(state, spec.Slot(slot))
            block_root = spec.Root(state.latest_block_header.hash_tree_root())
            await self.cache_state(block_root, state)
            is_canon = slot < canon_to_slot
            await dest.send((pre_state, state.copy(), None, is_canon))
        return state

    async def watch_hot_chain(self, dest: trio.MemorySendChannel, poll_interval: float = 2.0):
        last_hot_node_roots = set()
        while True:
            # Poll the forkchoice data
            hot: ForkchoiceData
            head_info: HeadInfo
            try:
                hot = await self.api.advanced.fork_choice()
                head_info = await self.api.beacon.head()
            except Exception as e:
                print(f"Failed to fetch fork choice data, error: {e}")
                await trio.sleep(poll_interval)
                continue

            # Get the new set of nodes
            hot_roots = set(hot.indices.keys())
            new_block_roots = hot_roots - last_hot_node_roots
            new_nodes = list(map(lambda r: hot.nodes[hot.indices[r]], new_block_roots))

            if head_info.block_root not in hot_roots:
                print("head root cannot be found in hot data, skipping watch step")
                await trio.sleep(poll_interval)
                continue

            if len(new_nodes) == 0:
                print("no new hot block nodes")
                await trio.sleep(poll_interval)
                continue

            head_node_index = hot.indices[head_info.block_root]
            head_node = hot.nodes[head_node_index]
            if head_node.best_descendant is not None:
                head_node_index = head_node.best_descendant
                head_node = hot.nodes[head_node_index]

            # Block root, mapped to all following empty slots that lead up to a later block
            # Including the slot of the later block itself, before the later block was applied.
            empty_slots: Dict[spec.Root, Set[spec.Slot]] = dict()
            # Similarly, track which of those are canonical. Which is always a subset, and may be empty.
            canon_empty_slots: Dict[spec.Root, Set[spec.Slot]] = dict()
            for node in new_nodes:
                is_canon = node.best_descendant == head_node_index or node.root == head_node.root
                if node.parent is not None:
                    parent = hot.nodes[node.parent]
                    if parent.root not in empty_slots:
                        empty_slots[parent.root] = set()
                    if is_canon:
                        canon_empty_slots[parent.root] = set()
                    for slot in range(parent.slot + 1, node.slot+1):
                        empty_slots[parent.root].add(slot)
                    if is_canon:
                        for slot in range(parent.slot + 1, node.slot+1):
                            canon_empty_slots[parent.root].add(slot)

            # Process all new blocks, sorted by increasing slot.
            sorted_nodes = sorted(hot.nodes, key=lambda x: x.slot)
            processed_node_roots = set()

            for node in sorted_nodes:

                # If the block is new, process it.
                if node.root in new_block_roots:
                    try:
                        print(f"eth2 watcher is fetching block {node.root.hex()}")
                        api_block = await self.api.beacon.block(root=node.root)
                        print(f"fetched block {api_block.root.hex()} (slot {api_block.beacon_block.message.slot}, "
                              f"state root {api_block.beacon_block.message.state_root.hex()}, parent root: {api_block.beacon_block.message.parent_root.hex()}) "
                              f"for node block {node.root.hex()}")
                    except Exception as e:
                        print(f"Failed to fetch block for by root {node.root.hex()}: {e}")
                        await trio.sleep(poll_interval)
                        continue
                    is_canon = node.best_descendant == head_node_index or node.root == head_node.root
                    ok = await self._fetch_state_and_process_block(signed_block=api_block.beacon_block,
                                                                   dest=dest, is_canon=is_canon)
                    if not ok:
                        print(f"state transition/fetch error! slot {node.slot}, block root: {node.root.hex()} "
                              f"state root: {node.state_root.hex()}")
                        continue

                # If the node is a start of a gap to a later unprocessed node, then process the empty slots
                if node.root in empty_slots:
                    pre_state: spec.BeaconState
                    try:
                        pre_state = await self.get_state_by_block_and_slot(node.root, node.slot)
                    except Exception as e:
                        print(f"Failed to fetch state: {e}")
                        continue

                    following_empty_slots = len(empty_slots[node.root])
                    # 0 or more of these empty slots may be leading up to the canonical chain.
                    # Count them, and share that these will be canonical.
                    following_canon_empty_slots = 0 if node.root not in canon_empty_slots else len(canon_empty_slots[node.root])
                    print(f"after {node.root} {following_empty_slots} empty slots follow")
                    await self._fetch_state_empty_slots(state=pre_state,
                                                        delta_slots=following_empty_slots, dest=dest,
                                                        deltas_canon=following_canon_empty_slots)

                # If all processing succeeds, then keep remember the node to not re-do work for it next round
                processed_node_roots.add(node.root)

            # Add all processed nodes
            last_hot_node_roots |= processed_node_roots
            # Remove nodes that are no longer hot
            last_hot_node_roots &= hot_roots

    async def backfill_cold_chain(self, from_slot: spec.Slot, to_slot: spec.Slot,
                                  dest: trio.MemorySendChannel, step_slowdown: float = 0.5):
        genesis = False
        if from_slot == 0:
            genesis = True
            from_slot = 1
        api_block = await self.api.beacon.block(slot=spec.Slot(from_slot-1))
        if genesis:
            api_state = await self.api.beacon.state(slot=spec.Slot(0))
            await self.cache_state(api_block.root, api_state.beacon_state)
            await dest.send((None, api_state.beacon_state, None, True))
        prev_block_root = api_block.root
        print(f"starting block root: {prev_block_root}")
        slot = from_slot
        while slot < to_slot:
            try:
                signed_block = (await self.api.beacon.block(slot=slot)).beacon_block
            except Exception as e:
                print(f"Failed to fetch block for slot {slot}: {e}")
                await trio.sleep(step_slowdown)
                continue

            if signed_block.message.slot < slot:
                print(f"empty slot {slot}")
                # We got the same block again, it's an empty slot.
                pre_state = await self.get_state_by_block_and_slot(prev_block_root, spec.Slot(slot-1))
                await self._fetch_state_empty_slots(pre_state, 1, dest, 1)
                print(f"completed processing empty slot {slot}")
            else:
                print(f"block {signed_block.message.hash_tree_root().hex()} state_root: {signed_block.message.state_root.hex()} parent_root: {signed_block.message.parent_root.hex()} slot: {signed_block.message.slot}")
                ok = await self._fetch_state_and_process_block(
                    signed_block=signed_block, dest=dest, is_canon=True)
                if not ok:
                    print(f"state transition/fetch error! slot {slot}, block: {signed_block.message.hash_tree_root().hex()}")
                    continue
                print(f"completed processing filled slot {slot}")

            slot += 1
            print(f"new prev block root: {prev_block_root.hex()}")

            # Don't spam the serving side with requests too much, pause a little
            await trio.sleep(step_slowdown)

    # TODO: watch network, forkchoice, gossip, etc.
