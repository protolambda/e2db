import trio
from typing import Dict, Set, Optional, NamedTuple
from eth2.models.lighthouse import Eth2API, ForkchoiceData, HeadInfo
from eth2spec.phase0 import spec
import eth2fastspec
from lru import LRU
import traceback


def unchecked_state_transition(epc: eth2fastspec.EpochsContext, state: spec.BeaconState, signed_block: spec.SignedBeaconBlock) -> spec.BeaconState:
    block = signed_block.message
    # Process slots (including those with no blocks) since block
    if block.slot > state.slot:
        eth2fastspec.process_slots(epc, state, block.slot)
    # Process block
    eth2fastspec.process_block(epc, state, block)
    # Return post-state
    return state


class CachedState(NamedTuple):
    state: spec.BeaconState
    epc: eth2fastspec.EpochsContext

    def copy(self) -> "CachedState":
        return CachedState(self.state.copy(), self.epc.copy())


class Eth2Monitor(object):
    api: Eth2API

    state_by_block_slot_cache_dict = LRU(size=20)
    state_by_state_root_cache_dict = LRU(size=20)
    block_cache = LRU(size=20)

    def __init__(self, api: Eth2API) -> None:
        self.api = api

    async def get_block(self, block_root: spec.Root) -> spec.SignedBeaconBlock:
        if block_root not in self.block_cache:
            print(f"fetching block {block_root.hex()}")
            api_block = await self.api.beacon.block(root=block_root)
            signed_block = api_block.beacon_block
            await self.cache_signed_block(signed_block)
            return signed_block.copy()

        out = self.block_cache[block_root]
        print(f"block getter out: {out.message.hash_tree_root().hex()} key: ({block_root.hex()})")
        return out.copy()

    async def get_state_by_state_root(self, state_root: spec.Root) -> CachedState:
        if state_root not in self.state_by_state_root_cache_dict:
            print(f"fetching state by state root {state_root.hex()}")
            api_state = await self.api.beacon.state(root=state_root)
            state = api_state.beacon_state
            print(f"retrieved state: {state.hash_tree_root().hex()}")
            temp_header = state.latest_block_header.copy()
            if temp_header.state_root == spec.Bytes32():
                temp_header.state_root = state.hash_tree_root()
            block_root = spec.Root(temp_header.hash_tree_root())
            print(f"last block of retrieved state: {block_root.hex()}")
            epc = eth2fastspec.EpochsContext()
            epc.load_state(state)
            await self.cache_state(block_root, state, epc)
            return CachedState(state, epc)

        out: CachedState = self.state_by_state_root_cache_dict[state_root]
        print(f"state (by state root) getter out: {out.state.hash_tree_root().hex()} key: ({state_root.hex()})")
        return out.copy()

    async def get_state_by_block_and_slot(self, block_root: spec.Root, slot: spec.Slot) -> CachedState:
        """May fail if the state is being fetched from API"""

        key = (block_root, slot)
        if key not in self.state_by_block_slot_cache_dict:
            print(f"key: ({block_root.hex()}, {slot})")
            print(f"cached: " + ', '.join(f"({b.hex()}, {s})" for b,s in self.state_by_block_slot_cache_dict.keys()))
            signed_block = await self.get_block(block_root)
            pre_state_root = signed_block.message.state_root
            print(f"fetching state {pre_state_root.hex()} (from block: {signed_block.message.hash_tree_root().hex()})")
            cached_state = await self.get_state_by_state_root(state_root=pre_state_root)
            if cached_state.state.slot < slot:
                eth2fastspec.process_slots(cached_state.epc, cached_state.state, slot)
            print(f"computed state {cached_state.state.hash_tree_root().hex()}")
            await self.cache_state(block_root, cached_state.state, cached_state.epc)
            return cached_state

        out: CachedState = self.state_by_block_slot_cache_dict[key]
        print(f"state (by block root and slot) getter out: {out.state.hash_tree_root().hex()} key: ({block_root.hex()}, {slot})")
        return out.copy()

    async def cache_signed_block(self, signed_block: spec.SignedBeaconBlock):
        block = signed_block.message
        block_root = spec.Root(block.hash_tree_root())
        print(f"caching block (root: {block_root.hex()}, slot: {block.slot})")
        cached = signed_block.copy()
        self.block_cache[block_root] = cached

    async def cache_state(self, block_root: spec.Root, state: spec.BeaconState, epc: eth2fastspec.EpochsContext):
        print(f"caching state (last block: {block_root.hex()}, slot: {state.latest_block_header.slot}) "
              f"{state.hash_tree_root().hex()} (state slot {state.slot})")
        cached = CachedState(state.copy(), epc.copy())
        self.state_by_block_slot_cache_dict[(block_root, state.slot)] = cached
        self.state_by_state_root_cache_dict[state.hash_tree_root()] = cached

    async def _fetch_state_and_process_block(self, signed_block: Optional[spec.SignedBeaconBlock], dest: trio.MemorySendChannel, is_canon: bool) -> bool:
        block = signed_block.message
        block_root = spec.Root(block.hash_tree_root())
        prev_slot = spec.Slot(block.slot - 1)
        pre_state_cached: CachedState
        try:
            pre_state_cached = await self.get_state_by_block_and_slot(block.parent_root, prev_slot)
        except Exception as e:
            print(f"Failed to fetch state: {e}")
            return False
        state = pre_state_cached.state
        pre_state = state.copy()
        epc = pre_state_cached.epc

        print(f"processing state transition of block {block_root} (slot {block.slot}) "
              f"on state {state.hash_tree_root().hex()} (slot {state.slot}) "
              f"(asked for state of parent {block.parent_root.hex()}, at slot {prev_slot})")

        try:
            unchecked_state_transition(epc, state, signed_block)
        except Exception as e:
            print(f"WARNING: {block_root.hex()} (slot {block.slot}) failed state transition: {e}")
            traceback.print_exc()
            return False
        if block.state_root != state.hash_tree_root():
            print(f"WARNING: {block.hash_tree_root().hex()} (slot {block.slot}) state root ({block.state_root.hex()})"
                  f" does not match computed state root ({state.hash_tree_root().hex()})")
            return False
        await self.cache_state(block_root, state, epc)
        await dest.send((pre_state, state.copy(), signed_block, is_canon))
        return True

    async def _fetch_state_empty_slots(self, epc: eth2fastspec.EpochsContext, state: spec.BeaconState,
                                       delta_slots: int, dest: trio.MemorySendChannel, deltas_canon: int) -> spec.BeaconState:
        state = state.copy()
        epc = epc.copy()
        start_slot = state.slot + 1
        to_slot = start_slot + delta_slots
        # If deltas_canon == 0, then none of the slots are canon
        canon_to_slot = start_slot + deltas_canon
        for slot in range(start_slot, to_slot):
            pre_state = state.copy()
            print(f"processing state transition of empty slot {slot} on pre-state {pre_state.hash_tree_root().hex()})")
            eth2fastspec.process_slots(epc, state, spec.Slot(slot))
            block_root = spec.Root(state.latest_block_header.hash_tree_root())
            await self.cache_state(block_root, state, epc)
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
                        signed_block = await self.get_block(block_root=node.root)
                        block = signed_block.message
                        print(f"fetched block {block.hash_tree_root().hex()} (slot {block.slot}, "
                              f"state root {block.state_root.hex()}, parent root: {block.parent_root.hex()}) "
                              f"for node block {node.root.hex()}")
                    except Exception as e:
                        print(f"Failed to fetch block for by root {node.root.hex()}: {e}")
                        await trio.sleep(poll_interval)
                        continue
                    is_canon = node.best_descendant == head_node_index or node.root == head_node.root
                    ok = await self._fetch_state_and_process_block(signed_block=signed_block,
                                                                   dest=dest, is_canon=is_canon)
                    if not ok:
                        print(f"state transition/fetch error! slot {node.slot}, block root: {node.root.hex()} "
                              f"state root: {node.state_root.hex()}")
                        continue

                # If the node is a start of a gap to a later unprocessed node, then process the empty slots
                if node.root in empty_slots:
                    pre_state_cached: CachedState
                    try:
                        pre_state_cached = await self.get_state_by_block_and_slot(node.root, node.slot)
                    except Exception as e:
                        print(f"Failed to fetch state: {e}")
                        continue

                    following_empty_slots = len(empty_slots[node.root])
                    # 0 or more of these empty slots may be leading up to the canonical chain.
                    # Count them, and share that these will be canonical.
                    following_canon_empty_slots = 0 if node.root not in canon_empty_slots else len(canon_empty_slots[node.root])
                    print(f"after {node.root} {following_empty_slots} empty slots follow")
                    await self._fetch_state_empty_slots(epc=pre_state_cached.epc,
                                                        state=pre_state_cached.state,
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
        # Don't try to catch an exception here, if the first thing fails, then we restart the process as a whole.
        api_block = await self.api.beacon.block(slot=spec.Slot(from_slot-1))
        if genesis:
            api_state = await self.api.beacon.state(slot=spec.Slot(0))
            epc = eth2fastspec.EpochsContext()
            epc.load_state(api_state.beacon_state)
            await self.cache_state(api_block.root, api_state.beacon_state, epc)
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
                try:
                    pre_state_cached = await self.get_state_by_block_and_slot(prev_block_root, spec.Slot(slot-1))
                except Exception as e:
                    print(f"Failed to fetch state for slot {slot} after block {prev_block_root.hex()}: {e}")
                    await trio.sleep(step_slowdown)
                    continue
                await self._fetch_state_empty_slots(pre_state_cached.epc, pre_state_cached.state, 1, dest, 1)
                print(f"completed processing empty slot {slot}")
            else:
                print(f"block {signed_block.message.hash_tree_root().hex()} state_root: {signed_block.message.state_root.hex()} parent_root: {signed_block.message.parent_root.hex()} slot: {signed_block.message.slot}")
                ok = await self._fetch_state_and_process_block(
                    signed_block=signed_block, dest=dest, is_canon=True)
                if not ok:
                    print(f"state transition/fetch error! slot {slot}, block: {signed_block.message.hash_tree_root().hex()}")
                    continue
                print(f"completed processing filled slot {slot}")
                prev_block_root = spec.Root(signed_block.message.hash_tree_root())
                print(f"backfill: new prev block root: {prev_block_root.hex()}")

            slot += 1
            print(f"backfill: new slot: {slot}")

            # Don't spam the serving side with requests too much, pause a little
            await trio.sleep(step_slowdown)

    # TODO: watch network, forkchoice, gossip, etc.
