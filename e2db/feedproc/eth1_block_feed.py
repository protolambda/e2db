import trio
from typing import Sequence
from e2db.depwatch.monitor import EnhancedEth1Block
from e2db.models import Eth1Block, CanonEth1Block
from sqlalchemy.orm import Session


async def ev_eth1_block_loop(session: Session, recv: trio.MemoryReceiveChannel):
    ev_batch: Sequence[EnhancedEth1Block]
    async for ev in recv:
        print(f"storing eth1 block: {ev.eth1_block.block_hash.hex()}")
        session.merge(Eth1Block(
            block_hash=ev.eth1_block.block_hash,
            parent_hash=ev.eth1_block.parent_hash,
            block_num=ev.eth1_block.number,
            timestamp=ev.eth1_block.timestamp,
            deposit_count=ev.deposit_count,
        ))
        session.merge(CanonEth1Block(
            block_num=ev.eth1_block.number,
            block_hash=ev.eth1_block.block_hash,
        ))
        session.commit()
