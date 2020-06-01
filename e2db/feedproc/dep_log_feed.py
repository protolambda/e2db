import trio
from typing import Sequence
from e2db.depwatch.monitor import DepositLog
from e2db.models import DepositTx, DepositData
from eth2spec.phase0 import spec
from sqlalchemy.orm import Session


async def ev_deposit_batch_loop(session: Session, recv: trio.MemoryReceiveChannel):
    ev_batch: Sequence[DepositLog]
    async for ev_batch in recv:
        print(ev_batch)
        for ev in ev_batch:
            data = spec.DepositData(
                pubkey=ev.pubkey,
                withdrawal_credentials=ev.withdrawal_credentials,
                amount=ev.amount,
                signature=ev.signature,
            )
            data_root = data.hash_tree_root()
            session.merge(DepositData(
                data_root=data_root,
                pubkey=ev.pubkey,
                withdrawal_credentials=ev.withdrawal_credentials,
                amount=ev.amount,
                signature=ev.signature,
            ))
            session.merge(DepositTx(
                block_hash=ev.block_hash,
                block_num=ev.block_number,
                tx_index=ev.tx_index,
                tx_hash=ev.tx_hash,
                data_root=data_root,
            ))
        session.commit()
