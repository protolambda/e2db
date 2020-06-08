import trio
from typing import Type, List as PyList, Set, Dict
from typing import Optional
from itertools import zip_longest
from e2db.models import (
    Fork, BeaconState, Validator, ValidatorStatus, ValidatorEpochBalance, ValidatorOddBalance,
    BeaconBlockBody, BeaconBlock, SignedBeaconBlock,
    Eth1Data, Eth1BlockVote,
    ProposerSlashing, ProposerSlashingInclusion,
    AttesterSlashing, AttesterSlashingInclusion,
    AttestationData, IndexedAttestation, PendingAttestation,
    DepositData, Deposit, DepositInclusion,
    SignedVoluntaryExit, SignedVoluntaryExitInclusion,
    Checkpoint, format_epoch, BitsAttestation,
    CanonBeaconBlock, CanonBeaconState,
    Base
)
from eth2spec.phase0 import spec
from sqlalchemy_mate import ExtendedBase

from sqlalchemy.orm import Session

from sqlalchemy.dialects import postgresql
import traceback


def upsert_all(session: Session, table: Type[Base], data: PyList[ExtendedBase]):
    if session.bind.dialect.name == "postgresql":
        # Special postgres dialect upsert, to avoid talking to the database for every value individually
        values = list(map(lambda x: x.to_dict(), data))
        insert_stmt = postgresql.insert(table.__table__).values(values)
        pk = insert_stmt.table.primary_key
        update_columns = {col.name: col for col in insert_stmt.excluded if col.name not in pk}
        update_stmt = insert_stmt.on_conflict_do_update(
            index_elements=pk,
            set_=update_columns,
        )
        session.execute(update_stmt)
    else:
        # slower, but for sqlite it's fast anyway
        table: ExtendedBase
        # noinspection PyTypeChecker
        table.upsert_all(session, data)


def upsert(session: Session, inst: ExtendedBase):
    # noinspection PyTypeChecker
    upsert_all(session, inst.__class__, [inst])


def store_state(session: Session, state: spec.BeaconState):
    state_root = state.hash_tree_root()
    eth1_data = state.eth1_data
    eth1_data_root = eth1_data.hash_tree_root()
    upsert(session, Eth1Data(
        data_root=eth1_data_root,
        deposit_root=eth1_data.deposit_root,
        deposit_count=eth1_data.deposit_count,
        block_hash=eth1_data.block_hash,
    ))
    fork = state.fork
    upsert(session, Fork(
        current_version=fork.current_version,
        previous_version=fork.previous_version,
        epoch=fork.epoch,
    ))

    prev_just_ch = state.previous_justified_checkpoint
    prev_just_ch_root = prev_just_ch.hash_tree_root()
    upsert(session, Checkpoint(
        checkpoint_root=prev_just_ch_root,
        epoch=prev_just_ch.epoch,
        block_root=prev_just_ch.root,
    ))
    curr_just_ch = state.current_justified_checkpoint
    curr_just_ch_root = curr_just_ch.hash_tree_root()
    upsert(session, Checkpoint(
        checkpoint_root=curr_just_ch_root,
        epoch=curr_just_ch.epoch,
        block_root=curr_just_ch.root,
    ))
    finalized_ch = state.finalized_checkpoint
    finalized_ch_root = finalized_ch.hash_tree_root()
    upsert(session, Checkpoint(
        checkpoint_root=finalized_ch_root,
        epoch=finalized_ch.epoch,
        block_root=finalized_ch.root,
    ))

    header = state.latest_block_header.copy()
    if header.state_root == spec.Bytes32():
        header.state_root = state_root
    header_root = header.hash_tree_root()
    upsert(session, BeaconState(
        state_root=state_root,
        latest_block_root=header_root,
        slot=state.slot,
        eth1_data_root=eth1_data_root,
        fork=fork.current_version,
        eth1_deposit_index=state.eth1_deposit_index,
        validators_root=state.validators.hash_tree_root(),
        balances=state.hash_tree_root(),
        total_slashings=spec.Gwei(sum(state.slashings.readonly_iter())),
        prev_epoch_att_count=len(state.previous_epoch_attestations),
        curr_epoch_att_count=len(state.current_epoch_attestations),
        justification_bits=''.join('1' if state.justification_bits[i] else '0' for i in range(spec.JUSTIFICATION_BITS_LENGTH)),
        prev_just_checkpoint=prev_just_ch_root,
        curr_just_checkpoint=curr_just_ch_root,
        finalized_checkpoint=finalized_ch_root,
    ))


def store_validator_all(session: Session, curr_state: spec.BeaconState, is_canon: bool):
    header = curr_state.latest_block_header.copy()
    header.state_root = curr_state.hash_tree_root()
    block_root = header.hash_tree_root()
    slot = curr_state.slot

    result_validators = []
    result_validator_statuses = []
    result_balances = []
    epoch = spec.compute_epoch_at_slot(slot)
    for i, (v, b) in enumerate(zip(curr_state.validators.readonly_iter(), curr_state.balances.readonly_iter())):
        # Create new validator
        print(f"val cre {block_root.hex()} {i}")
        result_validators.append(Validator(
            intro_block_root=block_root,
            validator_index=i,
            intro_slot=slot,
            pubkey=v.pubkey,
            withdrawal_credentials=v.withdrawal_credentials,
        ))
        # Update validator status if it's a new or changed validator
        result_validator_statuses.append(ValidatorStatus(
            intro_block_root=block_root,
            validator_index=i,
            intro_slot=slot,
            effective_balance=v.effective_balance,
            slashed=bool(v.slashed),
            activation_eligibility_epoch=format_epoch(v.activation_eligibility_epoch),
            activation_epoch=format_epoch(v.activation_epoch),
            exit_epoch=format_epoch(v.exit_epoch),
            withdrawable_epoch=format_epoch(v.withdrawable_epoch),
        ))
        # And its balance if canon and epoch
        if is_canon:
            result_balances.append(ValidatorEpochBalance(
                epoch=epoch,
                validator_index=i,
                balance=b,
                eff_balance=v.effective_balance,
            ))
    if len(result_validators) > 0:
        upsert_all(session, Validator, result_validators)
    if len(result_validator_statuses) > 0:
        upsert_all(session, ValidatorStatus, result_validator_statuses)
    if len(result_balances):
        upsert_all(session, ValidatorEpochBalance, result_balances)


def store_validator_diff(session: Session, prev_state: spec.BeaconState, curr_state: spec.BeaconState, is_canon: bool):
    header = curr_state.latest_block_header.copy()
    header.state_root = curr_state.hash_tree_root()
    block_root = header.hash_tree_root()
    slot = curr_state.slot

    if prev_state.validators.hash_tree_root() != curr_state.validators.hash_tree_root():
        prev: Optional[spec.Validator]
        curr: Optional[spec.Validator]

        print("checking validators diff")

        # First put them in lists to avoid len() lookups reducing it to O(n^2) performance.
        prev_vals = list(prev_state.validators.readonly_iter())
        curr_vals = list(curr_state.validators.readonly_iter())
        result_validators = []
        result_validator_statuses = []
        for i, (prev, curr) in enumerate(zip_longest(prev_vals, curr_vals)):
            assert curr is not None
            if prev is None:
                # Create new validator
                result_validators.append(Validator(
                    intro_block_root=block_root,
                    validator_index=i,
                    intro_slot=slot,
                    pubkey=curr.pubkey,
                    withdrawal_credentials=curr.withdrawal_credentials,
                ))
            print(f"val {block_root.hex()} {i}")
            if prev is None or prev != curr:
                # Update validator status if it's a new or changed validator
                result_validator_statuses.append(ValidatorStatus(
                    intro_block_root=block_root,
                    validator_index=i,
                    intro_slot=slot,
                    effective_balance=curr.effective_balance,
                    slashed=bool(curr.slashed),
                    activation_eligibility_epoch=format_epoch(curr.activation_eligibility_epoch),
                    activation_epoch=format_epoch(curr.activation_epoch),
                    exit_epoch=format_epoch(curr.exit_epoch),
                    withdrawable_epoch=format_epoch(curr.withdrawable_epoch),
                ))
        if len(result_validators) > 0:
            print("Upserting validators")
            upsert_all(session, Validator, result_validators)
        if len(result_validator_statuses) > 0:
            print("Upserting validator statuses")
            upsert_all(session, ValidatorStatus, result_validator_statuses)

    if is_canon:
        if slot % spec.SLOTS_PER_EPOCH == 0:
            epoch = spec.compute_epoch_at_slot(slot)
            curr_bal: spec.Gwei
            curr_val: spec.Validator

            print("checking epoch balances diff")
            curr_bals = list(curr_state.balances.readonly_iter())
            curr_vals = list(curr_state.validators.readonly_iter())
            result_balances = []
            for i, (curr_bal, curr_val) in enumerate(zip(curr_bals, curr_vals)):
                result_balances.append(ValidatorEpochBalance(
                    epoch=epoch,
                    validator_index=i,
                    balance=curr_bal,
                    eff_balance=curr_val.effective_balance,
                ))
            upsert_all(session, ValidatorEpochBalance, result_balances)
        elif prev_state.balances.hash_tree_root() != curr_state.balances.hash_tree_root():
            prev_bals = list(prev_state.balances.readonly_iter())
            curr_bals = list(curr_state.balances.readonly_iter())
            result_balances = []
            for i, (prev, curr) in enumerate(zip_longest(prev_bals, curr_bals)):
                if prev is None or prev != curr:
                    # Only track changes, and key by block-root, to be able to reorg without overwrite/deletes.
                    result_balances.append(ValidatorOddBalance(
                        intro_block_root=block_root,
                        intro_slot=slot,
                        validator_index=i,
                        balance=curr,
                    ))
            if len(result_balances) > 0:
                print("Upserting odd validator balances")
                upsert_all(session, ValidatorOddBalance, result_balances)


def store_block(session: Session, post_state: spec.BeaconState, signed_block: spec.SignedBeaconBlock):
    block = signed_block.message
    block_root = block.hash_tree_root()
    body = block.body

    # Eth1
    eth1_data = body.eth1_data
    eth1_data_root = eth1_data.hash_tree_root()
    upsert(session, Eth1Data(
        data_root=eth1_data_root,
        deposit_root=eth1_data.deposit_root,
        deposit_count=eth1_data.deposit_count,
        block_hash=eth1_data.block_hash,
    ))
    upsert(session, Eth1BlockVote(
        beacon_block_root=block_root,
        slot=block.slot,
        eth1_data_root=eth1_data_root,
        proposer_index=block.proposer_index,
    ))

    def handle_header(block: spec.BeaconBlockHeader):
        upsert(session, BeaconBlock(
            block_root=block.hash_tree_root(),
            slot=block.slot,
            proposer_index=block.proposer_index,
            parent_root=block.parent_root,
            state_root=block.state_root,
            body_root=block.body_root,
        ))

    def handle_signed_header(signed_block: spec.SignedBeaconBlockHeader):
        upsert(session, SignedBeaconBlock(
            root=signed_block.hash_tree_root(),
            signature=signed_block.signature,
            block_root=signed_block.message.hash_tree_root(),
        ))

    # Ugly but effective: collect operations, ensuring they are unique first, and then upsert as batch.

    # Proposer slashings
    proposer_slashing: spec.ProposerSlashing
    result_prop_slashing = []
    result_prop_slashing_inc = []
    for i, proposer_slashing in enumerate(body.proposer_slashings.readonly_iter()):
        handle_header(proposer_slashing.signed_header_1.message)
        handle_header(proposer_slashing.signed_header_2.message)
        handle_signed_header(proposer_slashing.signed_header_1)
        handle_signed_header(proposer_slashing.signed_header_2)
        result_prop_slashing.append(ProposerSlashing(
            root=proposer_slashing.hash_tree_root(),
            signed_header_1=proposer_slashing.signed_header_1.hash_tree_root(),
            signed_header_2=proposer_slashing.signed_header_2.hash_tree_root(),
        ))
        result_prop_slashing_inc.append(ProposerSlashingInclusion(
            intro_block_root=block_root,
            intro_index=i,
            root=proposer_slashing.hash_tree_root(),
        ))
    if len(result_prop_slashing) > 0:
        upsert_all(session, ProposerSlashing, result_prop_slashing)
    if len(result_prop_slashing_inc) > 0:
        upsert_all(session, ProposerSlashingInclusion, result_prop_slashing_inc)

    result_checkpoints: Set[spec.Checkpoint] = set()
    result_att_datas: Set[spec.AttestationData] = set()

    def handle_att_data(data: spec.AttestationData):
        result_checkpoints.add(data.source)
        result_checkpoints.add(data.target)
        result_att_datas.add(data)

    result_indexed_atts: Set[spec.IndexedAttestation] = set()
    bits_to_indexed: Dict[spec.Root, spec.Root] = dict()

    def handle_indexed_att(indexed: spec.IndexedAttestation):
        result_indexed_atts.add(indexed)

    # Attester slashings
    attester_slashing: spec.AttesterSlashing
    result_att_slashing = []
    result_att_slashing_inc = []
    for i, attester_slashing in enumerate(body.attester_slashings.readonly_iter()):
        handle_att_data(attester_slashing.attestation_1.data)
        handle_att_data(attester_slashing.attestation_2.data)
        handle_indexed_att(attester_slashing.attestation_1)
        handle_indexed_att(attester_slashing.attestation_2)
        result_att_slashing.append(AttesterSlashing(
            root=attester_slashing.hash_tree_root(),
            attestation_1=attester_slashing.attestation_1.hash_tree_root(),
            attestation_2=attester_slashing.attestation_2.hash_tree_root(),
        ))
        result_att_slashing_inc.append(AttesterSlashingInclusion(
            intro_block_root=block_root,
            intro_index=i,
            root=attester_slashing.hash_tree_root(),
        ))

    # Attestations
    attestation: spec.Attestation
    result_pending_atts: PyList[spec.IndexedAttestation] = []
    for i, attestation in enumerate(body.attestations.readonly_iter()):
        data = attestation.data
        handle_att_data(data)
        indexed = spec.get_indexed_attestation(post_state, attestation)
        bits_to_indexed[spec.Root(attestation.hash_tree_root())] = spec.Root(indexed.hash_tree_root())
        handle_indexed_att(indexed)
        result_pending_atts.append(indexed)
    if len(result_checkpoints) > 0:
        upsert_all(session, Checkpoint, [
            Checkpoint(
                checkpoint_root=ch.hash_tree_root(),
                epoch=ch.epoch,
                block_root=ch.root,
            ) for ch in result_checkpoints
        ])
    if len(result_att_datas) > 0:
        upsert_all(session, AttestationData, [
            AttestationData(
                att_data_root=data.hash_tree_root(),
                slot=data.slot,
                index=data.index,
                beacon_block_root=data.beacon_block_root,
                source=data.source.hash_tree_root(),
                target=data.target.hash_tree_root(),
            ) for data in result_att_datas
        ])
    if len(bits_to_indexed) > 0:
        upsert_all(session, BitsAttestation, [
            BitsAttestation(
                bits_attestation_root=attestation_root,
                indexed_attestation_root=indexed_root,
            ) for attestation_root, indexed_root in bits_to_indexed.items()
        ])
    if len(result_indexed_atts) > 0:
        upsert_all(session, IndexedAttestation, [
            IndexedAttestation(
                indexed_attestation_root=indexed.hash_tree_root(),
                attesting_indices=', '.join(map(str, indexed.attesting_indices.readonly_iter())),
                data=indexed.data.hash_tree_root(),
                signature=indexed.signature,
            ) for indexed in result_indexed_atts
        ])
    if len(result_pending_atts) > 0:
        upsert_all(session, PendingAttestation, [
            PendingAttestation(
                intro_block_root=block_root,
                intro_index=i,
                indexed_att=indexed.hash_tree_root(),
                inclusion_delay=block.slot - indexed.data.slot,
                proposer_index=block.proposer_index,
            ) for i, indexed in enumerate(result_pending_atts)
        ])

    # After inserting the attestations, do the attester slashings (attestations may be foreign key)
    if len(result_att_slashing) > 0:
        upsert_all(session, AttesterSlashing, result_att_slashing)
    if len(result_att_slashing_inc) > 0:
        upsert_all(session, AttesterSlashingInclusion, result_att_slashing_inc)

    # Deposits
    deposit: spec.Deposit
    pre_dep_count = post_state.eth1_deposit_index - len(body.deposits)
    result_dep_datas: Set[spec.DepositData] = set()
    result_deps: PyList[Deposit] = []
    result_dep_incl: PyList[DepositInclusion] = []
    for i, deposit in enumerate(body.deposits.readonly_iter()):
        data = deposit.data
        dep_data_root = data.hash_tree_root()
        result_dep_datas.add(data)
        result_deps.append(Deposit(
            root=deposit.hash_tree_root(),
            deposit_index=pre_dep_count + i,
            dep_tree_root=post_state.eth1_data.deposit_root,
            data=dep_data_root,
        ))
        result_dep_incl.append(DepositInclusion(
            intro_block_root=block_root,
            intro_index=i,
            root=deposit.hash_tree_root(),
        ))
    if len(result_dep_datas) > 0:
        upsert_all(session, DepositData, [
            DepositData(
                data_root=data.hash_tree_root(),
                pubkey=data.pubkey,
                withdrawal_credentials=data.withdrawal_credentials,
                amount=data.amount,
                signature=data.signature,
            ) for data in result_dep_datas
        ])
    if len(result_deps) > 0:
        upsert_all(session, Deposit, result_deps)
    if len(result_dep_incl) > 0:
        upsert_all(session, DepositInclusion, result_dep_incl)

    # Voluntary Exits
    sig_vol_exit: spec.SignedVoluntaryExit
    vol_exits = list(body.voluntary_exits.readonly_iter())
    if len(vol_exits) > 0:
        upsert_all(session, SignedVoluntaryExit, [
            SignedVoluntaryExit(
                root=sig_vol_exit.hash_tree_root(),
                epoch=sig_vol_exit.message.epoch,
                validator_index=sig_vol_exit.message.validator_index,
                signature=sig_vol_exit.signature,
            ) for sig_vol_exit in vol_exits
        ])
        upsert_all(session, SignedVoluntaryExitInclusion, [
            SignedVoluntaryExitInclusion(
                intro_block_root=block_root,
                intro_index=i,
                root=sig_vol_exit.hash_tree_root(),
            ) for i, sig_vol_exit in enumerate(vol_exits)
        ])

    # The body
    upsert(session, BeaconBlockBody(
        body_root=body.hash_tree_root(),
        randao_reveal=body.randao_reveal,
        eth1_data_root=body.eth1_data.hash_tree_root(),
        graffiti=body.graffiti,
        # Operations
        proposer_slashings_count=len(body.proposer_slashings),
        attester_slashings_count=len(body.attester_slashings),
        attestations_count=len(body.attestations),
        deposits_count=len(body.deposits),
        voluntary_exits_count=len(body.voluntary_exits),
    ))

    # The block itself
    upsert(session, BeaconBlock(
        block_root=block_root,
        slot=block.slot,
        proposer_index=block.proposer_index,
        parent_root=block.parent_root,
        state_root=block.state_root,
        body_root=body.hash_tree_root(),
    ))

    # Block signature
    upsert(session, SignedBeaconBlock(
        root=signed_block.hash_tree_root(),
        signature=signed_block.signature,
        block_root=block_root,
    ))


def calc_beacon_proposer_index(state: BeaconState, slot: spec.Slot) -> spec.ValidatorIndex:
    epoch = spec.compute_epoch_at_slot(slot)
    seed = spec.hash(spec.get_seed(state, epoch, spec.DOMAIN_BEACON_PROPOSER) + spec.int_to_bytes(state.slot, length=8))
    indices = spec.get_active_validator_indices(state, epoch)
    return spec.compute_proposer_index(state, indices, seed)


def store_canon_chain(session: Session, post: spec.BeaconState,
                      signed_block: Optional[spec.SignedBeaconBlock]):
    proposer_index: spec.ValidatorIndex
    if signed_block is not None:
        block = signed_block.message
        assert post.slot == block.slot
        upsert(session, CanonBeaconBlock(
            slot=block.slot,
            block_root=block.hash_tree_root(),
        ))
        proposer_index = block.proposer_index
    else:
        proposer_index = calc_beacon_proposer_index(post, post.slot)

    upsert(session, CanonBeaconState(
        slot=post.slot,
        state_root=post.hash_tree_root(),
        proposer_index=proposer_index,
        empty_slot=(signed_block is None)
    ))


async def ev_eth2_state_loop(session: Session, recv: trio.MemoryReceiveChannel):
    prev_state: spec.BeaconState
    state: spec.BeaconState
    block: Optional[spec.SignedBeaconBlock]
    async for (prev_state, post_state, block, is_canon) in recv:
        try:
            if block is not None:
                print(f"storing block {block.hash_tree_root().hex()}")
                store_block(session, post_state, signed_block=block)
            print(f"storing post-state {post_state.hash_tree_root().hex()}")
            store_state(session, post_state)
            if prev_state is None:
                print(f"storing full validator set of post-state {post_state.hash_tree_root().hex()}")
                store_validator_all(session, post_state, is_canon)
            else:
                print(f"storing validator diff between pre {prev_state.hash_tree_root().hex()}"
                      f" and post {post_state.hash_tree_root().hex()}")
                store_validator_diff(session, prev_state, post_state, is_canon)
            if is_canon:
                print(f"storing canonical ref to post-state {post_state.hash_tree_root().hex()}")
                store_canon_chain(session, post_state, block)
            session.commit()
        except Exception as e:
            print(f"Error: Failed to store state! {post_state.hash_tree_root().hex()}: {e}")
            traceback.print_exc()
