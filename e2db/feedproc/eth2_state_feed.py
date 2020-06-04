import trio
from typing import Optional
from itertools import zip_longest
from e2db.models import (
    Fork, BeaconState, Validator, ValidatorStatus, ValidatorBalance,
    BeaconBlockBody, BeaconBlock, SignedBeaconBlock,
    Eth1Data, Eth1BlockVote,
    ProposerSlashing, ProposerSlashingInclusion,
    AttesterSlashing, AttesterSlashingInclusion,
    AttestationData, IndexedAttestation, PendingAttestation,
    DepositData, Deposit, DepositInclusion,
    SignedVoluntaryExit, SignedVoluntaryExitInclusion,
    Checkpoint, format_epoch, BitsAttestation,
    CanonBeaconBlock, CanonBeaconState,
)
from eth2spec.phase0 import spec

from sqlalchemy.orm import Session


def store_state(session: Session, state: spec.BeaconState):
    state_root = state.hash_tree_root()
    eth1_data = state.eth1_data
    eth1_data_root = eth1_data.hash_tree_root()
    session.merge(Eth1Data(
        data_root=eth1_data_root,
        deposit_root=eth1_data.deposit_root,
        deposit_count=eth1_data.deposit_count,
        block_hash=eth1_data.block_hash,
    ))
    fork = state.fork
    session.merge(Fork(
        current_version=fork.current_version,
        previous_version=fork.previous_version,
        epoch=fork.epoch,
    ))

    prev_just_ch = state.previous_justified_checkpoint
    prev_just_ch_root = prev_just_ch.hash_tree_root()
    session.merge(Checkpoint(
        checkpoint_root=prev_just_ch_root,
        epoch=prev_just_ch.epoch,
        block_root=prev_just_ch.root,
    ))
    curr_just_ch = state.current_justified_checkpoint
    curr_just_ch_root = curr_just_ch.hash_tree_root()
    session.merge(Checkpoint(
        checkpoint_root=curr_just_ch_root,
        epoch=curr_just_ch.epoch,
        block_root=curr_just_ch.root,
    ))
    finalized_ch = state.finalized_checkpoint
    finalized_ch_root = finalized_ch.hash_tree_root()
    session.merge(Checkpoint(
        checkpoint_root=finalized_ch_root,
        epoch=finalized_ch.epoch,
        block_root=finalized_ch.root,
    ))

    header = state.latest_block_header.copy()
    header.state_root = state_root
    header_root = header.hash_tree_root()
    session.merge(BeaconState(
        state_root=state_root,
        latest_block_root=header_root,
        slot=state.slot,
        eth1_data_root=eth1_data_root,
        fork=fork.current_version,
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


def store_validator_all(session: Session, curr_state: spec.BeaconState):
    header = curr_state.latest_block_header.copy()
    header.state_root = curr_state.hash_tree_root()
    block_root = header.hash_tree_root()
    slot = curr_state.slot

    for i, (v, b) in enumerate(zip(curr_state.validators.readonly_iter(), curr_state.balances.readonly_iter())):
        # Create new validator
        session.merge(Validator(
            intro_block_root=block_root,
            validator_index=i,
            intro_slot=slot,
            pubkey=v.pubkey,
            withdrawal_credentials=v.withdrawal_credentials,
        ))
        # Update validator status if it's a new or changed validator
        session.merge(ValidatorStatus(
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
        # And its balance
        session.merge(ValidatorBalance(
            intro_block_root=block_root,
            validator_index=i,
            intro_slot=slot,
            balance=b,
        ))


def store_validator_diff(session: Session, prev_state: spec.BeaconState, curr_state: spec.BeaconState):
    header = curr_state.latest_block_header.copy()
    header.state_root = curr_state.hash_tree_root()
    block_root = header.hash_tree_root()
    slot = curr_state.slot

    if prev_state.validators.hash_tree_root() != curr_state.validators.hash_tree_root():
        prev: Optional[spec.Validator]
        curr: Optional[spec.Validator]

        for i, (prev, curr) in enumerate(zip_longest(
                prev_state.validators.readonly_iter(),
                curr_state.validators.readonly_iter())):

            assert curr is not None
            if prev is None:
                # Create new validator
                session.merge(Validator(
                    intro_block_root=block_root,
                    validator_index=i,
                    intro_slot=slot,
                    pubkey=curr.pubkey,
                    withdrawal_credentials=curr.withdrawal_credentials,
                ))
            if prev is None or prev != curr:
                # Update validator status if it's a new or changed validator
                session.merge(ValidatorStatus(
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

    if prev_state.balances.hash_tree_root() != curr_state.balances.hash_tree_root():
        prev: Optional[spec.Gwei]
        curr: Optional[spec.Gwei]

        for i, (prev, curr) in enumerate(zip_longest(prev_state.balances.readonly_iter(), curr_state.balances.readonly_iter())):
            if prev is None or prev != curr:
                # The balance may have changed
                session.merge(ValidatorBalance(
                    intro_block_root=block_root,
                    validator_index=i,
                    intro_slot=slot,
                    balance=curr,
                ))


def store_block(session: Session, post_state: spec.BeaconState, signed_block: spec.SignedBeaconBlock):
    block = signed_block.message
    block_root = block.hash_tree_root()
    body = block.body

    # Eth1
    eth1_data = body.eth1_data
    eth1_data_root = eth1_data.hash_tree_root()
    session.merge(Eth1Data(
        data_root=eth1_data_root,
        deposit_root=eth1_data.deposit_root,
        deposit_count=eth1_data.deposit_count,
        block_hash=eth1_data.block_hash,
    ))
    session.merge(Eth1BlockVote(
        beacon_block_root=block_root,
        slot=block.slot,
        eth1_data_root=eth1_data_root,
        proposer_index=block.proposer_index,
    ))

    def handle_header(block: spec.BeaconBlockHeader):
        session.merge(BeaconBlock(
            block_root=block.hash_tree_root(),
            slot=block.slot,
            proposer_index=block.proposer_index,
            parent_root=block.parent_root,
            state_root=block.state_root,
            body_root=block.body.hash_tree_root(),
        ))

    def handle_signed_header(signed_block: spec.SignedBeaconBlockHeader):
        session.merge(SignedBeaconBlock(
            root=signed_block.hash_tree_root(),
            signature=signed_block.signature,
            block_root=signed_block.message.hash_tree_root(),
        ))

    # Proposer slashings
    proposer_slashing: spec.ProposerSlashing
    for i, proposer_slashing in enumerate(body.proposer_slashings.readonly_iter()):
        handle_header(proposer_slashing.signed_header_1.message)
        handle_header(proposer_slashing.signed_header_2.message)
        handle_signed_header(proposer_slashing.signed_header_1)
        handle_signed_header(proposer_slashing.signed_header_2)
        session.merge(ProposerSlashing(
            root=proposer_slashing.hash_tree_root(),
            signed_header_1=proposer_slashing.signed_header_1.hash_tree_root(),
            signed_header_2=proposer_slashing.signed_header_2.hash_tree_root(),
        ))
        session.merge(ProposerSlashingInclusion(
            intro_block_root=block_root,
            intro_index=i,
            root=proposer_slashing.hash_tree_root(),
        ))

    def handle_att_data(data: spec.AttestationData):
        source_ch = data.source
        source_ch_root = source_ch.hash_tree_root()
        session.merge(Checkpoint(
            checkpoint_root=source_ch_root,
            epoch=source_ch.epoch,
            block_root=source_ch.root,
        ))
        target_ch = data.target
        target_ch_root = target_ch.hash_tree_root()
        session.merge(Checkpoint(
            checkpoint_root=target_ch_root,
            epoch=target_ch.epoch,
            block_root=target_ch.root,
        ))
        data_root = data.hash_tree_root()
        session.merge(AttestationData(
            att_data_root=data_root,
            slot=data.slot,
            index=data.index,
            beacon_block_root=data.beacon_block_root,
            source=source_ch_root,
            target=target_ch_root,
        ))

    def handle_indexed_att(indexed: spec.IndexedAttestation):
        session.merge(IndexedAttestation(
            indexed_attestation_root=indexed.hash_tree_root(),
            attesting_indices=', '.join(map(str, indexed.attesting_indices.readonly_iter())),
            data=indexed.data.hash_tree_root(),
            signature=indexed.signature,
        ))

    # Attester slashings
    attester_slashing: spec.AttesterSlashing
    for i, attester_slashing in enumerate(body.attester_slashings.readonly_iter()):
        handle_att_data(attester_slashing.attestation_1.data)
        handle_att_data(attester_slashing.attestation_2.data)
        handle_indexed_att(attester_slashing.attestation_1)
        handle_indexed_att(attester_slashing.attestation_2)
        session.merge(AttesterSlashing(
            root=attester_slashing.hash_tree_root(),
            attestation_1=attester_slashing.attestation_1.hash_tree_root(),
            attestation_2=attester_slashing.attestation_2.hash_tree_root(),
        ))
        session.merge(AttesterSlashingInclusion(
            intro_block_root=block_root,
            intro_index=i,
            root=attester_slashing.hash_tree_root(),
        ))

    # Attestations
    attestation: spec.Attestation
    for i, attestation in enumerate(body.attestations.readonly_iter()):
        data = attestation.data
        handle_att_data(data)
        indexed = spec.get_indexed_attestation(post_state, attestation)
        handle_indexed_att(indexed)
        session.merge(BitsAttestation(
            bits_attestation_root=attestation.hash_tree_root(),
            indexed_attestation_root=indexed.hash_tree_root(),
        ))
        session.merge(PendingAttestation(
            intro_block_root=block_root,
            intro_index=i,
            indexed_att=indexed.hash_tree_root(),
            inclusion_delay=block.slot - data.slot,
            proposer_index=block.proposer_index,
        ))

    # Deposits
    deposit: spec.Deposit
    pre_dep_count = post_state.eth1_deposit_index - len(body.deposits)
    for i, deposit in enumerate(body.deposits.readonly_iter()):
        data = deposit.data
        dep_data_root = data.hash_tree_root()
        session.merge(DepositData(
            data_root=dep_data_root,
            pubkey=data.pubkey,
            withdrawal_credentials=data.withdrawal_credentials,
            amount=data.amount,
            signature=data.signature,
        ))
        session.merge(Deposit(
            root=deposit.hash_tree_root(),
            deposit_index=pre_dep_count + i,
            dep_tree_root=post_state.eth1_data.deposit_root,
            data=dep_data_root,
        ))
        session.merge(DepositInclusion(
            intro_block_root=block_root,
            intro_index=i,
            root=deposit.hash_tree_root(),
        ))

    # Voluntary Exits
    sig_vol_exit: spec.SignedVoluntaryExit
    for i, sig_vol_exit in enumerate(body.voluntary_exits.readonly_iter()):
        session.merge(SignedVoluntaryExit(
            root=sig_vol_exit.hash_tree_root(),
            epoch=sig_vol_exit.message.epoch,
            validator_index=sig_vol_exit.message.validator_index,
            signature=sig_vol_exit.signature,
        ))
        session.merge(SignedVoluntaryExitInclusion(
            intro_block_root=block_root,
            intro_index=i,
            root=sig_vol_exit.hash_tree_root(),
        ))

    # The body
    session.merge(BeaconBlockBody(
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
    session.merge(BeaconBlock(
        block_root=block_root,
        slot=block.slot,
        proposer_index=block.proposer_index,
        parent_root=block.parent_root,
        state_root=block.state_root,
        body_root=body.hash_tree_root(),
    ))

    # Block signature
    session.merge(SignedBeaconBlock(
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
        session.merge(CanonBeaconBlock(
            slot=block.slot,
            block_root=block.hash_tree_root(),
        ))
        proposer_index = block.proposer_index
    else:
        proposer_index = calc_beacon_proposer_index(post, post.slot)

    session.merge(CanonBeaconState(
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
        if block is not None:
            print(f"storing block {block.hash_tree_root().hex()}")
            store_block(session, post_state, signed_block=block)
        print(f"storing post-state {post_state.hash_tree_root().hex()}")
        store_state(session, post_state)
        if prev_state is None:
            print(f"storing full validator set of post-state {post_state.hash_tree_root().hex()}")
            store_validator_all(session, post_state)
        else:
            print(f"storing validator diff between pre {prev_state.hash_tree_root().hex()}"
                  f" and post {post_state.hash_tree_root().hex()}")
            store_validator_diff(session, prev_state, post_state)
        if is_canon:
            print(f"storing canonical ref to post-state {post_state.hash_tree_root().hex()}")
            store_canon_chain(session, post_state, block)
        session.commit()
