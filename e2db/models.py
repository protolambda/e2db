from sqlalchemy import Column, Integer, ForeignKey, LargeBinary, BigInteger, Boolean, String
from sqlalchemy_mate import ExtendedBase
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()

BlockNumber = BigInteger
Eth1BlockHash = LargeBinary(length=32)

TxHash = LargeBinary(length=32)

BLSPubkey = LargeBinary(length=48)
BLSSignature = LargeBinary(length=96)

Bytes32 = LargeBinary(length=32)
Root = LargeBinary(length=32)

Version = LargeBinary(length=4)

# It says BigInteger, but it's a SQL int64
Gwei = BigInteger
Slot = BigInteger
CommitteeIndex = BigInteger
DepositIndex = BigInteger
Epoch = BigInteger
ValidatorIndex = BigInteger


def format_epoch(epoch: int) -> int:
    # limit to 63 bits, to fit in a signed integer.
    # Used for validator-status epochs, which can have all bits set as special value.
    if epoch == 0xFFFF_FFFF_FFFF_FFFF:
        return 0x7FFF_FFFF_FFFF_FFFF
    if epoch > 0x7FFF_FFFF_FFFF_FFFF:
        raise Exception(f"unexpected high epoch, but not a special value {epoch}")
    return epoch


class CanonEth1Block(Base, ExtendedBase):
    __tablename__ = 'canon_eth1_block'
    block_num = Column(BlockNumber, primary_key=True)
    block_hash = Column(Eth1BlockHash)


class Eth1Block(Base, ExtendedBase):
    __tablename__ = 'eth1_block'
    block_hash = Column(Eth1BlockHash, primary_key=True)
    parent_hash = Column(Eth1BlockHash, nullable=True)

    block_num = Column(BlockNumber)
    timestamp = Column(Integer)

    deposit_count = Column(Integer)


class Eth1Data(Base, ExtendedBase):
    __tablename__ = 'eth1_data'
    data_root = Column(Root, primary_key=True)
    deposit_root = Column(Root)
    deposit_count = Column(Integer)
    block_hash = Column(Eth1BlockHash)


class Eth1BlockVote(Base, ExtendedBase):
    __tablename__ = 'eth1_block_vote'
    beacon_block_root = Column(Root, primary_key=True)
    slot = Column(Slot)
    eth1_data_root = Column(Root)
    proposer_index = Column(ValidatorIndex)


class DepositData(Base, ExtendedBase):
    __tablename__ = 'deposit_data'
    data_root = Column(Root, primary_key=True)
    pubkey = Column(BLSPubkey)
    withdrawal_credentials = Column(Bytes32)
    amount = Column(Gwei)
    signature = Column(BLSSignature)


class DepositTx(Base, ExtendedBase):
    __tablename__ = 'deposit_tx'
    block_hash = Column(Eth1BlockHash)
    block_num = Column(Integer)
    tx_index = Column(Integer)
    tx_hash = Column(TxHash, primary_key=True)
    data_root = Column(Root, ForeignKey('deposit_data.data_root'))


class Validator(Base, ExtendedBase):
    __tablename__ = 'validator'
    # the root of the beacon block when the validator was created in the beacon state
    intro_block_root = Column(Root, primary_key=True)
    validator_index = Column(ValidatorIndex, primary_key=True)
    intro_slot = Column(Slot)
    pubkey = Column(BLSPubkey)
    withdrawal_credentials = Column(Bytes32)


# TODO: track canonical validator stats, to simplify queries ?
#
# class Validator(Base, ExtendedBase):
#     __tablename__ = 'validator'
#     validator_index = Column(ValidatorIndex, primary_key=True)
#     pubkey = Column(BLSPubkey)
#     withdrawal_credentials = Column(Bytes32)
#
#
# class CanonValidatorStats(Base, ExtendedBase):
#     __tablename__ = 'canon_validator'
#     slot = Column(Slot, primary_key=True)
#     validator_index = Column(ValidatorIndex, primary_key=True)
#     status = Column(String)



class ValidatorStatus(Base, ExtendedBase):
    __tablename__ = 'validator_status'
    # Intro of the new status, not necessarily the validator itself being introduced, just the new status
    intro_block_root = Column(Root, primary_key=True)
    intro_slot = Column(Slot)
    validator_index = Column(ValidatorIndex, primary_key=True)
    effective_balance = Column(Gwei)
    slashed = Column(Boolean)
    activation_eligibility_epoch = Column(Epoch)
    activation_epoch = Column(Epoch)
    exit_epoch = Column(Epoch)
    withdrawable_epoch = Column(Epoch)


# Only tracked for the canonical state (and updated on reorgs)
class ValidatorEpochBalance(Base, ExtendedBase):
    __tablename__ = 'validator_epoch_balance'
    # describes the epoch for which the balance will be valid.
    # I.e. first 32 slots = 0, based on genesis balance. Second 32 slots based on first epoch transition, etc.
    epoch = Column(Epoch, primary_key=True)
    validator_index = Column(ValidatorIndex, primary_key=True)
    balance = Column(Gwei)
    eff_balance = Column(Gwei)


# When the validator balance changes at a non-epoch moment
class ValidatorOddBalance(Base, ExtendedBase):
    __tablename__ = 'validator_odd_balance'
    intro_block_root = Column(Root, primary_key=True)
    intro_slot = Column(Slot)
    validator_index = Column(ValidatorIndex, primary_key=True)
    balance = Column(Gwei)


class BeaconBlock(Base, ExtendedBase):
    __tablename__ = 'beacon_block'
    block_root = Column(Root, primary_key=True)
    slot = Column(Slot)
    proposer_index = Column(ValidatorIndex)
    parent_root = Column(Root)
    state_root = Column(Root)
    body_root = Column(Root)  # Not a foreign key, i.e. body may not exist if we just have header data.


class BeaconBlockBody(Base, ExtendedBase):
    __tablename__ = 'beacon_block_body'
    body_root = Column(Root, primary_key=True)
    randao_reveal = Column(BLSSignature)
    eth1_data_root = Column(Root, ForeignKey('eth1_data.data_root'))
    graffiti = Column(Bytes32)
    # Operations
    proposer_slashings_count = Column(Integer)
    attester_slashings_count = Column(Integer)
    attestations_count = Column(Integer)
    deposits_count = Column(Integer)
    voluntary_exits_count = Column(Integer)


class SignedBeaconBlock(Base, ExtendedBase):
    __tablename__ = 'signed_beacon_block'
    # Root of the signed container (not the block root!)
    root = Column(Root, primary_key=True)
    signature = Column(BLSSignature)
    block_root = Column(Root, ForeignKey('beacon_block.block_root'))


class BeaconState(Base, ExtendedBase):
    __tablename__ = 'beacon_state'
    # Post state root, as referenced in the beacon block
    state_root = Column(Root, primary_key=True)
    # like latest-block-header, except that the state-root is nicely up to date before hashing the latest header.
    latest_block_root = Column(Root)
    slot = Column(Slot)
    eth1_data_root = Column(Root, ForeignKey('eth1_data.data_root'))

    fork = Column(Version, ForeignKey('beacon_fork.current_version'))
    eth1_deposit_index = Column(DepositIndex)

    validators_root = Column(Root)
    balances = Column(Root)  # TODO index balance growth

    # sum of state.slashings (i.e. not total since genesis, but of last EPOCHS_PER_SLASHINGS_VECTOR epochs)
    total_slashings = Column(Gwei)

    # Attestations
    prev_epoch_att_count = Column(Integer)
    curr_epoch_att_count = Column(Integer)

    # Finality
    justification_bits = Column(String)  # Bitvector[JUSTIFICATION_BITS_LENGTH], as literal bits, e.g. "1001"
    prev_just_checkpoint = Column(Root, ForeignKey('checkpoint.checkpoint_root'))  # Previous epoch snapshot
    curr_just_checkpoint = Column(Root, ForeignKey('checkpoint.checkpoint_root'))
    finalized_checkpoint = Column(Root, ForeignKey('checkpoint.checkpoint_root'))


class CanonBeaconBlock(Base, ExtendedBase):
    __tablename__ = 'canon_beacon_block'
    slot = Column(Slot, primary_key=True)
    block_root = Column(Root)


class CanonBeaconState(Base, ExtendedBase):
    __tablename__ = 'canon_beacon_state'
    slot = Column(Slot, primary_key=True)
    state_root = Column(Root)
    # Regardless of the slot being empty or not, there will always be an assigned proposer.
    proposer_index = Column(ValidatorIndex)
    empty_slot = Column(Boolean)


class CanonBeaconEpoch(Base, ExtendedBase):
    __tablename__ = 'canon_beacon_epoch'
    # Of the post-state after the epoch transition, i.e. the starting point of below epoch
    state_root = Column(Root, primary_key=True)
    epoch = Column(Epoch)  # Of the post-state, i.e. post state slot 31 -> epoch 0, slot 32 -> epoch 1


class StakingStats(Base, ExtendedBase):
    __tablename__ = 'staking_stats'
    state_root = Column(Root, primary_key=True)  # Of the post-state after the epoch transition
    slot = Column(Slot)  # Of the post state after epoch transition. I.e. 32 for epoch 0

    total_active_stake = Column(Gwei)

    # Of unslashed attesters, as used in the rewards calculation
    prev_unslashed_source_stake = Column(Gwei)
    prev_unslashed_target_stake = Column(Gwei)
    prev_unslashed_head_stake = Column(Gwei)

    # Of unslashed attesters, similar to what is used in the rewards calculation,
    # but a look into the live ongoing stake stats for next epoch of rewards/penalties.
    curr_unslashed_source_stake = Column(Gwei)
    curr_unslashed_target_stake = Column(Gwei)
    curr_unslashed_head_stake = Column(Gwei)

    # Of all attesters, as used in the justification/finality calculation
    prev_epoch_target_stake = Column(Gwei)
    curr_epoch_target_stake = Column(Gwei)

    # Number of active validators
    active_validators = Column(Integer)


class Fork(Base, ExtendedBase):
    __tablename__ = 'beacon_fork'
    current_version = Column(Version, primary_key=True)
    previous_version = Column(Version)
    epoch = Column(Epoch)


class ForkData(Base, ExtendedBase):
    __tablename__ = 'beacon_fork_data'
    current_version = Column(Version, primary_key=True)
    genesis_validators_root = Column(Root, primary_key=True)


class Checkpoint(Base, ExtendedBase):
    __tablename__ = 'checkpoint'
    checkpoint_root = Column(Root, primary_key=True)
    epoch = Column(Epoch)
    block_root = Column(Root)


class AttestationData(Base, ExtendedBase):
    __tablename__ = 'attestation_data'
    att_data_root = Column(Root, primary_key=True)
    slot = Column(Slot)
    index = Column(CommitteeIndex)
    # LMD GHOST vote
    beacon_block_root = Column(Root)
    # FFG vote
    source = Column(Root, ForeignKey('checkpoint.checkpoint_root'))
    target = Column(Root, ForeignKey('checkpoint.checkpoint_root'))


# TODO: improve how we represent participation in attestations

class IndexedAttestation(Base, ExtendedBase):
    __tablename__ = 'indexed_attestation'
    indexed_attestation_root = Column(Root, primary_key=True)
    attesting_indices = Column(String)  # List[ValidatorIndex, MAX_VALIDATORS_PER_COMMITTEE]
    data = Column(Root, ForeignKey('attestation_data.att_data_root'))
    signature = Column(BLSSignature)


class BitsAttestation(Base, ExtendedBase):
    __tablename__ = 'bits_attestation'
    bits_attestation_root = Column(Root, primary_key=True)
    indexed_attestation_root = Column(Root)


# PendingAttestation is essentially a "AttestationInclusion"
class PendingAttestation(Base, ExtendedBase):
    __tablename__ = 'pending_attestation'
    intro_block_root = Column(Root, primary_key=True)
    intro_index = Column(Integer, primary_key=True)
    indexed_att = Column(Root, ForeignKey('indexed_attestation.indexed_attestation_root'))
    inclusion_delay = Column(Slot)
    proposer_index = Column(ValidatorIndex)


class ProposerSlashing(Base, ExtendedBase):
    __tablename__ = 'proposer_slashing'
    root = Column(Root, primary_key=True)
    signed_header_1 = Column(Root, ForeignKey('signed_beacon_block.root'))
    signed_header_2 = Column(Root, ForeignKey('signed_beacon_block.root'))


class ProposerSlashingInclusion(Base, ExtendedBase):
    __tablename__ = 'proposer_slashing_inclusion'
    intro_block_root = Column(Root, primary_key=True)
    intro_index = Column(Integer, primary_key=True)
    root = Column(Root, ForeignKey('proposer_slashing.root'))


class AttesterSlashing(Base, ExtendedBase):
    __tablename__ = 'attester_slashing'
    root = Column(Root, primary_key=True)
    attestation_1 = Column(Root, ForeignKey('indexed_attestation.indexed_attestation_root'))
    attestation_2 = Column(Root, ForeignKey('indexed_attestation.indexed_attestation_root'))


class AttesterSlashingInclusion(Base, ExtendedBase):
    __tablename__ = 'attester_slashing_inclusion'
    intro_block_root = Column(Root, primary_key=True)
    intro_index = Column(Integer, primary_key=True)
    root = Column(Root, ForeignKey('attester_slashing.root'))


class Deposit(Base, ExtendedBase):
    __tablename__ = 'deposit'
    root = Column(Root, primary_key=True)
    deposit_index = Column(DepositIndex)
    dep_tree_root = Column(Root)
    # TODO not storing: proof = Column(String)  # Vector[Bytes32, DEPOSIT_CONTRACT_TREE_DEPTH + 1]
    data = Column(Root, ForeignKey('deposit_data.data_root'))


class DepositInclusion(Base, ExtendedBase):
    __tablename__ = 'deposit_inclusion'
    intro_block_root = Column(Root, primary_key=True)
    intro_index = Column(Integer, primary_key=True)
    root = Column(Root, ForeignKey('deposit.root'))


class SignedVoluntaryExit(Base, ExtendedBase):
    __tablename__ = 'vol_exit'
    root = Column(Root, primary_key=True)
    epoch = Column(Epoch)  # Earliest epoch when voluntary exit can be processed
    validator_index = Column(ValidatorIndex)
    signature = Column(BLSSignature)


class SignedVoluntaryExitInclusion(Base, ExtendedBase):
    __tablename__ = 'vol_exit_inclusion'
    intro_block_root = Column(Root, primary_key=True)
    intro_index = Column(Integer, primary_key=True)
    root = Column(Root, ForeignKey('vol_exit.root'))
