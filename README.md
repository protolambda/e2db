# E2DB

Database models and feeds to track the Eth2 Beacon Chain and Eth1 deposit process.

The goal is to enable anyone to index their own DB of enhanced chain information,
 to track their validators, build dashboards, and back other tooling with.

The DB can use a local sqlite file, or attach to a remote mysql/postgres DB.

This package uses:
- SQL-Alchemy for a nice SQL DB ORM
- Trio for safe async python, and memory send/receive channels
- `eth2`, a new experimental API package for Eth2
- `eth2spec`, the canonical spec for datatypes and state processing functions
- Web3.py, for Eth1 monitoring

*Work in progress*

## Planned features

- Full indexing of Eth2 state for testnet analytics
- An explorer utilizing the data, build on Dash and Plotly
- Filtering to only track a subset of the validators or state. Keeps the DB small, and useful for staking operations.
- Switch from lighthouse API to standardized Eth2 API 
- Maybe DB support for some network-level analytics, depending on integration with Rumor and Prrkl networking tools.

## License

MIT, see [`LICENSE`](./LICENSE) file.
