# xmip-core-contract-avro

Apache Avro content contract: a sound object container file always, every datum decoded against the schema it carries; of a bound named type when a Location names one. A technology of [xmip-core-contract](https://github.com/IlleNilsson/xmip-core-contract).

A departure names the datum and field it is at, `block 1 datum[2].lines[0]`,
spelled only when one is raised: a sound container costs no path.

## The declaration

What a Location naming this contract is configured with is two `const`s in
its `declaration` module, `src/declaration.rs`, beside the code, which its
`ContractFactory` reads (ADR-0064, amendment 2026-11-26):
`declaration::SETTINGS`, `reference`, the full name of the record every
container carries, and `NAME`, `avro`, the name a Validate step's `format`
names it by. Built with its `declaration` feature alone
(`--no-default-features --features declaration`), the crate is those and
nothing more: its code and every crate the code is built on are behind the
default feature. So the library the surfaces load can carry it without the
code, for the language server and the desktop to suggest and check a
Location's contract settings though no node in their process loaded the
technology, once the gathering crate of open problem 48 links it.
`test/Declaration.Test.ps1` in the estate holds that the declaration builds
nothing the contract capability does not.

## Toolchain

`rust-toolchain.toml` pins the toolchain for the whole estate. Do not change it
here.

## Verification

The included workflow is manual-only and calls the versioned shared workflow at
`IlleNilsson/.github@v1`.
