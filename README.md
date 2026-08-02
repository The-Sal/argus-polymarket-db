# Argus Polymarket DB (APDB)

This repository contains the source for a new database backing the Argus Polymarket Dispatcher.

Historically, the Polymarket dispatcher was Argus's largest component by memory footprint, using 4.5GB+ under load. This was largely due to the sheer number of markets it tracked, compounded by Python's allocation/deallocation behavior elsewhere in the Argus codebase. APDB acts as an external database that the dispatcher queries directly, replacing the in-memory central dictionary Argus previously relied on — while delivering much faster performance and a dramatically smaller memory footprint. On macOS, the repo uses only 13.1MB of RAM with a dataset of 16,000 Polymarket Events loaded.

APDB also includes a built-in proxy system that accepts multiple SOCKS5 proxies. Argus uses this to pass along its `WIREPROXY_BIND_ADDRESS`; when no proxy is explicitly provided and a direct path isn't available, APDB auto-detects one by encoding the default ports of Argus-managed WireGuard instances. The exact mechanism Argus and APDB will use to exchange results hasn't been finalized yet.

The core database and its memory optimizations are already complete. Since this is an experimental library with its own networking stack and few constraints, we plan to build out a number of additional features over time.

**MeshData** is one such feature. In production, many APDB-enabled Argus instances run simultaneously — across prod, dev machines, etc. — some reachable directly, others only through a proxy. MeshData lets this entire network of instances share cached data with one another, cutting down on redundant lookups across the mesh. Discovery assumes a Tailscale mesh is available.

Beyond simple cache sharing, the mesh will eventually be able to identify the fastest node to handle a request when multiple entries expire at once — though that capability is still further out.
