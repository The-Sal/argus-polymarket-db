# Argus Polymarket DB (APDB)
This repository contains the source code for a new database for the Argus Polymarket Dispatcher. 
Historically the Polymarket dispatcher was the largest (memory wise) taking 4.5GB+ under strain, this was mostly
due to the large number of markets it stores and Python's alloc/dealloc quirks within the Argus codebase. This module
acts as an external database for the dispatcher to consult, replacing the central dictionary within Argus. It also keep extremely
fast performance and extremely low memory usage. This repo uses only 13.1MB of RAM (on macOS) with a dataset of 16,000 Polymarket Events 
inside the database. It also features a builtin proxy system that can be given multiple SOCKS5 proxies to use. This is used
by Argus to passon the `WIREPROXY_BIND_ADDRESS`, and when not in use APDB auto-detects proxies (if a direct path is unavailable) by encoding the default ports
of Argus managed WireGuard instances. The exact mechanism that Argus and APDB use to pass results is not yet decided. The groundwork for the database
and its memory usage is already completed. 