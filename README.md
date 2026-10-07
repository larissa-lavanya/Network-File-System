# Network File System (NFS) in C

A small distributed file system: one **Naming Server (NM)** keeps the directory, many **Storage Servers (SS)** hold the files, and **Clients** talk to the NM to find files, then read/write directly from the SS. TCP sockets + pthreads (POSIX C). Supports sync/async writes, concurrent clients, trie + LRU path lookup, error codes, backups with failover and recovery, audio streaming, and logging.

## Files
| File | What it does |
|---|---|
| `naming_server.c` | NM: SS registration, path lookup, client commands, write locks, heartbeat failure detection, replication, logging |
| `storage_server.c` | SS: stores files; handles NM commands (create/delete/copy) and client I/O (read/write/info/stream), async write flushing |
| `client.c` | Interactive/scripted client with async-notification listener and music streaming |
| `common.h / common.c` | Shared protocol constants, error codes, socket helpers, path utils, thread-safe logging |
| `trie.h / trie.c` | Trie: fast path -> storage-server lookup (no linear search) |
| `lru.h / lru.c` | LRU cache of recent path lookups |
| `Makefile` | Builds the three binaries |
| `run_demo.sh` | Starts 1 NM + 3 SS on localhost with sample files |

## Build and run
```
make                                   # build naming_server, storage_server, client
./naming_server -p 5000                # start NM (prints its IP:port; log in nm.log)
./storage_server -n <nm_ip> -p 5000 -m 6001 -c 7001 -d ./ss1 [-e path]...   # start an SS
./client -n <nm_ip> -p 5000            # start a client shell
./run_demo.sh  /  ./run_demo.sh stop   # local 3-SS demo / stop it
```
SS flags: `-n/-p` NM address, `-m` port NM uses to command this SS, `-c` port clients use, `-d` storage root, `-e` path to export (repeatable, default: whole root), `-i` own IP (auto-detected otherwise). Run SSs/clients on other laptops by passing the NM's real IP. Restarting an SS with the same IP and `-c` port makes it a *recovering* SS.
Client flags: `-e "CMD"` run a command non-interactively (repeatable), `-w N` wait N s for async notifications before exiting.

## Client commands
| Command | Meaning |
|---|---|
| `READ <path>` | Print file contents (DATA packets until STOP) |
| `WRITE <path> [--SYNC] [--file <local> \| --text <txt>]` | Overwrite a file (no flag: content typed until a line `EOF`). Large writes are async unless `--SYNC` |
| `INFO <path>` | Size, permissions, timestamps |
| `STREAM <path>` | Stream an audio file (.mp3/.wav/.flac/...) into `mpv` (override with env `NFS_PLAYER`) |
| `CREATE <dir\|.> <name> [-d]` | Create file (or folder with `-d`) inside `dir` |
| `DELETE <path>` | Delete file/folder |
| `COPY <src> <dst>` | Copy file/folder (between SSs or within one); `dst` = existing folder or a new name |
| `LIST` | All accessible paths with owning SS |
| `LS [dir]` | Children of a folder |
| `HELP` / `EXIT` | Help / quit |

## How the features map
- **Async/sync write:** SS ACKs a large write immediately, flushes in 4 KB chunks, then NM pushes `[NOTIFY] WRITE_DONE/WRITE_FAILED` to the client (also if the SS dies mid-write).
- **Concurrency:** NM is thread-per-connection with an immediate `ACK`; many readers allowed, one writer; reads are refused (`FILE_BUSY`) while a file is being written.
- **Errors:** `[ERROR <code> <NAME>]` e.g. 1 NOT_FOUND, 2 ALREADY_EXISTS, 3 FILE_BUSY, 4 SS_DOWN, 6 INVALID_FORMAT (see `common.h`).
- **Backup/failover:** every SS is mirrored (async) to the next two SSs (one if only two exist) under `.backup/ss<id>/`; NM pings SSs every 2 s, and reads fail over to a backup when the primary is down (writes/creates/deletes are refused).
- **Recovery:** a restarted SS is restored from its backup; no new NM entries are added.
- **Logs:** NM logs every request/ACK/response with IP:port to `nm.log` and stdout; SS logs to `ss_<port>.log`.

## Assumptions
Paths contain no spaces; global path == path under the SS root; a folder's contents live on the SS that owns it (COPY of a folder spanning several SSs copies only the owner's part); if two SSs register the same path, the first wins; backups mirror the SS's whole root; `mpv` must be installed for real audio playback.
