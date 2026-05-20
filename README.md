# ECE NTUA, Operating Systems Spring 2026
# Exercise 4 – Parallel Character Counter

## Architecture

```
[User] ──stdin──▶ [frontend] ──FIFO fe2disp──▶ [dispatcher] ──pipe──▶ [worker 1]
                      ▲                               │           ──pipe──▶ [worker 2]
                      └──────FIFO disp2fe────────────┘           ──pipe──▶ [worker N]
```

Three separate binaries communicate exclusively via pipes and FIFOs:

| Binary | Role |
|---|---|
| `frontend` | CLI, spawns dispatcher, multiplexes stdin ↔ FIFO with `select()` |
| `dispatcher` | Work pool, worker lifecycle, result aggregation, `select()` event loop |
| `worker` | Receives `(offset, length)` chunks, counts with `pread`, returns result |

## Build

```bash
cd exer4
make
```

## Run

```bash
./frontend <file> <char> <n_chunks>

# Example: search for 'e' in a large file, split into 128 chunks
./frontend /usr/share/dict/words e 128
```

## Commands

| Command | Action |
|---|---|
| `add` | Spawn one more worker |
| `remove` | Kill one worker (its chunk is re-queued automatically) |
| `status` | List active worker PIDs |
| `progress` | Show % complete + occurrences found so far |
| `quit` | Graceful shutdown, print final count |
| `help` | List commands |

## Design notes

- **Work pool**: file split into `n_chunks` equal slices, stored as `PENDING / INPROGRESS / FINISHED`.
- **Distribution**: FCFS — dispatcher assigns the next `PENDING` chunk to any idle worker.
- **Crash recovery**: `SIGCHLD` self-pipe wakes `select()` → `waitpid(WNOHANG)` reaps zombies → in-progress chunk is re-queued → reassigned to alive workers.
- **Progress**: frontend sends `SIGUSR1` to dispatcher (signal-based, as per spec) and also a `PRG` command as fallback; dispatcher responds via `disp2fe` FIFO.
- **Auto-respawn**: if all workers die unexpectedly while work remains, dispatcher spawns a replacement automatically.
