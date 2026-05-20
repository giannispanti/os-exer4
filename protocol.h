#pragma once

/* ── FIFOs used for frontend ↔ dispatcher communication ─────────────── */
#define FIFO_FE2DISP   "/tmp/cc_fe2disp"   /* frontend writes, dispatcher reads */
#define FIFO_DISP2FE   "/tmp/cc_disp2fe"   /* dispatcher writes, frontend reads */

/* ── Paths & limits ─────────────────────────────────────────────────── */
#define WORKER_BIN      "./worker"
#define MAX_WORKERS     32
#define MAX_CHUNKS      4096
#define INITIAL_WORKERS 2

/*
 * ── Protocol summary ────────────────────────────────────────────────────
 *
 * All messages are newline-terminated ASCII lines (≤ PIPE_BUF → atomic).
 *
 * Frontend → Dispatcher (via FIFO_FE2DISP):
 *   ADD\n              add a worker
 *   REM\n              remove a worker (kills least-recently-added)
 *   STA\n              request worker status
 *   PRG\n              request progress (also triggered by SIGUSR1)
 *   QUI\n              graceful shutdown
 *
 * Dispatcher → Frontend (via FIFO_DISP2FE):
 *   OK\n               ACK for ADD / REM
 *   STA <n> <p1> ... <pn>\n   n workers, PIDs
 *   PRG <pct> <found> <done> <total>\n
 *   DONE <found>\n     search complete (or final count on QUI)
 *
 * Dispatcher → Worker (per-worker anonymous pipe):
 *   W <offset> <length>\n    assign chunk
 *   X\n                      terminate
 *
 * Worker → Dispatcher (per-worker anonymous pipe):
 *   R <count>\n              chunk result
 */
