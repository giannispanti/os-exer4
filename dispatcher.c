/*
 * dispatcher.c
 * ------------
 * Spawned by frontend via fork+execv.
 *
 * argv: ./dispatcher <file> <char> <n_chunks> <fifo_fe2disp> <fifo_disp2fe>
 *
 * Responsibilities:
 *   - Split file into n_chunks work items (Chunk pool)
 *   - Spawn and manage a dynamic set of worker processes
 *   - Distribute work round-robin / FCFS to available workers
 *   - Collect results and maintain running total
 *   - Handle worker crashes: re-queue their in-progress chunk
 *   - Respond to frontend commands via FIFOs
 *   - Report progress on SIGUSR1 or PRG command
 */

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <sys/select.h>
#include <sys/wait.h>
#include "protocol.h"

/* ── Types ─────────────────────────────────────────────────────────────── */
typedef enum { PENDING, INPROGRESS, FINISHED } CStatus;

typedef struct {
    off_t   offset, length;
    CStatus status;
    int     worker_idx;   /* valid only when INPROGRESS */
    long    count;
} Chunk;

typedef struct {
    pid_t pid;
    int   to_w;     /* dispatcher→worker: write end of pipe */
    int   from_w;   /* worker→dispatcher: read  end of pipe */
    int   chunk;    /* current chunk index, -1 if idle       */
    int   alive;
} Worker;

/* ── Globals ─────────────────────────────────────────────────────────── */
static Chunk  pool[MAX_CHUNKS];
static int    n_chunks;
static Worker workers[MAX_WORKERS];
static int    n_workers;

static const char *g_file;
static char        g_target;
static int         fe_rd, fe_wr;   /* FIFOs */

static int  sigpipe[2];            /* self-pipe for SIGCHLD */
static long total_found;
static int  chunks_done;
static int  search_complete;

static volatile sig_atomic_t sigusr1_pending;

/* ── Signal handlers ─────────────────────────────────────────────────── */
static void h_chld(int s) {
    (void)s;
    char b = 1;
    write(sigpipe[1], &b, 1);   /* wake up select() */
}
static void h_usr1(int s) { (void)s; sigusr1_pending = 1; }

/* ── I/O helpers ─────────────────────────────────────────────────────── */
static int read_line(int fd, char *buf, int max) {
    int i = 0; char c;
    while (i < max - 1) {
        int r = read(fd, &c, 1);
        if (r <= 0) { buf[i] = '\0'; return r == 0 ? 0 : -1; }
        if (c == '\n') { buf[i] = '\0'; return i; }
        buf[i++] = c;
    }
    buf[i] = '\0'; return i;
}

static void emit(int fd, const char *msg) {
    int n = (int)strlen(msg), idx = 0, w;
    while (idx < n) {
        w = write(fd, msg + idx, n - idx);
        if (w <= 0) return;
        idx += w;
    }
}

/* ── Work pool ───────────────────────────────────────────────────────── */
static int next_pending(void) {
    for (int i = 0; i < n_chunks; i++)
        if (pool[i].status == PENDING) return i;
    return -1;
}

static int all_done(void) {
    for (int i = 0; i < n_chunks; i++)
        if (pool[i].status != FINISHED) return 0;
    return 1;
}

/* ── Worker management ───────────────────────────────────────────────── */

/* Assign the next pending chunk to worker wi. Does nothing if no work left. */
static void assign_chunk(int wi) {
    int ci = next_pending();
    if (ci < 0) { workers[wi].chunk = -1; return; }

    pool[ci].status     = INPROGRESS;
    pool[ci].worker_idx = wi;
    workers[wi].chunk   = ci;

    char msg[64];
    int  n = snprintf(msg, sizeof(msg), "W %lld %lld\n",
                      (long long)pool[ci].offset, (long long)pool[ci].length);
    if (write(workers[wi].to_w, msg, n) != n) {
        /* Write failed (worker died) – re-queue */
        pool[ci].status     = PENDING;
        pool[ci].worker_idx = -1;
        workers[wi].chunk   = -1;
    }
}

/* Remove dead entries and fix pool.worker_idx references. */
static void compact_workers(void) {
    int j = 0;
    for (int i = 0; i < n_workers; i++) {
        if (!workers[i].alive) continue;
        if (workers[i].chunk >= 0)
            pool[workers[i].chunk].worker_idx = j;   /* update reference */
        if (i != j) workers[j] = workers[i];
        j++;
    }
    n_workers = j;
}

/* Handle SIGCHLD: reap zombies, re-queue orphaned chunks. */
static void reap_workers(void) {
    /* Drain self-pipe */
    char b; while (read(sigpipe[0], &b, 1) > 0);

    pid_t pid; int status;
    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        for (int i = 0; i < n_workers; i++) {
            if (workers[i].pid != pid) continue;
            if (workers[i].alive) {
                int ci = workers[i].chunk;
                if (ci >= 0 && pool[ci].status == INPROGRESS) {
                    pool[ci].status     = PENDING;
                    pool[ci].worker_idx = -1;
                }
                close(workers[i].to_w);
                close(workers[i].from_w);
                workers[i].alive = 0;
            }
            break;
        }
    }
    compact_workers();

    /* Give pending chunks to idle workers */
    for (int i = 0; i < n_workers; i++)
        if (workers[i].alive && workers[i].chunk < 0)
            assign_chunk(i);

    /* Auto-respawn if all workers died but work remains */
    if (n_workers == 0 && !search_complete && !all_done()) {
        /* spawn_worker defined below – forward declaration via prototype */
        extern int spawn_worker(void);
        spawn_worker();
    }
}

/* Spawn a new worker process, assign the first pending chunk.
   Returns worker index, or -1 on error. */
int spawn_worker(void) {
    if (n_workers >= MAX_WORKERS) return -1;

    int to_w[2], from_w[2];
    if (pipe(to_w) < 0) return -1;
    if (pipe(from_w) < 0) { close(to_w[0]); close(to_w[1]); return -1; }

    pid_t pid = fork();
    if (pid < 0) {
        close(to_w[0]); close(to_w[1]);
        close(from_w[0]); close(from_w[1]);
        return -1;
    }

    if (pid == 0) {
        /* Child: close every fd except the two we want to pass */
        for (int i = 3; i < 256; i++)
            if (i != to_w[0] && i != from_w[1]) close(i);

        char rfd_s[8], wfd_s[8], tgt[2] = { g_target, '\0' };
        snprintf(rfd_s, sizeof(rfd_s), "%d", to_w[0]);
        snprintf(wfd_s, sizeof(wfd_s), "%d", from_w[1]);
        char *args[] = { WORKER_BIN, (char *)g_file, tgt, rfd_s, wfd_s, NULL };
        execv(WORKER_BIN, args);
        perror("execv worker"); exit(1);
    }

    /* Parent */
    close(to_w[0]); close(from_w[1]);

    int wi = n_workers++;
    workers[wi].pid    = pid;
    workers[wi].to_w   = to_w[1];
    workers[wi].from_w = from_w[0];
    workers[wi].chunk  = -1;
    workers[wi].alive  = 1;

    assign_chunk(wi);
    return wi;
}

/* Send X to worker wi, close pipes, mark dead, re-queue chunk. */
static void kill_worker(int wi) {
    if (!workers[wi].alive) return;

    int ci = workers[wi].chunk;
    if (ci >= 0 && pool[ci].status == INPROGRESS) {
        pool[ci].status     = PENDING;
        pool[ci].worker_idx = -1;
    }
    emit(workers[wi].to_w, "X\n");
    close(workers[wi].to_w);
    close(workers[wi].from_w);
    workers[wi].alive = 0;
    workers[wi].chunk = -1;
}

/* ── Progress ────────────────────────────────────────────────────────── */
static void send_progress(void) {
    int  pct = (n_chunks > 0) ? (chunks_done * 100 / n_chunks) : 100;
    char buf[128];
    int  n = snprintf(buf, sizeof(buf), "PRG %d %ld %d %d\n",
                      pct, total_found, chunks_done, n_chunks);
    write(fe_wr, buf, n);
}

/* ── Main ────────────────────────────────────────────────────────────── */
int main(int argc, char *argv[]) {
    if (argc != 6) { write(STDERR_FILENO,"dispatcher: bad args\n",21); exit(1); }

    g_file   = argv[1];
    g_target = argv[2][0];
    n_chunks = atoi(argv[3]);
    if (n_chunks < 1 || n_chunks > MAX_CHUNKS) n_chunks = 64;

    /* Self-pipe for SIGCHLD */
    if (pipe(sigpipe) < 0) { perror("pipe"); exit(1); }
    fcntl(sigpipe[1], F_SETFL, O_NONBLOCK);

    /* Signals */
    struct sigaction sa; memset(&sa, 0, sizeof(sa));
    sa.sa_flags = SA_RESTART; sigemptyset(&sa.sa_mask);
    sa.sa_handler = h_chld;  sigaction(SIGCHLD, &sa, NULL);
    sa.sa_handler = h_usr1;  sigaction(SIGUSR1, &sa, NULL);
    signal(SIGPIPE, SIG_IGN);

    /* Build chunk pool */
    int fdin = open(g_file, O_RDONLY);
    if (fdin < 0) { perror("dispatcher: open"); exit(1); }
    off_t fsz = lseek(fdin, 0, SEEK_END); close(fdin);
    if (fsz <= 0) { write(STDERR_FILENO, "empty file\n", 11); exit(1); }

    off_t csz = (fsz + n_chunks - 1) / n_chunks;
    for (int i = 0; i < n_chunks; i++) {
        pool[i].offset     = (off_t)i * csz;
        pool[i].length     = csz;
        if (pool[i].offset + pool[i].length > fsz)
            pool[i].length = fsz - pool[i].offset;
        pool[i].status     = PENDING;
        pool[i].worker_idx = -1;
    }

    /* Open FIFOs.
     * Ordering matches frontend: dispatcher reads fe2disp first,
     * then writes disp2fe – symmetric with frontend's write-then-read. */
    fe_rd = open(argv[4], O_RDONLY);
    if (fe_rd < 0) { perror("open fe2disp"); exit(1); }
    fe_wr = open(argv[5], O_WRONLY);
    if (fe_wr < 0) { perror("open disp2fe"); exit(1); }

    /* Spawn initial workers */
    for (int i = 0; i < INITIAL_WORKERS; i++) spawn_worker();

    /* ── Event loop ─────────────────────────────────────────────────── */
    for (;;) {

        /* Handle pending SIGUSR1 (progress request via signal) */
        if (sigusr1_pending) { sigusr1_pending = 0; send_progress(); }

        /* Build fd_set */
        fd_set rfds; FD_ZERO(&rfds);
        FD_SET(fe_rd,      &rfds);
        FD_SET(sigpipe[0], &rfds);
        int maxfd = fe_rd > sigpipe[0] ? fe_rd : sigpipe[0];

        for (int i = 0; i < n_workers; i++) {
            if (!workers[i].alive || workers[i].chunk < 0) continue;
            FD_SET(workers[i].from_w, &rfds);
            if (workers[i].from_w > maxfd) maxfd = workers[i].from_w;
        }

        struct timeval tv = { .tv_sec = 0, .tv_usec = 100000 };
        if (select(maxfd + 1, &rfds, NULL, NULL, &tv) < 0) continue; /* EINTR */

        /* ── SIGCHLD ────────────────────────────────────────────────── */
        if (FD_ISSET(sigpipe[0], &rfds)) reap_workers();

        /* ── Frontend command ───────────────────────────────────────── */
        if (FD_ISSET(fe_rd, &rfds)) {
            char cmd[64];
            if (read_line(fe_rd, cmd, sizeof(cmd)) <= 0) break; /* frontend gone */

            if (!strcmp(cmd, "ADD")) {
                spawn_worker();
                emit(fe_wr, "OK\n");

            } else if (!strcmp(cmd, "REM")) {
                if (n_workers > 0) {
                    kill_worker(n_workers - 1);
                    compact_workers();
                    for (int i = 0; i < n_workers; i++)
                        if (workers[i].alive && workers[i].chunk < 0)
                            assign_chunk(i);
                }
                emit(fe_wr, "OK\n");

            } else if (!strcmp(cmd, "STA")) {
                char buf[1024]; int pos = 0;
                pos += snprintf(buf + pos, (int)sizeof(buf) - pos, "STA %d", n_workers);
                for (int i = 0; i < n_workers; i++)
                    pos += snprintf(buf + pos, (int)sizeof(buf) - pos, " %d", (int)workers[i].pid);
                buf[pos++] = '\n'; buf[pos] = '\0';
                emit(fe_wr, buf);

            } else if (!strcmp(cmd, "PRG")) {
                send_progress();

            } else if (!strcmp(cmd, "QUI")) {
                /* Graceful shutdown: tell all workers to exit */
                for (int i = 0; i < n_workers; i++) {
                    if (!workers[i].alive) continue;
                    emit(workers[i].to_w, "X\n");
                    close(workers[i].to_w);
                    close(workers[i].from_w);
                }
                while (waitpid(-1, NULL, WNOHANG) > 0);
                /* Final count */
                char fin[64];
                snprintf(fin, sizeof(fin), "DONE %ld\n", total_found);
                emit(fe_wr, fin);
                break;
            }
        }

        /* ── Worker results ─────────────────────────────────────────── */
        for (int i = 0; i < n_workers; i++) {
            if (!workers[i].alive || workers[i].chunk < 0) continue;
            if (!FD_ISSET(workers[i].from_w, &rfds)) continue;

            char res[32];
            if (read_line(workers[i].from_w, res, sizeof(res)) <= 0) continue;
            if (res[0] != 'R') continue;

            long cnt  = atol(res + 2);
            int  ci   = workers[i].chunk;

            pool[ci].status = FINISHED;
            pool[ci].count  = cnt;
            total_found    += cnt;
            chunks_done++;
            workers[i].chunk = -1;

            /* Assign next chunk immediately */
            assign_chunk(i);

            /* Notify frontend when all work is done */
            if (!search_complete && all_done()) {
                search_complete = 1;
                char fin[64];
                snprintf(fin, sizeof(fin), "DONE %ld\n", total_found);
                emit(fe_wr, fin);
            }
        }
    }

    return 0;
}
