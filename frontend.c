/*
 * frontend.c
 * ----------
 * Entry point for the user.  Spawns the dispatcher, opens FIFOs,
 * then multiplexes stdin (user commands) and disp2fe (dispatcher responses)
 * via select() so neither blocks the other.
 *
 * Usage: ./frontend <file> <char> <n_chunks>
 *
 * Commands:
 *   add      – spawn one more worker
 *   remove   – kill one worker (re-queues its chunk automatically)
 *   status   – list active worker PIDs
 *   progress – show % complete and occurrences found so far
 *   quit     – graceful shutdown
 *   help     – print this list
 */

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <sys/select.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include "protocol.h"

static int    fe_wr = -1;          /* write end: fe2disp  */
static int    fe_rd = -1;          /* read  end: disp2fe  */
static pid_t  dispatcher_pid = -1;
static int    quit_sent = 0;

/* ── Cleanup ─────────────────────────────────────────────────────────── */
static void cleanup(void) {
    unlink(FIFO_FE2DISP);
    unlink(FIFO_DISP2FE);
}

/* ── I/O helper ──────────────────────────────────────────────────────── */
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

static void send_cmd(const char *cmd) {
    /* Each command fits in one write() call → atomic over FIFO */
    char buf[64];
    int  n = snprintf(buf, sizeof(buf), "%s\n", cmd);
    write(fe_wr, buf, n);
}

/* ── Response display ────────────────────────────────────────────────── */
static int handle_response(const char *msg) {
    if (!strncmp(msg, "OK", 2)) {
        printf("  ✓ done\n");

    } else if (!strncmp(msg, "STA", 3)) {
        int n = 0;
        const char *p = msg + 4;
        while (*p >= '0' && *p <= '9') n = n * 10 + (*p++ - '0');
        printf("  workers active: %d", n);
        if (n == 0) { printf("\n"); return 0; }
        printf("  (PIDs:");
        while (*p == ' ') {
            p++;
            int pid = 0;
            while (*p >= '0' && *p <= '9') pid = pid * 10 + (*p++ - '0');
            printf(" %d", pid);
        }
        printf(")\n");

    } else if (!strncmp(msg, "PRG", 3)) {
        int  pct, done, total; long found;
        sscanf(msg + 4, "%d %ld %d %d", &pct, &found, &done, &total);
        /* Progress bar (20 chars wide) */
        int filled = pct / 5;
        printf("  [");
        for (int i = 0; i < 20; i++) putchar(i < filled ? '#' : '-');
        printf("] %3d%%  chunks: %d/%d  found so far: %ld\n",
               pct, done, total, found);

    } else if (!strncmp(msg, "DONE", 4)) {
        long found; sscanf(msg + 5, "%ld", &found);
        printf("\n  ╔══════════════════════════════╗\n");
        printf("  ║  Search complete!            ║\n");
        printf("  ║  Total occurrences: %-8ld  ║\n", found);
        printf("  ╚══════════════════════════════╝\n");
        return quit_sent ? 1 : 0;  /* 1 = break main loop */
    }
    return 0;
}

static void print_help(void) {
    printf("  Commands: add | remove | status | progress | quit | help\n");
}

/* ── Signal handling ─────────────────────────────────────────────────── */
static volatile sig_atomic_t got_sigint = 0;
static void h_sigint(int s) { (void)s; got_sigint = 1; }

/* ── Main ────────────────────────────────────────────────────────────── */
int main(int argc, char *argv[]) {
    if (argc != 4) {
        fprintf(stderr, "Usage: %s <file> <char> <n_chunks>\n", argv[0]);
        exit(1);
    }
    const char *fname  = argv[1];
    const char *ch     = argv[2];
    const char *nchunks = argv[3];

    if (strlen(ch) != 1) {
        fprintf(stderr, "error: <char> must be exactly one character\n");
        exit(1);
    }
    if (atoi(nchunks) < 1) {
        fprintf(stderr, "error: <n_chunks> must be >= 1\n");
        exit(1);
    }

    /* Create FIFOs */
    unlink(FIFO_FE2DISP); unlink(FIFO_DISP2FE);
    if (mkfifo(FIFO_FE2DISP, 0600) < 0 || mkfifo(FIFO_DISP2FE, 0600) < 0) {
        perror("mkfifo"); exit(1);
    }
    atexit(cleanup);

    /* Fork + exec dispatcher */
    dispatcher_pid = fork();
    if (dispatcher_pid < 0) { perror("fork"); exit(1); }
    if (dispatcher_pid == 0) {
        char *args[] = {
            "./dispatcher",
            (char *)fname, (char *)ch, (char *)nchunks,
            FIFO_FE2DISP, FIFO_DISP2FE, NULL
        };
        execv("./dispatcher", args);
        perror("execv dispatcher"); exit(1);
    }

    /*
     * Open FIFOs.  Ordering is intentional to avoid deadlock:
     *   frontend:   opens fe2disp W first, then disp2fe R
     *   dispatcher: opens fe2disp R first, then disp2fe W
     * Each open() unblocks the other side's matching open().
     */
    fe_wr = open(FIFO_FE2DISP, O_WRONLY);   /* blocks until dispatcher reads */
    if (fe_wr < 0) { perror("open fe2disp"); exit(1); }
    fe_rd = open(FIFO_DISP2FE, O_RDONLY);   /* blocks until dispatcher writes */
    if (fe_rd < 0) { perror("open disp2fe"); exit(1); }

    signal(SIGPIPE, SIG_IGN);
    struct sigaction sa = {0};
    sa.sa_handler = h_sigint; sigemptyset(&sa.sa_mask);
    sigaction(SIGINT, &sa, NULL);

    /* Banner */
    printf("\n  ┌─────────────────────────────────────────┐\n");
    printf("  │  Parallel Character Counter             │\n");
    printf("  │  file: %-32s│\n", fname);
    printf("  │  char: '%-1s'  chunks: %-20s│\n", ch, nchunks);
    printf("  └─────────────────────────────────────────┘\n");
    print_help();
    printf("> "); fflush(stdout);

    /* ── Event loop ──────────────────────────────────────────────────── */
    for (;;) {

        /* Ctrl+C → graceful quit */
        if (got_sigint) {
            printf("\n  Interrupted – sending quit...\n");
            send_cmd("QUI"); quit_sent = 1; got_sigint = 0;
        }

        fd_set rfds; FD_ZERO(&rfds);
        FD_SET(STDIN_FILENO, &rfds);
        FD_SET(fe_rd, &rfds);
        int maxfd = fe_rd; /* fe_rd > STDIN_FILENO always */

        struct timeval tv = { .tv_sec = 0, .tv_usec = 100000 };
        int r = select(maxfd + 1, &rfds, NULL, NULL, &tv);
        if (r < 0) continue;

        /* Dispatcher message (may arrive asynchronously, e.g. DONE) */
        if (FD_ISSET(fe_rd, &rfds)) {
            char buf[256];
            int n = read_line(fe_rd, buf, sizeof(buf));
            if (n < 0) break;       /* dispatcher exited */
            if (handle_response(buf)) break;
            printf("> "); fflush(stdout);
        }

        /* User command */
        if (FD_ISSET(STDIN_FILENO, &rfds)) {
            char line[64];
            if (!fgets(line, sizeof(line), stdin)) {
                /* EOF on stdin → quit */
                send_cmd("QUI"); quit_sent = 1; continue;
            }
            line[strcspn(line, "\n")] = '\0';

            if      (!strcmp(line, "add"))      { send_cmd("ADD"); }
            else if (!strcmp(line, "remove"))   { send_cmd("REM"); }
            else if (!strcmp(line, "status"))   { send_cmd("STA"); }
            else if (!strcmp(line, "progress")) {
                /* Signal-based progress (as per spec) + fallback command */
                if (dispatcher_pid > 0) kill(dispatcher_pid, SIGUSR1);
                send_cmd("PRG");
            }
            else if (!strcmp(line, "quit")) {
                send_cmd("QUI"); quit_sent = 1;
                /* Response (DONE) will arrive asynchronously and break loop */
            }
            else if (!strcmp(line, "help"))     { print_help(); printf("> "); fflush(stdout); }
            else if (line[0] != '\0')           { printf("  Unknown command. "); print_help(); printf("> "); fflush(stdout); }
            else { printf("> "); fflush(stdout); }
        }
    }

    waitpid(dispatcher_pid, NULL, 0);
    return 0;
}
