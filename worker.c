/*
 * worker.c
 * --------
 * Spawned by dispatcher via fork+execv.
 *
 * argv: ./worker <file> <char> <read_fd> <write_fd>
 *
 * Protocol (see protocol.h):
 *   reads  "W offset length\n"  or  "X\n"
 *   writes "R count\n"
 */

#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>

#define BUFSIZE 8192

/* Read one '\n'-terminated line from fd.  Returns chars read, 0 on EOF, -1 on error. */
static int read_line(int fd, char *buf, int max) {
    int i = 0; char c;
    while (i < max - 1) {
        int r = read(fd, &c, 1);
        if (r <= 0) { buf[i] = '\0'; return r; }
        if (c == '\n') { buf[i] = '\0'; return i; }
        buf[i++] = c;
    }
    buf[i] = '\0'; return i;
}

/* Write exactly len bytes, retry on short writes. */
static void write_all(int fd, const char *buf, int len) {
    int idx = 0, w;
    while (idx < len) {
        w = write(fd, buf + idx, len - idx);
        if (w <= 0) return;
        idx += w;
    }
}

int main(int argc, char *argv[]) {
    if (argc != 5) {
        write(STDERR_FILENO, "worker: usage: worker <file> <char> <rfd> <wfd>\n", 48);
        exit(1);
    }

    char   target = argv[2][0];
    int    rfd    = atoi(argv[3]);   /* read work from dispatcher */
    int    wfd    = atoi(argv[4]);   /* write results to dispatcher */

    int fd = open(argv[1], O_RDONLY);
    if (fd < 0) { perror("worker: open"); exit(1); }

    char line[64], res[32];

    for (;;) {
        int n = read_line(rfd, line, sizeof(line));
        if (n <= 0) break;             /* dispatcher closed pipe → exit */
        if (line[0] == 'X') break;     /* explicit terminate */
        if (line[0] != 'W') continue;  /* unknown command, ignore */

        /* Parse "W <offset> <length>" manually (no sscanf / libc format parsing) */
        long long off = 0, len = 0;
        char *p = line + 2;
        while (*p >= '0' && *p <= '9') off = off * 10 + (*p++ - '0');
        while (*p == ' ') p++;
        while (*p >= '0' && *p <= '9') len = len * 10 + (*p++ - '0');

        /* Count target character in [off, off+len) using pread (no seek needed) */
        int cnt = 0;
        char buf[BUFSIZE];
        long long cur = off, end = off + len;

        while (cur < end) {
            size_t  want = BUFSIZE;
            if (cur + (long long)want > end) want = (size_t)(end - cur);
            ssize_t got  = pread(fd, buf, want, (off_t)cur);
            if (got <= 0) break;
            for (int i = 0; i < (int)got; i++)
                if (buf[i] == target) cnt++;
            cur += got;
        }

        /* Return result */
        int rlen = snprintf(res, sizeof(res), "R %d\n", cnt);
        write_all(wfd, res, rlen);
    }

    close(fd); close(rfd); close(wfd);
    return 0;
}
