CC      = gcc
CFLAGS  = -Wall -Wextra -g
TARGETS = frontend dispatcher worker

.PHONY: all clean

all: $(TARGETS)

frontend:   frontend.c   protocol.h
	$(CC) $(CFLAGS) -o $@ $<

dispatcher: dispatcher.c protocol.h
	$(CC) $(CFLAGS) -o $@ $<

worker:     worker.c     protocol.h
	$(CC) $(CFLAGS) -o $@ $<

clean:
	rm -f $(TARGETS)
	rm -f /tmp/cc_fe2disp /tmp/cc_disp2fe
