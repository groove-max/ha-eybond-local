# Callback socket storm

Maintainer contract for one collector that dials Home Assistant more than once
without closing the previous TCP session. Read
[CONNECTION_ARCHITECTURE.md](CONNECTION_ARCHITECTURE.md) first. This policy does
not replace PN identity, and it does not rebind the listen socket.

## What went wrong on the wire

The collector is the TCP client (`callback_on_demand` / inbound callback to
the configured Home Assistant listen port). It can leave two `ESTABLISHED`
sessions on that port at once, distinguished only by its ephemeral port. The
kernel treats those as independent byte streams. The framed poller owns one of
them. Replies and later dials land on the other, so entities go `unavailable`
while the host still shows established sockets.

Closing every accepted socket and listening again does not by itself clear
this. The collector can open two established sessions again within seconds.
Replacing the listen socket does not change that. The collector dials an
address and port; any acceptor there is the same server.

## Why the policy changed

The first contract reset the entire storm set when no socket had a framed
exchange in the last 15 seconds, then quiet-rejected new joins for 5 seconds.
A second dial during a 5-second settle window restarted that full-set reset.
After three full-set resets in 60 seconds it held quiet reject for 60 seconds.

That policy opened longer `unavailable` windows on the power traces that feed
energy dashboards than leaving overlapping TCP sessions in place. A collector
that still opens a second callback within seconds never completes a poll cycle
if settle or `reset_all` kills the admitted socket. The 5-second quiet window
is short. Chained 60-second backoff is the long hole.

Keeping a socket that still had a recent framed exchange, and only resetting
the extras (`keep_healthy`), did not cause that hole. The contract therefore
always keeps exactly one survivor and no longer opens a quiet window that
refuses every dial.

## Identity

Do not collapse sockets because they share a peer IP. Two collectors behind
one NAT legitimately share an address until each presents a full PN
(`CONNECTION_ARCHITECTURE.md`, session registry).

A socket is in a storm set only when:

- it already carries the same strong full PN as another live socket, or
- it has no PN yet, its peer IP matches a live routed payload
  connection that already carries that strong full PN, and it was
  accepted after that payload session (a later dial of the collector
  already in use). A PN-less socket accepted earlier is a different
  collector that has not identified yet.

A PN-less socket is not in that set when the identified socket is only
parked, stale, or not the live routed payload connection. Two sockets that
only share an IP, and neither has a strong PN, stay independent. A socket
that presents a different strong full PN is never part of another
collector's storm.

## Actions

Live means pending, parked, or routed, and not yet terminal.

Always keep exactly one survivor in a storm set of size ≥2. Reset extras
only. Do not reset the whole set. Do not open a quiet window that refuses
all dials. Do not close or rebind the listener.

**Healthy.** A socket is healthy when it completed a framed exchange in the
last 15 seconds. Keep that socket (`keep_healthy`) and reset the extras.

**Recent but not healthy.** A framed exchange in the last 45 seconds is still
kept (`keep_healthy`). Do not full-set reset because one poll was missed.

**No framed exchange yet.** Keep a candidate and give it 15 seconds of grace
from when it became the admitted survivor. Prefer a live routed payload
socket, else an identified socket, else the first socket in the set. Reset
extras during grace (`keep_candidate`). A later lone socket is admitted; it
is not quiet-rejected.

**After grace.** If the admitted survivor still has no usable framed
exchange, reset that stale survivor and keep the extra (`replace_stale`).
The replacement gets a new 15-second grace.

Reset means abort the TCP connection (RST), not a graceful `FIN`. A graceful
close leaves the stick on its own timeout, which is how the overlapping
sessions persisted.

## Placement and logs

Put the decision in `collector/transport/socket_storm.py`. The listener calls
it when a socket is accepted and when a strong PN is recorded. Do not grow a
second policy inside the accept loop.

One warning line per phase. Repeated `keep_candidate` lines in the same hold
are suppressed. Each line names the PN, or `pn-unknown` plus the peer IP when
the focus is a PN-less sibling, the closed session ids, and the reason
`keep_healthy` | `keep_candidate` | `replace_stale`.

## Tests

`tests/test_callback_socket_storm.py` via `.venv/bin/python -m unittest`.
Cover: healthy socket kept and extras reset; a framed exchange 15.001 seconds
ago still kept under the 45-second window; no exchange keeps one candidate
and a later lone socket is admitted; a second dial during 15-second grace
resets only the extra; after grace the extra replaces the candidate; two
PN-less sockets that only share an IP are not a storm; a probing PN-less
sibling of a live routed payload is reset and the routed socket is kept; a
different strong full PN is left alone; abort is RST, not a graceful close;
the listen socket is not closed.
