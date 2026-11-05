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
the extras (`keep_healthy`), did not cause that hole. A framed exchange in
the last 45 seconds that happened at or after the new socket accept keeps
that socket (`keep_healthy`) and resets the extra, including while an
earlier admission grace is still open. The contract no longer opens a quiet
window that refuses every dial. When the incumbent has not exchanged since
the new socket arrived, both sockets stay until that quiet-redial grace ends.

## Identity

Do not collapse sockets because they share a peer IP. Two collectors behind
one NAT legitimately share an address until each presents a full PN
(`CONNECTION_ARCHITECTURE.md`, session registry).

A socket is in a storm set only when it already carries the same strong
full PN as another live socket.

A PN-less socket is not in that set. Matching a live routed payload's
peer IP is not ownership, and neither is arriving later. A different
collector behind one NAT must still be able to present its own full PN.
Storm decisions that need identity run once a strong full PN is known.
Two sockets that then present the same strong full PN are one storm set.
A live routed AT session is in that same strong-PN storm set.
A socket that presents a different strong full PN is never part of
another collector's storm. Two sockets that only share an IP, and
neither has a strong PN, stay independent.

## Actions

Live means pending, parked, or routed, and not yet terminal.

Keep one survivor in a storm set of size ≥2 and reset extras only. During
the quiet-redial grace below, both sockets stay until that grace ends.
Do not reset the whole set. Do not open a quiet window that refuses all
dials. Do not close or rebind the listener.

**Healthy.** A socket is healthy when it completed a framed exchange in the
last 15 seconds and that exchange is at or after the other socket's accept
time. Keep that socket (`keep_healthy`) and reset the extras. A framed
exchange after the new socket arrived still wins. A completed AT reply is
the exchange time those windows use.

**Recent but not healthy.** A framed exchange in the last 45 seconds is still
kept (`keep_healthy`) when it happened at or after the new socket's accept
time. That holds while an earlier admission grace is open: do not
`replace_stale` the incumbent for a newcomer accepted before that exchange.
An exchange that predates the new socket does not reset that redial
for the stale window. Do not full-set reset because one poll was missed.

**Quiet redial.** When the incumbent has not framed since the new socket was
accepted (no exchange since that accept, or the last exchange is older than
the accept time), do not reset the newcomer at identification. Keep both
through the 15-second grace that starts when the same-PN overlap is first
seen. If the incumbent's last exchange is older than that accept when grace
ends, `replace_stale` admits the newcomer. A further quiet redial that arrives
while that grace is open does not move the deadline; only a redial accepted
after grace has ended starts a new grace.

**No framed exchange yet.** When accept times do not show a later socket,
keep a candidate and give it 15 seconds of grace from when it became the
admitted survivor. Prefer a live routed payload socket, else an identified
socket, else the first socket in the set. When neither socket has an
exchange time, that choice keeps a live routed AT session rather than a
pending socket listed first. Reset extras during grace
(`keep_candidate`). A later lone socket is admitted; it is not
quiet-rejected.

**After grace.** If the admitted survivor still has no usable framed
exchange, reset that stale survivor and keep the extra (`replace_stale`).
The replacement gets a new 15-second grace. When neither socket has an
exchange time, a live routed session stays the `keep_candidate` survivor
instead.

Reset means abort the TCP connection (RST), not a graceful `FIN`. A graceful
close leaves the stick on its own timeout, which is how the overlapping
sessions persisted.

## Placement and logs

Put the decision in `collector/transport/socket_storm.py`. The listener calls
the storm check when a strong PN is recorded, and it runs that same check
again when the quiet-redial grace ends. A framed or AT exchange during an
open cycle runs it again too. Do not grow a second policy inside the accept
loop.

One warning line per phase. Repeated `keep_candidate` lines in the same hold
are suppressed. Each line names the strong full PN, the peer IP, the closed
session ids, and the reason `keep_healthy` | `keep_candidate` |
`replace_stale`. A PN-less accept does not log a storm.

## Tests

`tests/test_callback_socket_storm.py` via `.venv/bin/python -m unittest`.
Cover: healthy socket kept and extras reset; a framed exchange 15.001 seconds
ago still kept under the 45-second window when it is not older than the
other socket's accept; a warm cycle keeps that incumbent when the exchange
is still inside 45 seconds and at or after the newcomer accept; a quiet
incumbent whose last exchange predates the new socket does not reset that
redial at accept+1, and replace_stale admits the newcomer after grace; no
exchange keeps one candidate
and a later lone socket is admitted; a second dial during 15-second grace
resets only the extra; after grace the extra replaces the candidate; two
PN-less sockets that only share an IP are not a storm; a PN-less socket
that arrives after a live routed payload on that IP is not reset; a later
different strong full PN on that IP is not a sibling; a different strong
full PN is left alone; two sockets with the same strong full PN keep one
survivor; abort is RST, not a graceful close; the listen socket is not
closed. A real listener test covers two loopback clients on 127.0.0.1
with different full FC2 PNs, both remaining `routed_framed`.
