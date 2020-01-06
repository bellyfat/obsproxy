# OBS Proxy

My roommate likes to live stream himself playing video games. The internet at our house
is decent (10 Mbps upload), but there are frequent, intermittent periods of high
latency/low throughput. OBS will not buffer the output stream; if the instantaneous
throughput is not high enough for the output bitrate, it will drop frames instead.

This program is a simple TCP proxy that buffers the upload stream. It will accept
a connection from OBS, receive data as fast as it can, and forward it to a streaming
service as fast as it can, keeping the data in a buffer in the meantime. Instead of OBS
dropping frames, the viewers' players will be delayed for a short time, and any further
shorter delays will be hidden.