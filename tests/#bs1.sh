#!/bin/sh
redis-cli XADD some_key 1526985054069-0 temperature 36 humidity 95
redis-cli XREAD block 5000 streams some_key 1526985054069-0 &
sleep 1 && redis-cli XADD some_key 1526985054079-0 temperature 37 humidity 94
