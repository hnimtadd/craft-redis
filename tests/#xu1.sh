#!/bin/sh
redis-cli XADD banana 0-1 temperature 68
redis-cli XREAD block 0 streams banana $ &
sleep 1 && redis-cli XADD banana 0-2 temperature 34
