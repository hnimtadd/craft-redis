#!/bin/sh
redis-cli XADD some_key 1526985054069-0 temperature 36 humidity 95
redis-cli XADD some_key 1526985054079-0 temperature 37 humidity 94
redis-cli XREAD streams some_key 1526985054069-0

redis-cli XADD blueberry 0-1 temperature 6
redis-cli XREAD streams blueberry 0-0
