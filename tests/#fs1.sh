#!/bin/sh
edis-cli XADD some_key 1526985054069-0 temperature 36 humidity 95
redis-cli XADD some_key 1526985054079-0 temperature 37 humidity 94
redis-cli XRANGE some_key 1526985054069 +
