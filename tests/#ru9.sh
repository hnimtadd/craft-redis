#!/bin/sh
redis-cli XADD strawberry 0-1 temperature 0
redis-cli XADD banana 0-2 humidity 1
redis-cli XREAD streams strawberry banana 0-0 0-1
