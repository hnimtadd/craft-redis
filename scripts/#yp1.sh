#!/bin/sh

redis-cli XADD pineapple 0-1 raspberry pineapple
redis-cli XADD pineapple 0-2 strawberry blueberry
redis-cli XADD pineapple 0-3 blueberry banana
redis-cli XADD pineapple 0-4 raspberry mango
redis-cli XRANGE pineapple - 0-2
