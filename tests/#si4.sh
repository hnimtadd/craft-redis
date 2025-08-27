#!/bin/sh
redis-cli SET foo 5
redis-cli INCR foo
redis-cli INCR foo
