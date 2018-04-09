#!/usr/bin/env python

import sys

hex=sys.stdin.read()
hex=hex.rstrip()

i=0
while (i != len(hex)):
    byte=hex[i:i+2]
    if len(byte)==2:
        sys.stdout.write(chr(int(byte,16)))
        i+=2

