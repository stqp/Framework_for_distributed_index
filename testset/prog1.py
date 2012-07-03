#! /usr/bin/env python3.1

import sys
import re
import math

if len(sys.argv) < 3:
    print("ERROR argv")
    sys.exit(1)

def extract_digit(s, n):
    res = ""
    i = 0
    for c in s:
        if not c.isdigit(): continue
        res = res + c
        i = i + 1
        if i == n: break
    return res

# id.dat
load_file = sys.argv[1] + "/load.dat"
all_items = {}
prog_load = re.compile(r"^INSERT usertable (?P<key>\S+) \[ field\d+=(?P<value>.*) \]$")
file = open(load_file, mode='r')
for line in file:
    m = prog_load.match(line)
    if m is None: continue
    all_items[m.group("key")] = extract_digit(m.group("value"), 32)
file.close()

all_keys = list(all_items.keys())
all_keys.sort()
numkeys = len(all_keys)

id_file = sys.argv[2] + "/id.dat"
out_file = open(id_file, mode='w')
for i in [1, 2, 4, 8, 16, 32]:
    print("#", i, file=out_file)
    a = numkeys // i
    b = numkeys % i
    n = 0
    for j in range(i):
        # print(n)
        print(all_keys[n], file=out_file)
        n = n + a
        if j < b: n = n + 1
    print(file=out_file)
out_file.close()


# put.dat
load_file = sys.argv[1] + "/load.dat"
all_items = []
prog_load = re.compile(r"^INSERT usertable (?P<key>\S+) \[ field\d+=(?P<value>.*) \]$")
file = open(load_file, mode='r')
for line in file:
    m = prog_load.match(line)
    if m is None: continue
    all_items.append((m.group("key"), extract_digit(m.group("value"), 32)))
file.close()

numkeys = len(all_items)
for i in [1, 2, 4, 8, 16, 32]:
    put_dir = sys.argv[2] + "/put/" + str(i) + "/"
    a = numkeys // i
    b = numkeys % i
    n = 0
    for j in range(i):
        put_file = put_dir + "put" + str(j) + ".dat"
        # print(put_file)
        out_file = open(put_file, mode='w')
        m = n + a
        if j < b: m = m + 1
        for item in all_items[n:m]:
            print("put", item[0], item[1], file=out_file)
        out_file.close()
        n = m


# get.dat
t_file = sys.argv[1] + "/t.dat"
all_queries = []
prog_t = re.compile(r"^READ usertable (?P<key>\S+) \[ .*$")
file = open(t_file, mode='r')
for line in file:
    m = prog_t.match(line)
    if m is None: continue
    all_queries.append(m.group("key"))
file.close()

numkeys = len(all_queries)

get_dir = sys.argv[2] + "/get/32/"
a = numkeys // 32
b = numkeys % 32
n = 0
for j in range(32):
    get_file = get_dir + "get" + str(j) + ".dat"
    # print(get_file)
    out_file = open(get_file, mode='w')
    m = n + a
    if j < b: m = m + 1
    for query in all_queries[n:m]:
        print("get", query, file=out_file)
    out_file.close()
    n = m


# range.dat
load_file = sys.argv[1] + "/load.dat"
all_items = {}
prog_load = re.compile(r"^INSERT usertable (?P<key>\S+) \[ field\d+=(?P<value>.*) \]$")
file = open(load_file, mode='r')
for line in file:
    m = prog_load.match(line)
    if m is None: continue
    all_items[m.group("key")] = extract_digit(m.group("value"), 32)
file.close()

all_keys = list(all_items.keys())
all_keys.sort()

all_keys_index = {}
i = 0
for key in all_keys:
    all_keys_index[key] = i
    i = i + 1

scan_file = sys.argv[1] + "/scan.dat"
all_queries = []
prog_scan = re.compile(r"^SCAN usertable (?P<key>\S+) (?P<count>\d+) \[ .*$")
file = open(scan_file, mode='r')
for line in file:
    m = prog_scan.match(line)
    if m is None: continue
    all_queries.append((m.group("key"), int(m.group("count"))))
file.close()

numkeys = len(all_queries)

range_dir = sys.argv[2] + "/range/32/"
a = numkeys // 32
b = numkeys % 32
n = 0
for j in range(32):
    range_file = range_dir + "range" + str(j) + ".dat"
    # print(range_file)
    out_file = open(range_file, mode='w')
    m = n + a
    if j < b: m = m + 1
    for query in all_queries[n:m]:
        if query[0] in all_items:
            k = all_keys_index.get(query[0]) + query[1] - 1
            if k >= len(all_keys): k = len(all_keys) - 1
            print("range", query[0], all_keys[k], file=out_file)
        else:
            print("WARNING")
    out_file.close()
    n = m
