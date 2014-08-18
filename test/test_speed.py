#!/usr/bin/python
import turnip
import time
import sys


def test_write(cnt):
    t = time.time()
    tp = turnip.Turnip()
    for i in xrange(cnt):
        tp.put(sys.argv[1]+str(i), str(i)*100)
    print 'write ', sys.argv[1], time.time() - t, cnt / (time.time()-t)


def test_read(cnt):
    t = time.time()
    tp = turnip.Turnip()
    for i in xrange(cnt):
        tp.get(sys.argv[1]+str(i))
    print 'read ', sys.argv[1], time.time() - t, cnt / (time.time()-t)


def test_read_one_key(cnt):
    t = time.time()
    tp = turnip.Turnip()
    key = '0'
    for i in xrange(cnt):
        tp.get(key)
    print 'one_key ', sys.argv[1], time.time() - t, cnt / (time.time()-t)


def test_long(cnt):
    tp = turnip.Turnip()
    key = '0000000000'
    key = '0123456789'
    tp.put(sys.argv[1]+key, key*cnt)
    status, val = tp.get(sys.argv[1]+key)
    print status, len(val)
    d = {}
    for c in val:
        d[c] = d.get(c, 0) + 1
    print d


def test_write_batch(cnt, batch_size):
    t = time.time()
    tp = turnip.Turnip()
    batch = []
    for i in xrange(cnt):
        batch.append(("batch"+sys.argv[1]+str(i), str(i)*100))
        if len(batch) >= batch_size:
            tp.write_batch(batch)
            batch = []
    tp.write_batch(batch)
    print 'write ', sys.argv[1], time.time() - t, cnt / (time.time()-t)

if __name__ == "__main__":
    test_write(10000)
    test_read(10000)
    test_read_one_key(10000)
    test_long(100000)
    test_write_batch(1000000, 50000)
    # tp = turnip.Turnip()
    # print tp.get("batch"+sys.argv[1]+str(12))
