#!/usr/bin/python
import turnip
import time
import sys
import mmh3


def write_urls(batch_size):
    t = time.time()
    tp = turnip.Turnip()
    batch = []
    cnt = 0
    for url in sys.stdin:
        cnt = cnt + 1
        batch.append((mmh3.hash_bytes(url), url))
        if len(batch) >= batch_size:
            tp.write_batch(batch)
            batch = []
    tp.write_batch(batch)
    print 'write ', time.time() - t, cnt / (time.time()-t)

if __name__ == "__main__":
    write_urls(10000)
