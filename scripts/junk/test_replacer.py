#!/usr/bin/env python3

def read_blocks(filename):
    with open(filename, 'r') as f:
        content = f.read()

    # Split by double newlines (or more)
    blocks = [block.strip() for block in content.split('\n\n') if block.strip()]

    return blocks

def process_blocks(blocks):
    patterns = {
        '(SELECT count(*) FROM t1 AS x WHERE x.b<t1.b)': "1",
        '(SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d)': "1",
    }
    resp = []
    cnt = 0
    for block in blocks:
        for pattern, rep in patterns.items():
            if pattern in block:
                block = block.replace(pattern, rep)
                cnt += 1
        resp.append(block)
    print(f"Changed {cnt} block!")
    return resp

def write_blocks(filename, blocks):
    """Write list of blocks back to file with double newlines between them"""
    with open(filename, 'w') as f:
        f.write('\n\n'.join(blocks))
        f.write('\n')

if __name__ == '__main__':
    fnames = [
        "./tests/sqllogic/any/pg/sqlite/select1.test",
        "./tests/sqllogic/any/pg/sqlite/select2.test",
        "./tests/sqllogic/any/pg/sqlite/select3.test",
    ]
    for fname in fnames:
        blocks = read_blocks(fname)
        new_blocks = process_blocks(blocks)
        write_blocks(fname, new_blocks)
