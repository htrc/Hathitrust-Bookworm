#!/usr/bin/env python3
import os
import sys
import fileinput


def __id_encode(s: str) -> str:
    return s.replace('/', '=').replace(':', '+').replace('.', ',')


def __id_decode(s: str) -> str:
    return s.replace('=', '/').replace('+', ':').replace(',', '.')


def volid_to_stubby(volid: str, ef_ext: str = '.json.bz2') -> str:
    assert '.' in volid
    lib_id, unclean_volid = volid.split('.', 1)
    clean_volid = __id_encode(unclean_volid)
    stubby_path = os.path.join(lib_id, clean_volid[::3])
    return os.path.join(stubby_path, "{}.{}{}".format(lib_id, clean_volid, ef_ext))


def stubby_to_volid(path: str, ef_ext: str = '.json.bz2') -> str:
    assert path.endswith(ef_ext)
    _, f = os.path.split(path)
    lib_id, clean_volid = f[:-len(ef_ext)].split('.', 1)
    unclean_volid = __id_decode(clean_volid)
    return "{}.{}".format(lib_id, unclean_volid)


if __name__ == '__main__':
    for line in fileinput.input(sys.argv[1:]):
        line = line.rstrip('\n')
        if not line:
            continue
        if line.endswith('.json.bz2'):
            print(stubby_to_volid(line))
        else:
            print(volid_to_stubby(line))

# v = 'loc.ark:/13960/t70v90g5f'
# p = volid_to_stubby(v)
# print("{} -> {}".format(v, p))
# print("{} -> {}".format(p, stubby_to_volid(p))) 

