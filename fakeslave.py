import io
import sys
import socket
from collections import namedtuple


AGAIN = namedtuple('Again', 'foo')


def parse_redis_cmd(data):
    buf = io.BytesIO(data)
    cmd = parse_redis_array(buf)
    if cmd == AGAIN:
        return (None, None)
    return cmd, buf.tell()


def parse_redis_array(buf):
    arr_len = parse_redis_len(buf, b'*')
    if arr_len in (None, AGAIN):
        return AGAIN
    arr = []
    for _ in range(arr_len):
        if len(buf.getvalue()) == buf.tell():
            return AGAIN
        prefix = buf.getvalue()[buf.tell():buf.tell() + 1]
        if prefix == b'*':
            res = parse_redis_array(buf)
        elif prefix == b'$':
            res = parse_redis_bulk_str(buf)
        else:
            raise InvalidRedisPacket(buf.getvalue(), buf.tell())
        if res == AGAIN:
            return AGAIN
        arr.append(res)
    return arr


def parse_redis_bulk_str(buf):
    l = parse_redis_len(buf, b'$')
    if l == AGAIN:
        return AGAIN
    s = b''
    while True:
        line = buf.readline()
        if not line:
            return AGAIN
        s += line
        if len(s) > l + 2:
            raise InvalidRedisPacket(buf.getvalue(), buf.tell())
        if len(s) == l + 2:
            s = s[:-2]
            break
    return s


def parse_redis_len(buf, prefix):
    if b'\n' not in buf.getvalue()[buf.tell():]:
        return AGAIN
    line = buf.readline()
    assert line[0:1] == prefix, (line, line[0:1], prefix)
    try:
        return int(line.strip(prefix + b'\r\n'))
    except ValueError:
        raise InvalidRedisPacket(buf.getvalue(), buf.tell())


def encode_command(*cmd_list):
    packet = "*{}\r\n".format(len(cmd_list))
    for e in cmd_list:
        packet += "${}\r\n{}\r\n".format(len(e), e)
    return packet.encode('utf-8')


def fullsync(host, port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((host, port))
    s.send(encode_command("PSYNC", "?", "-1"))
    print('waiting result')
    data = b''
    runid = None
    reploff = None
    rdb = None
    while True:
        d = s.recv(1024)
        # print(d)
        if not d:
            print("recv end")
            break
        data += d
        if reploff is not None:
            reploff += len(d)

        if runid is None:
            runid, reploff, pos = parse_fullresync(data)
            if runid is not None:
                print(data[:pos])
                print('runid, reploff', runid, reploff)
                data = data[pos:]
            continue

        if rdb is None:
            rdb, pos = parse_rdb(data)
            if rdb is not None:
                print(data[:pos])
                print('rdb:', rdb)
                data = data[pos:]

        if rdb is None:
            continue

        while data:
            cmd, pos = parse_redis_cmd(data)
            if cmd is None:
                break
            cmd_handler(cmd, s, reploff)
            data = data[pos:]


def parse_fullresync(data):
    CRLF = b'\r\n'
    if CRLF not in data:
        return None, None, None
    pos = data.index(CRLF)
    head = '+FULLRESYNC '
    head_len = len(head)
    assert data[:head_len].decode('utf-8') == head, data
    res = data[head_len:pos]
    pos += len(CRLF)
    runid, reploff = res.split(b' ')
    return runid.decode('utf-8'), int(reploff.decode('utf-8')), pos


def parse_rdb(data):
    buf = io.BytesIO(data)
    rdb = parse_rdb_bulk_str(buf)
    if rdb == AGAIN:
        return None, None
    return rdb, buf.tell()


def parse_rdb_bulk_str(buf):
    # Not like normal bulk string, without trailing '\r\n'
    l = parse_redis_len(buf, b'$')
    if l == AGAIN:
        return AGAIN
    s = b''
    while True:
        chunk = buf.read(l - len(s))
        if not chunk:
            return AGAIN
        s += chunk
        if len(s) == l:
            break
    return s


def cmd_handler(cmd, s, reploff):
    print(cmd)
    if cmd[0] == b'REPLCONF' and cmd[1] == b'GETACK':
        send_reploff(s, reploff)
    elif cmd[0] in (b'SELECT', b'PING'):
        pass
    else:
        redirect_cmd(cmd)


def send_reploff(s, reploff):
    print('sendding replconf ack')
    s.send(encode_command('REPLCONF', 'ACK', str(reploff)))


def redirect_cmd(cmd):
    print('redirect: ', cmd)


if __name__ == '__main__':
    _, host, port = sys.argv
    fullsync(host, int(port))
