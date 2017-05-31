import io
import sys
import socket
from collections import namedtuple


AGAIN = namedtuple('Again', 'foo')
PSYNC_FULLRESYNC = b'FULLRESYNC'
PSYNC_CONTITNUE = b'CONTINUE'


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


class SyncSession(object):
    def __init__(self, master_host, master_port):
        self.master_host = master_host
        self.master_port = master_port
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.data = b''

    def start_fullsync(self):
        self.s.connect((self.master_host, self.master_port))
        print('request full sync')
        self.s.send(encode_command("PSYNC", "?", "-1"))

    def start_psync(self, runid, reploff):
        self.s.connect((self.master_host, self.master_port))
        print('request psync {} {}'.format(runid, reploff))
        self.s.send(encode_command("PSYNC", runid, str(reploff)))

    def read(self):
        d = self.s.recv(1024)
        if not d:
            return None
        self.data += d
        print('===', d, '===')
        return len(d)

    def write(self, data):
        self.s.send(data)


class FakeSlave(object):
    STATE_START = 0
    STATE_CONTINUE = 2

    def __init__(self, master_host, master_port):
        self.master_host = master_host
        self.master_port = master_port
        self.state = self.STATE_START
        self.runid = None
        self.reploff = None
        self.rdb = None

    def loop(self):
        while True:
            self.sync_cron()

    def sync_cron(self):
        session = SyncSession(self.master_host, self.master_port)
        if self.state == self.STATE_START:
            session.start_fullsync()
        elif self.state == self.STATE_CONTINUE:
            assert None not in (self.runid, self.reploff), (self.runid, self.reploff)
            session.start_psync(self.runid, self.reploff + 1)

        psync_received = False

        while True:
            dlen = session.read()
            # print(d)
            if not dlen:
                print("session closed")
                return

            if not psync_received:
                res = parse_psync_resp(session.data)
                if res == AGAIN:
                    continue
                elif res[0] == PSYNC_CONTITNUE:
                    assert len(res) == 2
                    pos = res[1]
                    session.data = session.data[pos:]
                    print('CONTINUE')
                else:
                    self.runid, self.reploff, pos = res
                    assert None not in (self.runid, self.reploff)
                    print(session.data[:pos])
                    print('runid, reploff', self.runid, self.reploff)
                    session.data = session.data[pos:]
                    self.state = self.STATE_START
                    self.rdb = None
                psync_received = True

            assert self.runid

            if self.rdb is None:
                self.rdb, pos = parse_rdb(session.data)
                if self.rdb is not None:
                    print(session.data[:pos])
                    print('rdb: ', self.rdb)
                    session.data = session.data[pos:]
                else:
                    continue

            self.state = self.STATE_CONTINUE

            while session.data:
                cmd, pos = parse_redis_cmd(session.data)
                if cmd is None:
                    break
                cmd_handler(cmd, session, self)
                self.reploff += pos
                session.data = session.data[pos:]


def parse_psync_resp(data):
    rslt, pos = parse_simple_string(data)
    if rslt is AGAIN:
        return AGAIN
    assert b'+' == rslt[0:1]
    rslt = rslt[1:]  # remove '+'
    if rslt.startswith(PSYNC_CONTITNUE):
        return PSYNC_CONTITNUE, pos
    elif rslt.startswith(PSYNC_FULLRESYNC):
        res = list(parse_fullresync(rslt))
        res.append(pos)
        return res
    raise Exception("invalid data: {}".format(rslt))


def parse_simple_string(data):
    CRLF = b'\r\n'
    if CRLF not in data:
        return AGAIN
    pos = data.index(CRLF)
    return data[:pos], pos + 2


def parse_fullresync(data):
    head_len = len(PSYNC_FULLRESYNC) + 1  # 1 for space
    res = data[head_len:]
    runid, reploff = res.split(b' ')
    return runid.decode('utf-8'), int(reploff.decode('utf-8'))


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


def cmd_handler(cmd, session, fakeslave):
    print(cmd)
    if cmd[0] == b'REPLCONF' and cmd[1] == b'GETACK':
        send_reploff(session, fakeslave)
    elif cmd[0] in (b'SELECT', b'PING'):
        pass
    else:
        redirect_cmd(cmd)


def send_reploff(session, fakeslave):
    print('sendding replconf ack')
    session.write(encode_command('REPLCONF', 'ACK', str(fakeslave.reploff)))


def redirect_cmd(cmd):
    print('redirect: ', cmd)


if __name__ == '__main__':
    _, host, port = sys.argv
    fakeslave = FakeSlave(host, int(port))
    fakeslave.loop()
