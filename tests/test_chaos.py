import socket
import threading
import time
import unittest

from fleet.chaos import TcpProxy
from fleet.local import free_port


class EchoServer:
    def __init__(self):
        self.sock = socket.socket()
        self.sock.bind(("127.0.0.1", 0))
        self.sock.listen(16)
        self.port = self.sock.getsockname()[1]
        threading.Thread(target=self._serve, daemon=True).start()

    def _serve(self):
        while True:
            try:
                c, _ = self.sock.accept()
            except OSError:
                return
            threading.Thread(target=self._echo, args=(c,), daemon=True).start()

    @staticmethod
    def _echo(c):
        try:
            while data := c.recv(4096):
                c.sendall(data)
        except OSError:
            pass
        c.close()


def roundtrip(port: int, payload: bytes) -> bytes:
    with socket.create_connection(("127.0.0.1", port), timeout=2) as s:
        s.sendall(payload)
        return s.recv(4096)


class TestTcpProxy(unittest.TestCase):
    def setUp(self):
        self.echo = EchoServer()
        self.proxy = TcpProxy(free_port(), "127.0.0.1", self.echo.port).start()

    def tearDown(self):
        self.proxy.stop()
        self.echo.sock.close()

    def test_forwards(self):
        self.assertEqual(roundtrip(self.proxy.port, b"hello"), b"hello")

    def test_cut_breaks_open_and_new_connections_then_restore(self):
        s = socket.create_connection(("127.0.0.1", self.proxy.port), timeout=2)
        s.sendall(b"a")
        self.assertEqual(s.recv(10), b"a")
        self.proxy.cut()
        time.sleep(0.05)
        try:
            s.sendall(b"b")
            got = s.recv(10)
        except OSError:
            got = b""
        self.assertEqual(got, b"", "open connection must be closed by cut()")
        s.close()
        try:
            got = roundtrip(self.proxy.port, b"c")
        except OSError:
            got = b""
        self.assertEqual(got, b"", "new connection must be refused while cut")
        self.proxy.restore()
        self.assertEqual(roundtrip(self.proxy.port, b"d"), b"d")


if __name__ == "__main__":
    unittest.main()
