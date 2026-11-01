"""A TCP proxy that can cut a network link on command (fault injection).

Placed between two services (API -> SQS, worker -> PostgreSQL), `cut()` closes every open connection and
refuses new ones, which the client sees as a real outage; `restore()` lets traffic through again.
"""
from __future__ import annotations

import socket
import threading


class TcpProxy:
    def __init__(self, listen_port: int, target_host: str, target_port: int, listen_host: str = "127.0.0.1"):
        self.listen = (listen_host, listen_port)
        self.target = (target_host, target_port)
        self._cut = threading.Event()
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._open: set[socket.socket] = set()
        self._server: socket.socket | None = None
        self.connections = 0
        self.refused = 0

    @property
    def port(self) -> int:
        return self.listen[1]

    def start(self) -> "TcpProxy":
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(self.listen)
        srv.listen(128)
        srv.settimeout(0.2)
        self._server = srv
        threading.Thread(target=self._accept_loop, daemon=True).start()
        return self

    def cut(self) -> None:
        self._cut.set()
        with self._lock:
            socks = list(self._open)
            self._open.clear()
        for s in socks:
            self._close(s)

    def restore(self) -> None:
        self._cut.clear()

    def stop(self) -> None:
        self._stop.set()
        self.cut()
        if self._server:
            self._server.close()

    @staticmethod
    def _close(s: socket.socket) -> None:
        try:
            s.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            s.close()
        except OSError:
            pass

    def _accept_loop(self) -> None:
        while not self._stop.is_set():
            try:
                client, _ = self._server.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            if self._cut.is_set():
                self.refused += 1
                self._close(client)
                continue
            try:
                upstream = socket.create_connection(self.target, timeout=2)
            except OSError:
                self._close(client)
                continue
            upstream.settimeout(None)
            client.settimeout(None)
            for s in (client, upstream):
                s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            with self._lock:
                self._open.update((client, upstream))
            self.connections += 1
            threading.Thread(target=self._pump, args=(client, upstream), daemon=True).start()
            threading.Thread(target=self._pump, args=(upstream, client), daemon=True).start()

    def _pump(self, src: socket.socket, dst: socket.socket) -> None:
        try:
            while True:
                data = src.recv(65536)
                if not data or self._cut.is_set():
                    break
                dst.sendall(data)
        except OSError:
            pass
        with self._lock:
            self._open.discard(src)
            self._open.discard(dst)
        self._close(src)
        self._close(dst)
