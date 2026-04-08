from selectors import EpollSelector, EVENT_READ
from psycopg.errors import OperationalError, AdminShutdown
from threading import Thread, Event
from os import pipe2, O_NONBLOCK, write, close



def _exit_on_disconnect(conn, pipe_r, shutdown_event, callback):
    # After https://www.psycopg.org/psycopg3/docs/advanced/async.html#detecting-disconnections
    with EpollSelector() as sel:
        sel.register(conn.fileno(), EVENT_READ)
        sel.register(pipe_r, EVENT_READ)
        while True:
            sel.select()
            if shutdown_event.is_set():
                break
            try:
                with conn.cursor() as cur:
                    cur.execute("SHOW SERVER_VERSION")
            except (OperationalError, AdminShutdown):
                shutdown_event.set()
                callback()
                break


class OnDBDisconnect:
    def __init__(self, callback, conn):
        self.conn = conn
        self.pipe_r, self.pipe_w = pipe2(O_NONBLOCK)
        self.shutdown_event = Event()
        self.t = Thread(target = _exit_on_disconnect, args=(self.conn, self.pipe_r, self.shutdown_event, callback), daemon=True)
        self.t.start()


    def unmonitor(self):
        if not self.shutdown_event.is_set():
            self.shutdown_event.set()
            write(self.pipe_w, b'bye!')  # breaks out of the select()
        self.t.join()
        close(self.pipe_w)
        close(self.pipe_r)
