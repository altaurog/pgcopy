import threading


class RaisingThread(threading.Thread):
    exc = None

    def run(self):
        self.exc = None
        try:
            super().run()
        except Exception as e:
            self.exc = e

    def join(self, timeout=None):
        super().join(timeout)
        if self.exc:
            raise self.exc
