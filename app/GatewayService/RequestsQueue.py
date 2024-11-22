import queue
import threading
import time
import requests
from collections import deque

requestsQueue = None
lock = threading.Lock()
stopThread = False

def _checkQueue(timeout):
    global lock, stopThread
    while not stopThread:
        time.sleep(timeout)
        lock.acquire()
        while len(requestsQueue) != 0:
            request = requestsQueue.popleft()
            try:
                request()
            except requests.ConnectionError:
                requestsQueue.appendleft(request)
                break
        lock.release()


class RequestsQueueManager:
    def __init__(self, maxsize: int | None=None, timeout=10):
        global requestsQueue
        requestsQueue = deque(maxlen=maxsize)
        self.thread = threading.Thread(target=_checkQueue, args=[timeout])
        self.thread.start()
        
    def append(self, request):
        global requestsQueue, lock
        lock.acquire()
        requestsQueue.append(request)
        lock.release()

    def terminate(self):
        global stopThread
        stopThread = True
