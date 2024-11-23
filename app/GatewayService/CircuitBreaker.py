from multiprocessing import Process
import time
import requests
from http import HTTPStatus


def _routine(service, timeout):
    path = f"http://{service}/manage/health"
    goSleep = True
    while goSleep:
        time.sleep(timeout)
        try:
            response = requests.get(path)
            if response.status_code == HTTPStatus.OK:
                goSleep = False
        except:
            pass


class CircuitQueue:
    def __init__(self, lenght: int=10):
        self.queue = [0] * lenght
        self.index = 0
        
    def set(self):
        self.queue[self.index] = 1
        self.index = (self.index + 1) % len(self.queue)

    def reset(self):
        self.queue[self.index] = 0
        self.index = (self.index + 1) % len(self.queue)

    def set_last(self):
        index = (self.index + len(self.queue) - 1) % len(self.queue)
        self.queue[index] = 1

    def reset_last(self):
        index = (self.index + len(self.queue) - 1) % len(self.queue)
        self.queue[index] = 0

    def state(self):
        return sum(self.queue) == len(self.queue)
    
    def clear(self):
        self.queue = [0] * len(self.queue)
        



class CircuitBreakerData:
    def __init__(self, status: str="BLOCKED", retries: int=0, max_retries=10):
        self.status: str = status
        self.retries: CircuitQueue = CircuitQueue(max_retries)
        self.routine = None

    def set(self):
        self.retries.set()

    def reset(self):
        self.retries.reset()

    def state(self):
        return self.retries.state()
    
    def clear(self):
        self.retries.clear()
    

class CircuitBreaker:
    def __init__(self, maxRetries: int, timeout: int=10):
        self.services = dict()
        self.maxRetries = maxRetries
        self.timeout = timeout
    
    def append(self, service: str):
        self.checkRoutine()
        if service not in self.services.keys():
            self.services[service] = CircuitBreakerData(retries=1, max_retries=self.maxRetries)
        elif self.services[service].status == "BLOCKED":
            self.services[service].retries.set()
        if self.services[service].retries.state():
            self.services[service].retries.clear()
            self.services[service].status = "OPEN"
            self.services[service].routine = Process(target=_routine, args=[service, self.timeout])
            self.services[service].routine.start()
    
    def appendOK(self, service: str):
        self.checkRoutine()
        if service not in self.services.keys():
            self.services[service] = CircuitBreakerData(retries=1, max_retries=self.maxRetries)
        elif self.services[service].status == "BLOCKED":
            self.services[service].retries.reset()
    
    def isBlocked(self, service: str):
        self.checkRoutine()
        if service in self.services.keys():
            return self.services[service].status == "BLOCKED"
        return True

    def checkRoutine(self):
        for key in self.services.keys():
            if self.services[key].routine != None and self.services[key].routine.exitcode != None:
                self.services[key].routine = None
                self.services[key].status = "BLOCKED"

    def terminate(self):
        for service in self.services.values():
            if service.routine != None:
                service.routine.terminate()
    