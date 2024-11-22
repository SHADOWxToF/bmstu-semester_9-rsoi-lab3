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


class CircuitBreakerData:
    def __init__(self, status: str="BLOCKED", retries: int=0):
        self.status: str = status
        self.retries: int = retries
        self.routine = None
    

class CircuitBreaker:
    def __init__(self, maxRetries: int, timeout: int=10):
        self.services = dict()
        self.maxRetries = maxRetries
        self.timeout = timeout
    
    def append(self, service: str):
        self.checkRoutine()
        if service in self.services.keys() and self.services[service].status == "BLOCKED":
            self.services[service].retries += 1
        else:
            self.services[service] = CircuitBreakerData(retries=1)
        if self.services[service].retries == self.maxRetries:
            self.services[service].retries = 0
            self.services[service].status = "OPEN"
            self.services[service].routine = Process(target=_routine, args=[service, self.timeout])
            self.services[service].routine.start()
    
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
    