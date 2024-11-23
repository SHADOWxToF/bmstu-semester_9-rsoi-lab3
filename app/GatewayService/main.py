from fastapi import *
from fastapi.responses import *
from fastapi.exceptions import *
import requests.adapters as reqad
from sqlmodel import *
from database import *
from typing import Annotated
from fastapi.encoders import jsonable_encoder
from contextlib import asynccontextmanager
import uvicorn
from multiprocessing import Process
import os
import requests
from http import HTTPStatus
from CircuitBreaker import CircuitBreaker
from RequestsQueue import RequestsQueueManager

requestManager = RequestsQueueManager()
circuitBreaker = CircuitBreaker(2, 1)


reqSession = requests.Session()
reqSession.mount("http://", reqad.HTTPAdapter(max_retries=1))

# bonusesHost = "localhost:8050"
# flightsHost = "localhost:8060"
# ticketsHost = "localhost:8070"
bonusesHost = "bonuses:8050"
flightsHost = "flights:8060"
ticketsHost = "tickets:8070"
bonusesAPI = f"{bonusesHost}/api/v1"
flightsAPI = f"{flightsHost}/api/v1"
ticketsAPI = f"{ticketsHost}/api/v1"

@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        reqSession.post(f"http://{flightsHost}/manage/init")
    except:
        requestManager.append(lambda: reqSession.post(f"http://{flightsHost}/manage/init"))
        print(f"http://{flightsHost} is not available")
    try:
        reqSession.post(f"http://{bonusesHost}/manage/init")
    except:
        requestManager.append(lambda: reqSession.post(f"http://{bonusesHost}/manage/init"))
        print(f"http://{bonusesHost} is not available")
    yield
    circuitBreaker.terminate()
    requestManager.terminate()

app = FastAPI(lifespan=lifespan)


@app.get('/manage/health', status_code=200)
def get_persons():
    return


@app.get('/api/v1/flights', status_code=200)
def get_persons(page: int, size: int) -> PaginationResponse:
    if circuitBreaker.isBlocked(flightsHost):
        try:
            response = reqSession.get(f"http://{flightsAPI}/flights", params={"page": page, "size": size})
            circuitBreaker.appendOK(flightsHost)
        except requests.ConnectionError as ex:
            print(ex)
            circuitBreaker.append(flightsHost)
            return JSONResponse(content={"message": "Flight Service unavailable"}, status_code=503)
    else:
        return JSONResponse(content={"message": "Flight Service unavailable"}, status_code=503)
    
    if response.status_code == HTTPStatus.OK:
        return PaginationResponse(**response.json())
    else:
        return PaginationResponse(page=page, pageSize=0, totalElements=0, items=[])


@app.get('/api/v1/tickets', status_code=200)
def get_persons(x_user_name: str = Header()) -> list[TicketResponse]:
    if circuitBreaker.isBlocked(ticketsHost):
        try:
            response = reqSession.get(f"http://{ticketsAPI}/tickets/", params={"user_name": x_user_name})
            circuitBreaker.appendOK(ticketsHost)
        except requests.ConnectionError:
            circuitBreaker.append(ticketsHost)
            return JSONResponse(content={"message": "Ticket Service unavailable"}, status_code=503)
    else:
        return JSONResponse(content={"message": "Ticket Service unavailable"}, status_code=503)
    out = []
    if response.status_code == HTTPStatus.OK:
        tickets: list[TicketJSON] = [TicketJSON(**i) for i in response.json()]
        for ticket in tickets:
            if circuitBreaker.isBlocked(flightsHost):
                try:
                    response = reqSession.get(f"http://{flightsAPI}/flights/{ticket.flightNumber}")
                    circuitBreaker.appendOK(flightsHost)
                except requests.ConnectionError:
                    circuitBreaker.append(flightsHost)
                    out.append(TicketResponse(ticketUid=ticket.ticketUid, status=ticket.status, flightNumber=ticket.flightNumber, fromAirport="", toAirport="", data=""))
            else:
                out.append(TicketResponse(ticketUid=ticket.ticketUid, status=ticket.status, flightNumber=ticket.flightNumber, fromAirport="", toAirport="", data=""))
            if response.status_code == HTTPStatus.OK:
                flight_json = response.json()
                out.append(TicketResponse(ticketUid=ticket.ticketUid, status=ticket.status, **flight_json))
    return out


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request : Request, exc):
    return JSONResponse({"message": "what", "errors": exc.errors()[0]}, status_code=400)

@app.post('/api/v1/tickets', status_code=200)
def get_persons(ticketPurchaseRequest: TicketPurchaseRequest, x_user_name: str = Header()) -> TicketPurchaseResponse:
    if circuitBreaker.isBlocked(flightsHost):
        try:
            response = reqSession.get(f"http://{flightsAPI}/flights/{ticketPurchaseRequest.flightNumber}")
            circuitBreaker.appendOK(flightsHost)
        except requests.ConnectionError:
            circuitBreaker.append(flightsHost)
            return JSONResponse(content={"message": "Flight Service unavailable"}, status_code=503)
    else:
        return JSONResponse(content={"message": "Flight Service unavailable"}, status_code=503)
        
    if response.status_code != HTTPStatus.OK:
        return JSONResponse(content={"message": "Flight not found"}, status_code=404)
    
    flight = FlightData(**response.json())
    if circuitBreaker.isBlocked(ticketsHost):
        try:
            response = reqSession.post(f"http://{ticketsAPI}/tickets/", json=jsonable_encoder(TicketDataJSON(username=x_user_name, flightNumber=ticketPurchaseRequest.flightNumber, price=ticketPurchaseRequest.price)))
            circuitBreaker.appendOK(ticketsHost)
        except requests.ConnectionError:
            circuitBreaker.append(ticketsHost)
            return JSONResponse(content={"message": "Ticket Service unavailable"}, status_code=503)
    else:
        return JSONResponse(content={"message": "Ticket Service unavailable"}, status_code=503)

    if response.status_code != HTTPStatus.CREATED:
        return JSONResponse(content={"message": "Ticket Service error"}, status_code=502)
    
    ticket: TicketJSON = TicketJSON(**response.json())
    payment = None
    if circuitBreaker.isBlocked(bonusesHost):
        try:
            response = reqSession.post(f"http://{bonusesAPI}/bonuses/calculate_price", json=jsonable_encoder(CalculatePriceJSON(name=x_user_name, price=ticketPurchaseRequest.price, paidFromBalance=ticketPurchaseRequest.paidFromBalance, ticketUid=ticket.ticketUid)))
            if response.status_code != HTTPStatus.ACCEPTED:
                return JSONResponse(content={"message": "Bonus Service unavailable"}, status_code=503)
            payment: PaymentDataJSON = PaymentDataJSON(**response.json())
            circuitBreaker.appendOK(bonusesHost)
        except requests.ConnectionError:
            circuitBreaker.append(bonusesHost)
            if circuitBreaker.isBlocked(ticketsHost):
                try:
                    response = reqSession.delete(f"http://{ticketsAPI}/tickets/{ticket.ticketUid}")
                    circuitBreaker.appendOK(ticketsHost)
                except requests.ConnectionError:
                    circuitBreaker.append(ticketsHost)
                    requestManager.append(lambda: requests.delete(f"http://{ticketsAPI}/tickets/{ticket.ticketUid}"))
            else:
                requestManager.append(lambda: requests.delete(f"http://{ticketsAPI}/tickets/{ticket.ticketUid}"))
            return JSONResponse(content={"message": "Bonus Service unavailable"}, status_code=503)
    elif circuitBreaker.isBlocked(ticketsHost):
        try:
            response = reqSession.delete(f"http://{ticketsAPI}/tickets/{ticket.ticketUid}")
            circuitBreaker.appendOK(ticketsHost)
        except requests.ConnectionError:
            circuitBreaker.append(ticketsHost)
            requestManager.append(lambda: requests.delete(f"http://{ticketsAPI}/tickets/{ticket.ticketUid}"))
        return JSONResponse(content={"message": "Bonus Service unavailable"}, status_code=503)
    else:
        requestManager.append(lambda: requests.delete(f"http://{ticketsAPI}/tickets/{ticket.ticketUid}"))
        return JSONResponse(content={"message": "Bonus Service unavailable"}, status_code=503)

    if circuitBreaker.isBlocked(bonusesHost):
        try:
            response = reqSession.get(f"http://{bonusesAPI}/bonuses/{x_user_name}")
            circuitBreaker.appendOK(bonusesHost)
            if response.status_code != HTTPStatus.OK:
                privilege = PrivilegeDataJSON(balance=0, status="")
            privilege = PrivilegeDataJSON(**response.json())
        except requests.ConnectionError:
            circuitBreaker.append(bonusesHost)
            privilege = PrivilegeDataJSON(balance=0, status="")
    else:
        privilege = PrivilegeDataJSON(balance=0, status="")
    
    return TicketPurchaseResponse(ticketUid=ticket.ticketUid,
                                  flightNumber=flight.flightNumber,
                                  fromAirport=flight.fromAirport,
                                  toAirport=flight.toAirport,
                                  date=flight.date,
                                  price=flight.price,
                                  paidByMoney=payment.paidByMoney,
                                  paidByBonuses=payment.paidByBonuses,
                                  status=ticket.status,
                                  privilege=privilege)
    
    
@app.get('/api/v1/tickets/{ticketUid}', status_code=200)
def get_persons(ticketUid: str, x_user_name: str = Header()) -> TicketResponse:
    if circuitBreaker.isBlocked(ticketsHost):
        try:
            response = reqSession.get(f"http://{ticketsAPI}/tickets/", params={"user_name": x_user_name})
            circuitBreaker.appendOK(ticketsHost)
        except requests.ConnectionError:
            circuitBreaker.append(ticketsHost)
            return JSONResponse(content={"message": "Ticket Service unavailable"}, status_code=503)
    else:
        return JSONResponse(content={"message": "Ticket Service unavailable"}, status_code=503)
    
    if response.status_code != HTTPStatus.OK:
        return JSONResponse(content={"message": "User not found"}, status_code=404)

    tickets: list[TicketJSON] = [TicketJSON(**i) for i in response.json()]
    for ticket in tickets:
        if ticket.ticketUid == ticketUid:
            if circuitBreaker.isBlocked(flightsHost):
                try:
                    response = reqSession.get(f"http://{flightsAPI}/flights/{ticket.flightNumber}")
                    circuitBreaker.appendOK(flightsHost)
                except requests.ConnectionError:
                    circuitBreaker.append(flightsHost)
                    return TicketResponse(ticketUid=ticket.ticketUid, status=ticket.status, flightNumber=ticket.flightNumber, fromAirport="", toAirport="", data="")
            else:
                return TicketResponse(ticketUid=ticket.ticketUid, status=ticket.status, flightNumber=ticket.flightNumber, fromAirport="", toAirport="", data="")

            if response.status_code == HTTPStatus.OK:
                flight_json = response.json()
                return TicketResponse(ticketUid=ticket.ticketUid, status=ticket.status, **flight_json)
            else:
                return JSONResponse(content={"message": "Ticket Service error"}, status_code=502)
    return JSONResponse(content={"message": "Ticket not found"}, status_code=404)


@app.delete('/api/v1/tickets/{ticketUid}', status_code=204)
def get_persons(ticketUid: str, x_user_name: str = Header()):
    if circuitBreaker.isBlocked(ticketsHost):
        try:
            response = reqSession.put(f"http://{ticketsAPI}/tickets/{ticketUid}")
            circuitBreaker.appendOK(ticketsHost)
        except requests.ConnectionError:
            circuitBreaker.append(ticketsHost)
            return JSONResponse(content={"message": "Ticket Service unavailable"}, status_code=503)
    else:
        return JSONResponse(content={"message": "Ticket Service unavailable"}, status_code=503)
    
    if response.status_code == HTTPStatus.NOT_FOUND:
        return JSONResponse(content={"message": "Ticket not found"}, status_code=404)
    if response.status_code == HTTPStatus.OK:
        return

    if circuitBreaker.isBlocked(bonusesHost):
        try:
            response = reqSession.post(f"http://{bonusesAPI}/bonuses/cancel", json=jsonable_encoder(CancelTicketJSON(name=x_user_name, ticketUid=ticketUid)))
            circuitBreaker.appendOK(bonusesHost)
            if response.status_code != HTTPStatus.ACCEPTED:
                return JSONResponse(content={"message": "User or ticket not found"}, status_code=404)
        except:
            circuitBreaker.append(bonusesHost)
            requestManager.append(lambda: requests.post(f"http://{bonusesAPI}/bonuses/cancel", json=jsonable_encoder(CancelTicketJSON(name=x_user_name, ticketUid=ticketUid))))
    else:
        requestManager.append(lambda: requests.post(f"http://{bonusesAPI}/bonuses/cancel", json=jsonable_encoder(CancelTicketJSON(name=x_user_name, ticketUid=ticketUid))))
    return


@app.get('/api/v1/me', status_code=200)
def get_persons(x_user_name: str = Header()) -> UserInfoResponse:
    if circuitBreaker.isBlocked(ticketsHost):
        try:
            response = reqSession.get(f"http://{ticketsAPI}/tickets/", params={"user_name": x_user_name})
            circuitBreaker.appendOK(ticketsHost)
        except requests.ConnectionError:
            circuitBreaker.append(ticketsHost)
            return JSONResponse(content={"message": "Ticket Service unavailable"}, status_code=503)
    else:
        return JSONResponse(content={"message": "Ticket Service unavailable"}, status_code=503)
    
    tickets = []
    if response.status_code == HTTPStatus.OK:
        rawTickets: list[TicketJSON] = [TicketJSON(**i) for i in response.json()]
        for ticket in rawTickets:

            if circuitBreaker.isBlocked(flightsHost):
                try: 
                    response = reqSession.get(f"http://{flightsAPI}/flights/{ticket.flightNumber}")
                    circuitBreaker.appendOK(flightsHost)
                except requests.ConnectionError:
                    circuitBreaker.append(flightsHost)
                    tickets.append(TicketResponse(ticketUid=ticket.ticketUid, status=ticket.status, flightNumber=ticket.flightNumber, fromAirport="", toAirport="", data=""))
            else:
                tickets.append(TicketResponse(ticketUid=ticket.ticketUid, status=ticket.status, flightNumber=ticket.flightNumber, fromAirport="", toAirport="", data=""))

            if response.status_code == HTTPStatus.OK:
                flight_json = response.json()
                tickets.append(TicketResponse(ticketUid=ticket.ticketUid, status=ticket.status, **flight_json))
    
    if circuitBreaker.isBlocked(bonusesHost):
        try:
            response = reqSession.get(f"http://{bonusesAPI}/bonuses/{x_user_name}")
            circuitBreaker.appendOK(bonusesHost)
        except requests.ConnectionError:
            circuitBreaker.append(bonusesHost)
            return UserInfoResponse(tickets=tickets, privilege={})
    else:
        return UserInfoResponse(tickets=tickets, privilege={})

    if response.status_code != HTTPStatus.OK:
        return JSONResponse(content={"message": "Bonus Service error"}, status_code=502)
    return UserInfoResponse(tickets=tickets, privilege=PrivilegeDataJSON(**response.json()))


@app.get('/api/v1/privilege', status_code=200)
def get_persons(x_user_name: str = Header()) -> PrivilegeInfoResponse:
    if circuitBreaker.isBlocked(bonusesHost):
        try:
            response = reqSession.get(f"http://{bonusesAPI}/history/{x_user_name}")
            circuitBreaker.appendOK(bonusesHost)
        except requests.ConnectionError:
            circuitBreaker.append(bonusesHost)
            return JSONResponse(content={"message": "Bonus Service unavailable"}, status_code=503)
    else:
        return JSONResponse(content={"message": "Bonus Service unavailable"}, status_code=503)

    if response.status_code != HTTPStatus.OK:
        return JSONResponse(content={"message": "Bonus Service error"}, status_code=502)
    return PrivilegeInfoResponse(**response.json())