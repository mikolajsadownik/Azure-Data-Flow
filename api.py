from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from connection import get_event_hub_status, inject_environment, send_to_event_hub
from data import generate_uber_ride_confirmation

inject_environment()

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")


@app.get("/")
def booking_home(request: Request):
    return templates.TemplateResponse(
        "home.html",
        {"request": request, "event_hub": get_event_hub_status()},
    )


@app.get("/book")
def book_redirect():
    return RedirectResponse(url="/", status_code=303)


@app.post("/book")
def book_ride(request: Request):
    ride = generate_uber_ride_confirmation()
    sent = send_to_event_hub(ride)
    if not sent:
        raise HTTPException(
            status_code=503,
            detail="Could not send ride confirmation to Event Hub.",
        )

    return templates.TemplateResponse(
        "confirmation.html",
        {
            "request": request,
            "event_hub": get_event_hub_status(),
            "ride": ride,
            "sent": sent,
        },
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
