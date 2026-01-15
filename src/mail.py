from imapclient import IMAPClient
import threading
import email as email_lib
from email.header import decode_header
from email.utils import parseaddr
import time
import logging
import json
import os
import requests
from datetime import datetime
from pypdf import PdfReader
from icalendar import Calendar
from io import BytesIO

from py.utils import load_config, getCountryFromCoordinates, get_flag_emoji, getDistance
from src.trips import Trip, create_trip
from src.users import User
from src.utils import sendEmail, lang
from src.routing import forward_routing_core

logger = logging.getLogger(__name__)
_app = None

class FakeRequest:
    """Minimal request object for forward_routing_core."""
    def __init__(self):
        self.query_string = b"overview=full&geometries=geojson"
        self.args = {"use_new_router": "false"}

def route_path(origin, destination, trip_type):
    """Get routed path for ground transportation. Returns list of {lat, lng} or None on failure."""
    routable_types = {"train", "tram", "metro", "bus", "car", "walk", "cycle"}
    if trip_type not in routable_types:
        return None
    
    routing_type_map = {"tram": "train", "metro": "train"}
    routing_type = routing_type_map.get(trip_type, trip_type)
    
    coords = f"{origin['lng']},{origin['lat']};{destination['lng']},{destination['lat']}"
    path = f"route/v1/{'driving' if routing_type in ('bus', 'car') else routing_type}/{coords}"
    
    try:
        result = forward_routing_core(routing_type, path, FakeRequest())
        if hasattr(result, 'get_json'):
            data = result.get_json()
        elif isinstance(result, str):
            data = json.loads(result)
        else:
            data = result
        
        if data and "routes" in data and data["routes"]:
            geometry = data["routes"][0].get("geometry", {})
            coords_list = geometry.get("coordinates", [])
            if coords_list:
                return [{"lat": c[1], "lng": c[0]} for c in coords_list]
    except Exception as e:
        logger.warning(f"Routing failed for {trip_type}: {e}")
    
    return None

def get_email_body(msg):
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_type() == "text/plain":
                payload = part.get_payload(decode=True)
                if payload:
                    return payload.decode(errors="ignore")
            elif part.get_content_type() == "text/html":
                payload = part.get_payload(decode=True)
                if payload:
                    return payload.decode(errors="ignore")
    else:
        payload = msg.get_payload(decode=True)
        if payload:
            return payload.decode(errors="ignore")
    return ""

def extract_attachments(msg):
    """Extract ICS and PDF attachments from email."""
    attachments = {"ics": [], "pdf": []}
    
    if not msg.is_multipart():
        return attachments
    
    for part in msg.walk():
        content_type = part.get_content_type()
        filename = part.get_filename()
        
        if content_type == "text/calendar" or (filename and filename.lower().endswith(".ics")):
            payload = part.get_payload(decode=True)
            if payload:
                attachments["ics"].append({"filename": filename, "data": payload})
        
        elif content_type == "application/pdf" or (filename and filename.lower().endswith(".pdf")):
            payload = part.get_payload(decode=True)
            if payload:
                attachments["pdf"].append({"filename": filename, "data": payload})
    
    return attachments

def parse_ics_content(ics_data):
    """Parse ICS data and extract trip-relevant info."""
    events = []
    try:
        cal = Calendar.from_ical(ics_data)
        for component in cal.walk():
            if component.name == "VEVENT":
                dtstart = component.get("dtstart")
                dtend = component.get("dtend")
                event = {
                    "summary": str(component.get("summary", "")),
                    "description": str(component.get("description", "")),
                    "location": str(component.get("location", "")),
                    "dtstart": dtstart.dt if dtstart else None,
                    "dtend": dtend.dt if dtend else None,
                }
                events.append(event)
    except Exception as e:
        logger.error(f"ICS parsing error: {e}")
    return events

def extract_pdf_text(pdf_data):
    """Extract text from PDF bytes using pypdf."""
    text = ""
    try:
        reader = PdfReader(BytesIO(pdf_data))
        for page in reader.pages:
            page_text = page.extract_text()
            if page_text:
                text += page_text + "\n"
    except Exception as e:
        logger.error(f"PDF extraction error: {e}")
    return text

def get_user_from_sender(sender_raw):
    _, email_address = parseaddr(sender_raw)
    email_address = email_address.lower()
    
    user = User.query.filter_by(email=email_address).first()
    if not user:
        logger.info(f"No user found for email: {email_address}")
        try:
            sendEmail(
                email_address,
                "Trainlog - Email not recognized",
                """<h2>Email not recognized</h2>
                <p>We received your email but could not find a Trainlog account associated with this address.</p>
                <p>Please make sure you're sending from the email address registered with your Trainlog account.</p>
                <p><a href="https://trainlog.me">Visit Trainlog</a></p>"""
            )
        except Exception as e:
            logger.error(f"Failed to send unrecognized email notice: {e}")
        return None
    
    if not user.premium:
        logger.info(f"User {user.username} is not premium, ignoring email")
        l = lang.get(user.lang, lang["en"])
        try:
            sendEmail(
                email_address,
                l["email_premium_subject"],
                f"""<h2>{l["email_premium_title"]}</h2>
                <p>{l["email_premium_greeting"].format(username=user.username)}</p>
                <p>{l["email_premium_description"]}</p>
                <p><a href="https://buymeacoffee.com/trainlog/membership">{l["email_premium_cta"]}</a></p>"""
            )
        except Exception as e:
            logger.error(f"Failed to send premium notice: {e}")
        return None
    
    return user

def get_airport_by_iata(iata):
    from src.utils import mainConn, managed_cursor
    with managed_cursor(mainConn) as cursor:
        result = cursor.execute(
            "SELECT name, latitude, longitude, iso_country FROM airports WHERE iata = ?",
            (iata.upper(),)
        ).fetchone()
        if result:
            return dict(result)
    return None

def geocode_station(query, trip_type="train", fallback_coords=None):
    from py.utils import getCountryFromCoordinates
    
    osm_tags = {
        "bus": ["amenity:bus_station", "highway:bus_stop"],
        "train": ["railway:halt", "railway:station"],
        "tram": ["railway:tram_stop", "railway:station", "railway:halt"],
        "metro": ["railway:station", "railway:subway_entrance"],
        "ferry": ["amenity:ferry_terminal"],
        "helicopter": ["aeroway:helipad", "aeroway:heliport", "aeroway:aerodrome"],
        "accommodation": ["tourism:alpine_hut", "tourism:apartment", "tourism:chalet", "tourism:guest_house", "tourism:hostel", "tourism:hotel", "tourism:motel", "tourism:wilderness_hut"],
        "restaurant": ["amenity:restaurant", "amenity:pub", "amenity:biergarten", "amenity:cafe", "amenity:bar"],
        "aerialway": ["aerialway:station"],
    }
    
    komoot = "https://photon.komoot.io/api"
    chiel = "https://photon.chiel.uk/api"
    timeout = 10
    
    params = [("q", query), ("limit", 1), ("lang", "en")]
    for tag in osm_tags.get(trip_type, []):
        params.append(("osm_tag", tag))
    
    data = None
    for url in [chiel, komoot]:
        try:
            resp = requests.get(url, params=params, timeout=timeout)
            resp.raise_for_status()
            data = resp.json()
            if data.get("features"):
                break
        except Exception as e:
            logger.debug(f"Geocoding {url} failed: {e}")
            continue
    
    # Use AI fallback coordinates if geocoding failed
    if (not data or not data.get("features")) and fallback_coords:
        lat, lng = fallback_coords
        country = getCountryFromCoordinates(lat, lng)
        return {
            "name": query,
            "lat": lat,
            "lng": lng,
            "country_code": country.get("countryCode", "")
        }
    
    if not data or not data.get("features"):
        return None
    
    feat = data["features"][0]
    props = feat["properties"]
    lng, lat = feat["geometry"]["coordinates"]
    
    country_code = props.get("countrycode", "")
    if not country_code or country_code in ["CN", "FI"]:
        country = getCountryFromCoordinates(lat, lng)
        country_code = country.get("countryCode", "")
    
    name = props.get("name", query)
    city = props.get("city")
    if city and city.lower() not in name.lower():
        name = f"{city} - {name}"
    
    return {
        "name": name,
        "lat": lat,
        "lng": lng,
        "country_code": country_code
    }

def get_original_email_date(msg):
    """Extract original send date from potentially forwarded email."""
    from email.utils import parsedate_to_datetime
    date_str = msg.get("Date")
    if date_str:
        try:
            return parsedate_to_datetime(date_str).date()
        except:
            pass
    return datetime.now().date()


def parse_ticket_with_ai(subject, body, user_lang="en", ics_events=None, pdf_texts=None):
    config = load_config()
    api_key = config.get("infomaniak_ai", {}).get("api_key")
    if not api_key:
        logger.error("No Mistral API key found")
        return None
    
    lang_names = {
        "en": "English", "fr": "French", "de": "German", "es": "Spanish",
        "it": "Italian", "pt": "Portuguese", "nl": "Dutch", "pl": "Polish",
        "cs": "Czech", "ja": "Japanese", "zh": "Chinese", "ko": "Korean"
    }
    lang_name = lang_names.get(user_lang, "English")
    
    attachment_info = ""
    if ics_events:
        attachment_info += "\n\nICS CALENDAR DATA:\n"
        for i, evt in enumerate(ics_events, 1):
            attachment_info += f"Event {i}: {evt['summary']}\n"
            attachment_info += f"  Location: {evt['location']}\n"
            attachment_info += f"  Start: {evt['dtstart']}\n"
            attachment_info += f"  End: {evt['dtend']}\n"
            if evt['description']:
                attachment_info += f"  Description: {evt['description'][:500]}\n"
    
    if pdf_texts:
        attachment_info += "\n\nPDF ATTACHMENT CONTENT:\n"
        for i, text in enumerate(pdf_texts, 1):
            attachment_info += f"--- PDF {i} ---\n{text[:3000]}\n"
    
    prompt = f"""Extract all trips from this ticket confirmation email.
A trip is ONE segment (e.g., a flight with one connection = 2 trips).

Return ONLY valid JSON array, no markdown:
[{{
  "type": "train|air|bus|ferry|tram|metro",
  "origin": "City or Station name",
  "origin_iata": "ABC or null if not a flight",
  "origin_lat": latitude as number or null,
  "origin_lng": longitude as number or null,
  "destination": "City or Station name", 
  "destination_iata": "XYZ or null if not a flight",
  "destination_lat": latitude as number or null,
  "destination_lng": longitude as number or null,
  "date": "YYYY-MM-DD departure date",
  "arrival_date": "YYYY-MM-DD or null if same day as departure",
  "time_departure": "HH:MM or null",
  "time_arrival": "HH:MM or null",
  "operator": "Company name",
  "line_name": "Flight number or train number or null",
  "price": number or null,
  "currency": "ISO 4217 EUR|USD|etc or null",
  "aircraft_icao": "ICAO code like B738, A320, E295 or null if not a flight or unknown",
  "seat": "Seat number like 12A 08D or null",
  "booking_reference": "PNR/booking code or null",
  "ticket_number": "Ticket number or null",
  "cabin_class": "Economy/Business/First or null",
  "notes": "Any other relevant info in {lang_name}, or null"
}}]

For coordinates: provide approximate lat/lng if you know the location (e.g. major stations/cities).
For aircraft types, use ICAO codes: Boeing 737-800=B738, Airbus A320=A320, Embraer 195=E195, etc.
For multi-day trips (ferries, overnight trains), always provide arrival_date.

If not a valid ticket, return []

Subject: {subject}
Body: {body}{attachment_info}"""

    try:
        response = requests.post(
            "https://api.infomaniak.com/1/ai/106774/openai/chat/completions",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json"
            },
            json={
                "model": "mistral24b",
                "messages": [{"role": "user", "content": prompt}]
            }
        )
        result = response.json()
        content = result["choices"][0]["message"]["content"]
        content = content.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()
        return json.loads(content)
    except Exception as e:
        logger.error(f"AI parsing error: {e}")
        return None

def build_notes(parsed_trip, user_lang="en"):
    parts = ["From Trainlog email parser"]
    
    if parsed_trip.get("booking_reference"):
        parts.append(f"PNR: {parsed_trip['booking_reference']}")
    
    if parsed_trip.get("ticket_number"):
        parts.append(f"Ticket: {parsed_trip['ticket_number']}")
    
    if parsed_trip.get("cabin_class"):
        parts.append(f"Class: {parsed_trip['cabin_class']}")
    
    if parsed_trip.get("notes"):
        parts.append(parsed_trip["notes"])
    
    return " | ".join(parts)

def create_trip_from_parsed(user, parsed_trip, purchase_date=None, ics_source=False):
    from timezonefinder import TimezoneFinder
    import pytz
    from src.utils import getLocalDatetime
    
    trip_type = parsed_trip.get("type", "train")
    now = datetime.now()
    
    if trip_type == "air":
        origin_iata = parsed_trip.get("origin_iata")
        dest_iata = parsed_trip.get("destination_iata")
        
        origin_airport = get_airport_by_iata(origin_iata) if origin_iata else None
        dest_airport = get_airport_by_iata(dest_iata) if dest_iata else None
        
        if not origin_airport or not dest_airport:
            logger.error(f"Could not find airports: {origin_iata}, {dest_iata}")
            return None
        
        origin_flag = get_flag_emoji(origin_airport["iso_country"])
        dest_flag = get_flag_emoji(dest_airport["iso_country"])
        
        origin_station = f"{origin_flag} {origin_airport['name']} ({origin_iata.upper()})"
        dest_station = f"{dest_flag} {dest_airport['name']} ({dest_iata.upper()})"
        
        path = [
            {"lat": origin_airport["latitude"], "lng": origin_airport["longitude"]},
            {"lat": dest_airport["latitude"], "lng": dest_airport["longitude"]}
        ]
        
        trip_length = getDistance(path[0], path[-1])
        origin_country = getCountryFromCoordinates(path[0]["lat"], path[0]["lng"])
        dest_country = getCountryFromCoordinates(path[-1]["lat"], path[-1]["lng"])
        countries = json.dumps({
            origin_country["countryCode"]: trip_length / 2,
            dest_country["countryCode"]: trip_length / 2
        })
        
        material_type = parsed_trip.get("aircraft_icao")
    else:
        origin_fallback = None
        dest_fallback = None
        if parsed_trip.get("origin_lat") and parsed_trip.get("origin_lng"):
            origin_fallback = (parsed_trip["origin_lat"], parsed_trip["origin_lng"])
        if parsed_trip.get("destination_lat") and parsed_trip.get("destination_lng"):
            dest_fallback = (parsed_trip["destination_lat"], parsed_trip["destination_lng"])
        
        origin_geo = geocode_station(parsed_trip["origin"], trip_type, origin_fallback)
        dest_geo = geocode_station(parsed_trip["destination"], trip_type, dest_fallback)
        
        if not origin_geo or not dest_geo:
            logger.error(f"Could not geocode: {parsed_trip['origin']}, {parsed_trip['destination']}")
            return None
        
        origin_flag = get_flag_emoji(origin_geo["country_code"])
        dest_flag = get_flag_emoji(dest_geo["country_code"])
        
        origin_station = f"{origin_flag} {origin_geo['name']}"
        dest_station = f"{dest_flag} {dest_geo['name']}"
        
        origin_point = {"lat": origin_geo["lat"], "lng": origin_geo["lng"]}
        dest_point = {"lat": dest_geo["lat"], "lng": dest_geo["lng"]}
        
        routed_path = route_path(origin_point, dest_point, trip_type)
        if routed_path:
            path = routed_path
            logger.info(f"Used routing for {trip_type}: {len(path)} points")
        else:
            path = [origin_point, dest_point]
            logger.info(f"Fallback to straight line for {trip_type}")
        
        trip_length = getDistance(path[0], path[-1])
        countries = "{}"
        material_type = None
    
    start_datetime = None
    end_datetime = None
    utc_start_datetime = None
    utc_end_datetime = None
    estimated_duration = None
    
    tf = TimezoneFinder()
    # Check if we have UTC datetimes from ICS
    utc_start = parsed_trip.get("utc_start_datetime")
    utc_end = parsed_trip.get("utc_end_datetime")
    
    if utc_start:
        # ICS source: times are already UTC
        utc_start_datetime = utc_start.replace(tzinfo=None) if hasattr(utc_start, 'replace') else utc_start
        start_datetime = getLocalDatetime(path[0]["lat"], path[0]["lng"], utc_start.replace(tzinfo=pytz.UTC) if utc_start.tzinfo is None else utc_start)
        
        if utc_end:
            utc_end_datetime = utc_end.replace(tzinfo=None) if hasattr(utc_end, 'replace') else utc_end
            end_datetime = getLocalDatetime(path[-1]["lat"], path[-1]["lng"], utc_end.replace(tzinfo=pytz.UTC) if utc_end.tzinfo is None else utc_end)
        else:
            utc_end_datetime = utc_start_datetime
            end_datetime = start_datetime
    else:
        # AI/email source: times are local
        trip_date = parsed_trip.get("date")
        arrival_date = parsed_trip.get("arrival_date") or trip_date
        dep_time = parsed_trip.get("time_departure")
        arr_time = parsed_trip.get("time_arrival")
        
        if trip_date:
            if dep_time:
                start_datetime = datetime.strptime(f"{trip_date} {dep_time}", "%Y-%m-%d %H:%M")
                origin_tz_name = tf.timezone_at(lat=path[0]["lat"], lng=path[0]["lng"])
                if origin_tz_name:
                    origin_tz = pytz.timezone(origin_tz_name)
                    local_start = origin_tz.localize(start_datetime)
                    utc_start_datetime = local_start.astimezone(pytz.UTC).replace(tzinfo=None)
            else:
                start_datetime = datetime.strptime(f"{trip_date} 00:00:01", "%Y-%m-%d %H:%M:%S")
            
            if arr_time:
                end_datetime = datetime.strptime(f"{arrival_date} {arr_time}", "%Y-%m-%d %H:%M")
                dest_tz_name = tf.timezone_at(lat=path[-1]["lat"], lng=path[-1]["lng"])
                if dest_tz_name:
                    dest_tz = pytz.timezone(dest_tz_name)
                    local_end = dest_tz.localize(end_datetime)
                    utc_end_datetime = local_end.astimezone(pytz.UTC).replace(tzinfo=None)
            elif arrival_date and arrival_date != trip_date:
                end_datetime = datetime.strptime(f"{arrival_date} 23:59", "%Y-%m-%d %H:%M")
                dest_tz_name = tf.timezone_at(lat=path[-1]["lat"], lng=path[-1]["lng"])
                if dest_tz_name:
                    dest_tz = pytz.timezone(dest_tz_name)
                    local_end = dest_tz.localize(end_datetime)
                    utc_end_datetime = local_end.astimezone(pytz.UTC).replace(tzinfo=None)
            else:
                end_datetime = start_datetime
                utc_end_datetime = utc_start_datetime
    
    if utc_start_datetime and utc_end_datetime:
        estimated_duration = int((utc_end_datetime - utc_start_datetime).total_seconds())
        if estimated_duration < 0:
            estimated_duration = None
    
    is_project = False
    notes = build_notes(parsed_trip, user.lang)
    
    trip = Trip(
        username=user.username,
        user_id=user.uid,
        origin_station=origin_station,
        destination_station=dest_station,
        start_datetime=start_datetime,
        end_datetime=end_datetime,
        utc_start_datetime=utc_start_datetime,
        utc_end_datetime=utc_end_datetime,
        trip_length=trip_length,
        estimated_trip_duration=estimated_duration,
        manual_trip_duration=None,
        operator=parsed_trip.get("operator"),
        line_name=parsed_trip.get("line_name"),
        created=now,
        last_modified=now,
        type=trip_type,
        price=parsed_trip.get("price"),
        currency=parsed_trip.get("currency"),
        purchasing_date=purchase_date,
        ticket_id=None,
        is_project=is_project,
        path=path,
        countries=countries,
        seat=parsed_trip.get("seat"),
        material_type=material_type,
        reg=None,
        waypoints=None,
        notes=notes,
    )
    
    create_trip(trip)
    logger.info(f"Created trip for {user.username}")
    return trip

def send_confirmation_email(user, created_trips, subject):
    config = load_config()
    base_url = "https://trainlog.me"
    
    trip_ids = ",".join(str(t.trip_id) for t in created_trips)
    trip_url = f"{base_url}/public/trip/{trip_ids}"
    
    trip_lines = []
    for t in created_trips:
        date_str = t.start_datetime.strftime("%Y-%m-%d") if t.start_datetime else "?"
        trip_lines.append(f"• {t.origin_station} → {t.destination_station} ({date_str})")
    trips_summary = "<br>".join(trip_lines)
    
    l = lang.get(user.lang, lang["en"])
    
    body = f"""
        <h2>{l["email_success_title"]}</h2>
        <p>{l["email_received"]}: <strong>{subject}</strong></p>
        <p><strong>{len(created_trips)} {l["email_trips_added"]}</strong></p>
        <p>{trips_summary}</p>
        <p><a href="{trip_url}">{l["email_view_trips"]}</a></p>
    """
    
    sendEmail(user.email, l["email_success_subject"], body)
    logger.info(f"Sent confirmation email to {user.email}")

def send_error_email(user, subject, error_message):
    l = lang.get(user.lang, lang["en"])
    
    body = f"""
        <h2>{l["email_error_title"]}</h2>
        <p>{l["email_received"]}: <strong>{subject}</strong></p>
        <p>{l["email_error_description"]}</p>
        <p><em>{error_message}</em></p>
        <p>{l["email_error_advice"]}</p>
    """
    
    try:
        sendEmail(user.email, l["email_error_subject"], body)
        logger.info(f"Sent error email to {user.email}")
    except Exception as e:
        logger.error(f"Failed to send error email to {user.email}: {e}")

def send_no_trips_email(user, subject):
    l = lang.get(user.lang, lang["en"])
    
    body = f"""
        <h2>{l["email_no_trips_title"]}</h2>
        <p>{l["email_received"]}: <strong>{subject}</strong></p>
        <p>{l["email_no_trips_description"]}</p>
        <p>{l["email_no_trips_formats"]}</p>
    """
    
    try:
        sendEmail(user.email, l["email_no_trips_subject"], body)
        logger.info(f"Sent no-trips email to {user.email}")
    except Exception as e:
        logger.error(f"Failed to send no-trips email to {user.email}: {e}")

def process_incoming_email(raw):
    msg = email_lib.message_from_bytes(raw)
    sender = msg["From"]
    
    with _app.app_context():
        user = get_user_from_sender(sender)
        if not user:
            return
        
        subject = "Unknown"
        try:
            subject_raw, enc = decode_header(msg["Subject"])[0]
            if isinstance(subject_raw, bytes):
                subject = subject_raw.decode(enc or "utf-8")
            else:
                subject = subject_raw
        except Exception as e:
            logger.error(f"Failed to decode subject: {e}")
        
        body = get_email_body(msg)
        purchase_date = get_original_email_date(msg)
        
        attachments = extract_attachments(msg)
        ics_events = []
        pdf_texts = []
        
        for att in attachments["ics"]:
            events = parse_ics_content(att["data"])
            ics_events.extend(events)
        
        for att in attachments["pdf"]:
            text = extract_pdf_text(att["data"])
            if text.strip():
                pdf_texts.append(text)
        
        logger.info(f"Processing email from {user.username} (ICS: {len(ics_events)}, PDFs: {len(pdf_texts)})")
        
        try:
            trips = parse_ticket_with_ai(subject, body, user.lang, ics_events, pdf_texts)
        except Exception as e:
            logger.error(f"AI parsing failed: {e}")
            send_error_email(user, subject, "Failed to analyze the email content.")
            return
        
        if not trips:
            logger.info("No trips found in email")
            send_no_trips_email(user, subject)
            return
        
        logger.info(f"Found {len(trips)} trip(s)")
        created_trips = []
        errors = []
        
        for i, parsed in enumerate(trips):
            try:
                trip = create_trip_from_parsed(user, parsed, purchase_date)
                if trip:
                    created_trips.append(trip)
                else:
                    errors.append(f"Trip {i+1}: Could not geocode locations")
            except Exception as e:
                logger.error(f"Failed to create trip {i+1}: {e}")
                errors.append(f"Trip {i+1}: {str(e)}")
        
        if created_trips:
            try:
                send_confirmation_email(user, created_trips, subject)
            except Exception as e:
                logger.error(f"Failed to send confirmation email: {e}")
            
            if errors:
                error_msg = f"Created {len(created_trips)} trip(s), but some failed: " + "; ".join(errors)
                send_error_email(user, subject, error_msg)
        else:
            error_msg = "Could not create any trips. " + "; ".join(errors) if errors else "Unknown error occurred."
            send_error_email(user, subject, error_msg)

def email_listener():
    config = load_config()
    cfg = config.get("email_receiver")
    if not cfg:
        logger.warning("No email_receiver config found, skipping email listener")
        return
    
    while True:
        try:
            client = IMAPClient(cfg["imap"], ssl=True)
            client.login(cfg["user"], cfg["password"])
            client.select_folder("INBOX")
            logger.info("Email listener connected")
            
            while True:
                client.idle()
                responses = client.idle_check(timeout=300)
                client.idle_done()
                
                if responses:
                    messages = client.search("UNSEEN")
                    for msg_id in messages:
                        raw = client.fetch([msg_id], ["RFC822"])[msg_id][b"RFC822"]
                        process_incoming_email(raw)
                        
        except Exception as e:
            logger.error(f"Email listener error: {e}")
            time.sleep(10)

def start_email_listener(app):
    global _app
    _app = app
    thread = threading.Thread(target=email_listener, daemon=True)
    thread.start()