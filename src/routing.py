# src/routing.py
import requests
from flask import make_response

# Import these from wherever they currently live in your project
# Adjust imports to match your structure.
from py.utils import getCountryFromCoordinates          # example
from src.graphhopper import convert_graphhopper_to_osrm     # example


def forward_routing_core(routingType, path, flask_request):
    # Normalize routing type
    if routingType in ("train", "tram", "metro"):
        routingType = "train"

    radiuses = None
    use_new_router = False  # defined for all paths

    # Determine base URL + (optional) return_code for bus
    return_code = None

    if routingType == "train":
        use_new_router = flask_request.args.get("use_new_router", "false").lower() == "true"
        base = "https://openrailrouting.maahl.net" if use_new_router else "http://routing.trainlog.me:5000"

    elif routingType == "ferry":
        base = "http://routing.trainlog.me:5001"
        coord_pairs = [
            {"lng": float(coord.split(",")[0]), "lat": float(coord.split(",")[1])}
            for coord in path.replace("route/v1/ferry/", "").split(";")
        ]
        radiuses = ";".join(["10000"] * len(coord_pairs))

    elif routingType == "aerialway":
        base = "http://routing.trainlog.me:5003"

    elif routingType == "car":
        base = "https://routing.openstreetmap.de/routed-car"

    elif routingType == "walk":
        base = "https://routing.openstreetmap.de/routed-foot"

    elif routingType == "cycle":
        base = "https://routing.openstreetmap.de/routed-bike"

    elif routingType == "bus":
        routers = {
            "trainlog": ("http://routing.trainlog.me:5002", 231),
            "chiel": ("https://busrouter.chiel.uk", 232),
            "jkimb": ("https://busrouter.jkimball.dev", 233),
            "fallback": ("https://routing.openstreetmap.de/routed-car", 234),
        }

        routing_groups = [
            {
                "countries": {"NO", "SE", "FI", "DK", "GB", "IE", "IS", "IM", "FO", "GG", "JE"},
                "router": routers["trainlog"],
            },
            {
                "countries": {
                    "DE", "AT", "CH", "LI", "LU",
                    "EE", "LV", "LT",
                    "FR", "BE", "NL", "AD", "MC",
                    "PL", "CZ", "HU",
                    "IT", "ES", "PT",
                },
                "router": routers["chiel"],
            },
            {
                "countries": {"US", "CA", "GL", "MX"},
                "router": routers["jkimb"],
            },
        ]

        coord_pairs = [
            {"lng": float(coord.split(",")[0]), "lat": float(coord.split(",")[1])}
            for coord in path.replace("route/v1/driving/", "").split(";")
        ]

        countries = []
        for wp in coord_pairs:
            try:
                countries.append(getCountryFromCoordinates(wp["lat"], wp["lng"])["countryCode"])
            except Exception:
                countries.append("UN")

        unique_countries = set(countries)

        base, return_code = routers["fallback"]
        for group in routing_groups:
            if unique_countries.issubset(group["countries"]):
                base, return_code = group["router"]
                break

    else:
        # Optional: make unknown routing types explicit
        return make_response({"error": f"Unsupported routingType: {routingType}"}, 400)

    # Build args from incoming request
    args = flask_request.query_string.decode("utf-8") if flask_request.query_string else ""
    # remove use_new_router=true from forwarded query string
    args = (
        args.replace("&use_new_router=true", "")
            .replace("use_new_router=true&", "")
            .replace("use_new_router=true", "")
    ).strip("&")

    def build_url(base_url):
        q = f"?{args}" if args else ""
        full_url = f"{base_url}/{path}{q}"
        if routingType == "ferry" and radiuses:
            full_url += ("&" if q else "?") + f"radiuses={radiuses}"
        return full_url

    def build_gh_url(base_url):
        coords_part = path.split("/")[-1]
        points = []
        for coord in coords_part.split(";"):
            lon, lat = coord.split(",")
            points.append(f"point={lat}%2C{lon}")
        point_params = "&".join(points)

        full_url = (
            f"{base_url}/route?"
            f"{point_params}&type=json&profile=all&details=electrified&details=distance"
        )

        if routingType == "ferry" and radiuses:
            full_url += f"&radiuses={radiuses}"
        return full_url

    # Behavior per type
    if routingType == "bus":
        routers_fallback_base = "https://routing.openstreetmap.de/routed-car"
        try:
            response = requests.get(build_url(base), timeout=5)
            if response.status_code != 200:
                raise Exception("Non-200 response")

            data = response.json()
            if data.get("status") == "NoRoute":
                raise Exception("Router responded with NoRoute")

            return make_response(data, return_code)
        except Exception as e:
            fallback_url = build_url(routers_fallback_base)
            return make_response(requests.get(fallback_url).json(), 235)

    if routingType == "train" and use_new_router:
        gh_json = requests.get(build_gh_url(base), timeout=10).json()
        return convert_graphhopper_to_osrm(gh_json)

    # All other types: just proxy text
    return requests.get(build_url(base), timeout=10).text
