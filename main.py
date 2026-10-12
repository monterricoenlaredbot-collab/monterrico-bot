#!/usr/bin/env python3
"""Monitor de resultados de Monterrico. Cola Supabase/Postgres en modo live.
Modo demo: SQLite local. No realiza envíos a WhatsApp.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import secrets
import sys
import time
from datetime import datetime
from decimal import Decimal
from urllib.parse import urlsplit
from storage import make_store
from zoneinfo import ZoneInfo

TRACK_TOPIC = "/broker/v2.tracks.es-PR"
TRACK_ID = 690
COUNTRY = "PE"
LOCAL_TZ = ZoneInfo("America/Lima")
OFFICIAL_STATUS = "OFFICIALLY_COMPLETED"


def stomp(topic: str, command: str = "SUBSCRIBE", sid: str = "sub-1") -> str:
    if command == "CONNECT":
        return "CONNECT\naccept-version:1.2,1.1\nhost:wager.telepodromo.net\nheart-beat:0,0\n\n\x00"
    return f"SUBSCRIBE\nid:{sid}\ndestination:{topic}\nack:auto\n\n\x00"


def parse_sockjs(text: str) -> list[tuple[str, str, object]]:
    """Extrae mensajes STOMP de un frame SockJS 'a["MESSAGE..."]'."""
    if not text.startswith("a"):
        return []
    try:
        frames = json.loads(text[1:])
    except json.JSONDecodeError:
        return []
    parsed = []
    for frame in frames:
        for part in frame.split("\x00"):
            if not part.strip():
                continue
            head, sep, body = part.partition("\n\n")
            first, *headers = head.split("\n")
            options = {}
            for header in headers:
                key, colon, value = header.partition(":")
                if colon:
                    options[key] = value
            try:
                payload = json.loads(body) if sep and body else None
            except json.JSONDecodeError:
                payload = body
            parsed.append((first.strip(), options.get("destination", ""), payload))
    return parsed


def money(value: object) -> str:
    return f"S/ {Decimal(str(value)):,.2f}"


def format_result(card_id: int, race_number: int, data: dict) -> str:
    lines = [f"🏇 MONTERRICO · CARRERA {race_number}", "✅ Resultado oficial", "", "🏆 ORDEN DE LLEGADA"]
    for runner in sorted(data.get("runnersResults", []), key=lambda r: r.get("position", 100))[:5]:
        position = int(runner["position"])
        num = runner.get("programNumber", "?")
        name = runner.get("horse", {}).get("name", "Sin nombre")
        marker = {1: "🥇", 2: "🥈", 3: "🥉"}.get(position, f"{position}.")
        lines.append(f"{marker} {num} – {name}")
    lines += ["", "💰 DIVIDENDOS"]
    bet_names = {"win": "Ganador", "place": "Place", "show": "Show"}
    post_prices = data.get("wpsPostPrices") or {}
    simple = []
    for key, title in bet_names.items():
        winners = [r for r in data.get("runnersResults", []) if key in (r.get("payouts") or {})]
        if winners:
            base = f" (base {money(post_prices[key])})" if key in post_prices else ""
            values = " | ".join(f"{r.get('programNumber')}: {money(r['payouts'][key])}" for r in winners)
            simple.append(f"{title}{base}: {values}")
    lines.extend(simple)
    for item in data.get("exoticBetsResults") or []:
        name = (item.get("betType") or {}).get("name", "Otra apuesta")
        combination = item.get("winningRunners", "")
        base = f" (base {money(item['amount'])})" if item.get("amount") else ""
        if item.get("payout") is not None:
            lines.append(f"{name}{base} [{combination}]: {money(item['payout'])}")
    lines += ["", "Fuente: Telepódromo", f"Cartilla: {card_id}"]
    return "\n".join(lines)


class Monitor:
    def __init__(self, store):
        self.db = store
        self.sender = None
        self.subscribed = set()
        self.seq = 1
        self.monitored = {}  # raceId -> (raceCardId, raceNumber, date)
        self.status = {}
        self.results = {}
        self.ready = False

    def set_sender(self, sender):
        self.sender = sender

    def subscribe(self, topic: str):
        if topic in self.subscribed or not self.sender:
            return
        sid = f"sub-{self.seq}"
        self.seq += 1
        self.sender(stomp(topic, sid=sid))
        self.subscribed.add(topic)
        print(f"  Suscripción: {topic}", flush=True)

    def tracks(self, catalog: list):
        # Solo hipódromo 690, país PE, nombre Monterrico.
        target = next((t for t in catalog if t.get("id") == TRACK_ID and t.get("country") == COUNTRY), None)
        if not target:
            print("No está Monterrico en el catálogo recibido.", flush=True)
            return
        current_date = datetime.now(LOCAL_TZ).date().isoformat()
        for card in target.get("raceCards") or []:
            if "monterrico" not in str(card.get("name", "")).lower():
                continue
            card_id = card["id"]
            card_date = (card.get("nextRace") or {}).get("date")
            if card_date and card_date != current_date:
                print(f"Ignorando cartilla de otra fecha: {card_date}", flush=True)
                continue
            races = card.get("races") or []
            print(f"Cartilla Monterrico {card_id}: {len(races)} carreras", flush=True)
            for race in races:
                race_id, number = int(race["id"]), int(race["raceNumber"])
                current_status = race.get("status")
                observed = self.db.is_observed(race_id)
                if not observed and current_status != OFFICIAL_STATUS:
                    self.db.mark_observed(race_id, card_id)
                    observed = True
                if not observed:
                    # Evitar redistribuir de golpe carreras ya terminadas al iniciar.
                    continue
                self.monitored[race_id] = (card_id, number, card_date)
                self.status[race_id] = current_status
                self.subscribe(f"/broker/v3.race.{race_id}.es-PR")
                self.subscribe(f"/broker/v2.race.{race_id}.es-PR.result")
                self.emit_if_ready(race_id)
        self.ready = True

    def accept(self, command: str, destination: str, payload):
        if command == "CONNECTED":
            print("STOMP conectado; solicitando catálogo de hipódromos...", flush=True)
            self.subscribe(TRACK_TOPIC)
        if command != "MESSAGE" or not isinstance(payload, (dict, list)):
            return
        if destination == TRACK_TOPIC and isinstance(payload, list):
            self.tracks(payload)
            return
        if not isinstance(payload, dict):
            return
        parts = destination.split(".")
        if len(parts) < 3 or not parts[2].isdigit():
            return
        race_id = int(parts[2])
        if race_id not in self.monitored:
            return
        if destination == f"/broker/v3.race.{race_id}.es-PR":
            if payload.get("raceCardId") == self.monitored[race_id][0]:
                self.status[race_id] = payload.get("status")
        elif destination == f"/broker/v2.race.{race_id}.es-PR.result":
            if payload.get("raceId") == race_id:
                self.results[race_id] = payload
        else:
            return
        self.emit_if_ready(race_id)

    def emit_if_ready(self, race_id: int):
        if self.status.get(race_id) != OFFICIAL_STATUS:
            return
        result = self.results.get(race_id)
        if not result or result.get("complete") is not True:
            return
        if not result.get("runnersResults"):
            return
        card_id, race_number, _ = self.monitored[race_id]
        if result.get("raceNumber") != race_number:
            print(f"Inconsistencia carrera {race_id}; no se publica.", file=sys.stderr)
            return
        message = format_result(card_id, race_number, result)
        digest = hashlib.sha256(message.encode("utf-8")).hexdigest()
        previous = self.db.existing_digest(race_id)
        if previous:
            if previous != digest:
                print(f"ATENCIÓN: resultado modificado en carrera {race_number}; revisión manual necesaria.", flush=True)
            return
        inserted = self.db.insert_outbox(race_id, card_id, message, digest)
        if not inserted:
            return
        print("\n" + "="*50 + "\n" + message + "\n" + "="*50 + "\n", flush=True)
        print("Resultado guardado en cola (PENDIENTE: integrar envío a WhatsApp).", flush=True)


def build_websocket_url() -> str:
    explicit = os.getenv("MON_WS_URL", "").strip()
    if explicit:
        if not explicit.startswith("wss://"):
            raise ValueError("MON_WS_URL debe empezar con wss://")
        return explicit
    # URL TENTATIVA de transporte SockJS, derivada de app.config.js.
    # Si el servidor usa una ruta distinta, copiar Request URL desde Chrome Headers.
    session = secrets.token_hex(4)
    server = os.getenv("MON_WS_SERVER_ID", "000").strip()
    if not (server.isdigit() and len(server) == 3):
        raise ValueError("MON_WS_SERVER_ID debe contener tres dígitos")
    return f"wss://wager.telepodromo.net/api/falcon/ws/{server}/{session}/websocket?X-G440-Tenant=peru"


def run_live(m: Monitor):
    try:
        from websocket import create_connection
    except ImportError as e:
        raise SystemExit("Instala la dependencia: pip install -r requirements.txt") from e
    while True:
        url = build_websocket_url()
        print(f"Conectando a {urlsplit(url).netloc} mediante SockJS/STOMP...", flush=True)
        m.subscribed.clear()
        m.seq = 1
        try:
            ws = create_connection(url, timeout=20, origin="https://wager.telepodromo.net")
            def send(frame):
                ws.send(json.dumps([frame]))
            m.set_sender(send)
            from websocket import WebSocketTimeoutException
            ws.settimeout(35)
            connected_at = time.monotonic()
            max_age = max(300, int(os.getenv("MON_RECONNECT_SECONDS", "1800")))
            while True:
                if time.monotonic() - connected_at >= max_age:
                    raise ConnectionError("Reconexión periódica para refrescar cartillas")
                try:
                    raw = ws.recv()
                except WebSocketTimeoutException:
                    continue
                if raw == "o":
                    send(stomp("", command="CONNECT"))
                    continue
                if raw == "h":
                    continue
                if raw == "c" or (isinstance(raw,str) and raw.startswith("c[")):
                    raise ConnectionError("SockJS cerró la conexión")
                if isinstance(raw,str):
                    for command, destination, payload in parse_sockjs(raw):
                        m.accept(command,destination,payload)
        except KeyboardInterrupt:
            print("Detenido por usuario.")
            return
        except Exception as err:
            print(f"Conexión interrumpida: {type(err).__name__}: {err}; reintento en 10 s.", flush=True)
            time.sleep(10)
        finally:
            try: ws.close()
            except Exception: pass


def run_demo(m: Monitor):
    today = datetime.now(LOCAL_TZ).date().isoformat()
    catalog = [
        {"id":5227,"country":"URU","raceCards":[{"id":58205,"name":"Maronas URU","races":[{"id":2644019,"raceNumber":14,"status":"PENDING"}]}]},
        {"id":690,"country":"PE","raceCards":[{"id":58215,"name":"Monterrico","nextRace":{"date":today},"races":[{"id":2644114,"raceNumber":1,"status":"OPEN"}]}]},
    ]
    def source(frame):
        print("[demo] instrucción STOMP preparada")
    m.set_sender(source)
    m.subscribe(TRACK_TOPIC)
    m.accept("MESSAGE",TRACK_TOPIC,catalog)
    m.accept("MESSAGE","/broker/v2.race.2644114.es-PR.result",{
        "raceId":2644114,"raceNumber":1,"complete":True,
        "wpsPostPrices":{"win":"3.00","place":"3.00"},
        "runnersResults":[
            {"position":1,"programNumber":"2","horse":{"name":"DR. BRUNO"},"payouts":{"win":"4.20","place":"3.60"}},
            {"position":2,"programNumber":"7","horse":{"name":"LUCIANA MARIA"},"payouts":{"place":"5.10"}},
            {"position":3,"programNumber":"1","horse":{"name":"COMANDO SVR"},"payouts":{}},
            {"position":4,"programNumber":"6","horse":{"name":"TED LASSO"},"payouts":{}}
        ],
        "exoticBetsResults":[{"betType":{"name":"Exacta"},"winningRunners":"2/7","amount":"2.00","payout":"9.60"},{"betType":{"name":"Trifecta"},"winningRunners":"2/7/1","amount":"2.00","payout":"29.80"}]
    })
    m.accept("MESSAGE","/broker/v3.race.2644114.es-PR",{"raceCardId":58215,"status":"OFFICIALLY_COMPLETED"})
    print("\nLa segunda ejecución con la misma base no volverá a encolar este resultado.")


def main():
    parser = argparse.ArgumentParser(description="Monitor Monterrico: etapa de captura, sin envío WhatsApp")
    parser.add_argument("--demo",action="store_true",help="Prueba con datos de ejemplo, sin acceso a Internet")
    parser.add_argument("--live",action="store_true",help="Intenta conectarse a Telepódromo usando SockJS/STOMP")
    args = parser.parse_args()
    if not (args.demo or args.live):
        parser.print_help()
        return
    m = Monitor(make_store(demo=args.demo))
    if args.demo:
        run_demo(m)
    elif args.live:
        run_live(m)

if __name__ == "__main__":
    main()
