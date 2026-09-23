#!/usr/bin/env python3
"""
bordeaux_flights_site_officiel.py

Suivi quasi temps réel des arrivées et départs de l'aéroport de Bordeaux,
à partir du site officiel (page "Arrivées et départs du jour").

Conçu pour tourner plusieurs fois par jour (voir le workflow GitHub Actions
fourni), avec :
  - un délai aléatoire (0 à 10 min) avant chaque exécution, pour ne pas
    interroger le site à horaire pile fixe ;
  - une petite pause aléatoire entre les deux requêtes (arrivées / départs) ;
  - des tentatives avec attente progressive en cas d'erreur réseau ou de
    blocage temporaire (429 / 5xx), sans jamais marteler le site ;
  - un User-Agent réaliste choisi au hasard dans un petit pool à chaque
    exécution ;
  - un historique persistant (data/history.jsonl, une ligne par vol observé
    à chaque exécution) + un instantané du jour (data/latest.json), tous
    deux committés dans le dépôt à chaque exécution ;
  - un envoi optionnel du résultat vers un autre système, par webhook HTTP
    (variable d'environnement EXPORT_WEBHOOK_URL).

Usage :
    python bordeaux_flights_site_officiel.py                # aujourd'hui
    python bordeaux_flights_site_officiel.py --date 2026-09-20
    python bordeaux_flights_site_officiel.py --no-jitter     # utile en test
"""

import argparse
import io
import json
import os
import random
import re
import sys
import time
from datetime import date, datetime
from zoneinfo import ZoneInfo

import pandas as pd
import requests

BASE_URL = "https://www.bordeaux.aeroport.fr/vols-destinations/arrivees-departs-du-jour"
PARIS_TZ = ZoneInfo("Europe/Paris")

# Petit pool de User-Agents réalistes, un choisi au hasard à chaque exécution
# (juste pour ne pas avoir une empreinte parfaitement statique dans le temps).
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.5 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Firefox/126.0",
]

MAX_RETRIES = 3


def build_headers() -> dict:
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept-Language": "fr-FR,fr;q=0.9",
    }


def clean_text(value) -> str:
    if pd.isna(value):
        return ""
    return re.sub(r"\s+", " ", str(value)).strip().strip("*").strip()


def get_with_retries(params: dict, headers: dict) -> requests.Response:
    """Requête HTTP avec tentatives progressives sur erreurs transitoires
    uniquement (429 / 5xx / réseau). Les erreurs définitives (ex: 404)
    remontent immédiatement, sans retenter inutilement."""
    delay = 3
    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(BASE_URL, params=params, headers=headers, timeout=30)
        except requests.RequestException as exc:
            last_error = exc
        else:
            if resp.status_code == 200:
                resp.encoding = "utf-8"
                return resp
            if resp.status_code in (429, 500, 502, 503, 504):
                last_error = requests.HTTPError(f"HTTP {resp.status_code}")
            else:
                resp.raise_for_status()  # erreur non transitoire : on arrête tout de suite
        if attempt < MAX_RETRIES:
            print(f"  Tentative {attempt} échouée ({last_error}), nouvel essai dans {delay}s...")
            time.sleep(delay)
            delay *= 2
    raise RuntimeError(f"Échec après {MAX_RETRIES} tentatives : {last_error}")


def fetch_table(direction: str, day: date) -> pd.DataFrame:
    """direction : 'in' (arrivées) ou 'out' (départs)."""
    params = {"w": direction, "date": day.strftime("%d/%m/%Y"), "time": "00:00"}
    resp = get_with_retries(params, build_headers())

    tables = pd.read_html(io.StringIO(resp.text))
    for t in tables:
        cols = [clean_text(c) for c in t.columns]
        if any("Numéro de vol" in c for c in cols):
            t.columns = cols
            return t
    return pd.DataFrame()


def normalize(df: pd.DataFrame, direction: str, day: date, checked_at: datetime) -> pd.DataFrame:
    if df.empty:
        return df

    loc_col = "Provenance" if "Provenance" in df.columns else "Destination"
    if "Sortie" in df.columns:
        terminal_col = "Sortie"
    elif "Embarquement" in df.columns:
        terminal_col = "Embarquement"
    else:
        terminal_col = None

    out = pd.DataFrame(
        {
            "date_vol": day.strftime("%Y-%m-%d"),
            "type": "Atterrissage" if direction == "in" else "Décollage",
            "heure": df["Heure"].map(clean_text),
            "ville": df[loc_col].map(clean_text),
            "numero_vol": df["Numéro de vol"].map(clean_text),
            "compagnie": df["Compagnie aérienne"].map(clean_text),
            "terminal_porte": df[terminal_col].map(clean_text) if terminal_col else "",
            "statut": df["Statut"].map(clean_text) if "Statut" in df.columns else "",
            "consulte_a": checked_at.isoformat(timespec="seconds"),
        }
    )
    # Les codeshares (plusieurs numéros/compagnies pour un même vol physique)
    # restent groupés tels qu'affichés par le site, séparés par un espace.
    out = out[out["numero_vol"] != ""]
    return out


def send_webhook(records: list) -> None:
    url = os.environ.get("EXPORT_WEBHOOK_URL")
    if not url:
        return
    try:
        resp = requests.post(url, json={"flights": records}, timeout=20)
        resp.raise_for_status()
        print(f"Envoyé à {url} ({len(records)} enregistrements).")
    except requests.RequestException as exc:
        print(f"Avertissement : l'envoi au webhook a échoué ({exc}).")


def append_history(records: list, history_path: str) -> None:
    os.makedirs(os.path.dirname(history_path), exist_ok=True)
    with open(history_path, "a", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def write_latest(records: list, latest_path: str) -> None:
    os.makedirs(os.path.dirname(latest_path), exist_ok=True)
    with open(latest_path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Suivi des vols à l'aéroport de Bordeaux, depuis le site officiel."
    )
    parser.add_argument(
        "--date", type=str, default=None,
        help="AAAA-MM-JJ (par défaut : aujourd'hui, heure de Paris).",
    )
    parser.add_argument("--out-dir", type=str, default="data")
    parser.add_argument(
        "--no-jitter", action="store_true",
        help="Désactive le délai aléatoire (utile en test/développement).",
    )
    args = parser.parse_args()

    if not args.no_jitter:
        jitter = random.uniform(0, 600)  # 0 à 10 minutes de délai aléatoire
        print(f"Pause aléatoire de {jitter:.0f}s avant de commencer...")
        time.sleep(jitter)

    checked_at = datetime.now(PARIS_TZ)
    target_day = date.fromisoformat(args.date) if args.date else checked_at.date()

    print(f"Récupération des vols pour le {target_day.strftime('%d/%m/%Y')} (consulté à {checked_at:%H:%M})...")

    arrivals_raw = fetch_table("in", target_day)
    time.sleep(random.uniform(2, 5))
    departures_raw = fetch_table("out", target_day)

    if arrivals_raw.empty and departures_raw.empty:
        sys.exit(
            "Aucune donnée récupérée : soit la page a changé de structure, "
            "soit aucun vol n'est programmé ce jour-là."
        )

    arrivals = normalize(arrivals_raw, "in", target_day, checked_at)
    departures = normalize(departures_raw, "out", target_day, checked_at)

    all_flights = pd.concat([arrivals, departures], ignore_index=True)
    all_flights = all_flights.sort_values("heure")
    records = all_flights.to_dict(orient="records")

    history_path = os.path.join(args.out_dir, "history.jsonl")
    latest_path = os.path.join(args.out_dir, "latest.json")

    append_history(records, history_path)
    write_latest(records, latest_path)
    send_webhook(records)

    print(f"{len(arrivals)} atterrissage(s), {len(departures)} décollage(s) enregistrés.")
    print(f"Historique : {history_path}")
    print(f"Dernier instantané : {latest_path}")


if __name__ == "__main__":
    main()
