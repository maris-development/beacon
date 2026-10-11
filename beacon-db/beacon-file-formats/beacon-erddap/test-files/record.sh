#!/usr/bin/env bash
# Records the ERDDAP responses the beacon-erddap tests replay.
set -euo pipefail
S=https://coastwatch.pfeg.noaa.gov/erddap
T=erdGlobecBottle
cd "$(dirname "$0")"
curl -sf "$S/info/$T/index.json" -o tabledap_info.json
# One cruise day keeps the fixture small; adjust the window if it returns no rows.
curl -sf "$S/tabledap/$T.parquet?cruise_id,ship,cast,longitude,latitude,time,bottle_posn,temperature0&time%3E=2002-05-30T00:00:00Z&time%3C=2002-05-31T00:00:00Z" -o tabledap.parquet
# Every variable of the dataset, in info order, for SELECT * tests.
ALL=cruise_id,ship,cast,longitude,latitude,time,bottle_posn,chl_a_total,chl_a_10um,phaeo_total,phaeo_10um,sal00,sal11,temperature0,temperature1,fluor_v,xmiss_v,PO4,N_N,NO3,Si,NO2,NH4,oxygen,par
curl -sf "$S/tabledap/$T.parquet?$ALL&time%3E=2002-05-30T00:00:00Z&time%3C=2002-05-31T00:00:00Z" -o tabledap_all.parquet
# The body ERDDAP sends when a query matches nothing; -f is off to keep it.
curl -s -o no_results.txt -w '%{http_code}\n' "$S/tabledap/$T.parquet?cruise_id&time%3C=1900-01-01T00:00:00Z"
curl -s -o error_500.txt -w '%{http_code}\n' "$S/tabledap/$T.parquet?no_such_variable"
