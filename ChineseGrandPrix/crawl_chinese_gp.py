"""
F1 2026 Chinese Grand Prix — Data Crawler
==========================================
Sessions: FP1, Sprint Qualifying, Sprint, Qualifying, Race

Cài đặt trước khi chạy:
    pip install fastf1 pandas openpyxl

Chạy:
    python crawl_chinese_gp.py
"""

import fastf1
import pandas as pd
import numpy as np
import os
import warnings
warnings.filterwarnings("ignore")

# ── Config ────────────────────────────────────────────────────────────────────
YEAR       = 2026
GP_NAME    = "Chinese"
OUTPUT_DIR = "./ChineseGrandPrix"

# Sprint weekend — 5 sessions
SESSIONS = [
    ("Practice 1",        "fp1"),
    ("Sprint Qualifying", "sq"),
    ("Sprint",            "sprint"),
    ("Qualifying",        "q"),
    ("Race",              "r"),
]

# ── Setup ─────────────────────────────────────────────────────────────────────
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs("./fastf1_cache", exist_ok=True)
fastf1.Cache.enable_cache("./fastf1_cache")

print("=" * 60)
print(f"  F1 {YEAR} {GP_NAME} Grand Prix — Data Crawler")
print("=" * 60)
print(f"  Output → {OUTPUT_DIR}/")
print()


# ── Helper functions ──────────────────────────────────────────────────────────

def to_seconds(td):
    """Chuyển timedelta → float seconds. Trả về NaN nếu không hợp lệ."""
    try:
        return td.total_seconds()
    except Exception:
        return np.nan


def get_fastest_laps(session, session_key):
    """
    Lấy fastest lap của mỗi driver trong session.
    Trả về DataFrame với LapTime_s, Sector times, SpeedFL, Gap_to_best.
    """
    rows = []
    laps = session.laps

    # Fastest lap toàn session (để tính gap)
    best_time = laps["LapTime"].dropna().min()
    best_s    = to_seconds(best_time)

    for driver in session.drivers:
        try:
            drv_laps   = laps.pick_drivers(driver)
            fast_lap   = drv_laps.pick_fastest()

            if fast_lap is None or fast_lap.empty:
                continue

            lt_s  = to_seconds(fast_lap["LapTime"])
            s1_s  = to_seconds(fast_lap["Sector1Time"])
            s2_s  = to_seconds(fast_lap["Sector2Time"])
            s3_s  = to_seconds(fast_lap["Sector3Time"])

            rows.append({
                "Driver"        : fast_lap["Driver"],
                "DriverNumber"  : fast_lap["DriverNumber"],
                "Team"          : fast_lap["Team"],
                "LapTime_s"     : lt_s,
                "Sector1Time_s" : s1_s,
                "Sector2Time_s" : s2_s,
                "Sector3Time_s" : s3_s,
                "SpeedFL"       : fast_lap["SpeedFL"],
                "Compound"      : fast_lap["Compound"],
                "LapTime"       : str(fast_lap["LapTime"]),
                "Gap_to_Best_s" : round(lt_s - best_s, 3) if (lt_s and best_s) else np.nan,
            })
        except Exception:
            continue

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values("LapTime_s").reset_index(drop=True)
    return df


def get_laps(session, session_key):
    """
    Lấy toàn bộ lap data của session.
    Bao gồm: sector times, speed traps, compound, tyre life, pit info.
    """
    laps = session.laps.copy()
    if laps.empty:
        return pd.DataFrame()

    rows = []
    for _, lap in laps.iterrows():
        rows.append({
            "Driver"         : lap.get("Driver"),
            "DriverNumber"   : lap.get("DriverNumber"),
            "Team"           : lap.get("Team"),
            "LapNumber"      : lap.get("LapNumber"),
            "LapTime_s"      : to_seconds(lap.get("LapTime")),
            "Sector1Time_s"  : to_seconds(lap.get("Sector1Time")),
            "Sector2Time_s"  : to_seconds(lap.get("Sector2Time")),
            "Sector3Time_s"  : to_seconds(lap.get("Sector3Time")),
            "SpeedI1"        : lap.get("SpeedI1"),
            "SpeedI2"        : lap.get("SpeedI2"),
            "SpeedFL"        : lap.get("SpeedFL"),
            "SpeedST"        : lap.get("SpeedST"),
            "Compound"       : lap.get("Compound"),
            "TyreLife"       : lap.get("TyreLife"),
            "FreshTyre"      : lap.get("FreshTyre"),
            "IsPersonalBest" : lap.get("IsPersonalBest"),
            "TrackStatus"    : lap.get("TrackStatus"),
            "PitInTime_s"    : to_seconds(lap.get("PitInTime")),
            "PitOutTime_s"   : to_seconds(lap.get("PitOutTime")),
        })

    df = pd.DataFrame(rows)
    return df


def get_results(session):
    """
    Lấy kết quả chính thức của session (Position, GridPosition, Status...).
    """
    try:
        results = session.results.copy()
        if results is None or results.empty:
            return pd.DataFrame()
        return results.reset_index(drop=True)
    except Exception:
        return pd.DataFrame()


def get_weather(session):
    """
    Lấy weather data của session.
    """
    try:
        weather = session.weather_data.copy()
        if weather is None or weather.empty:
            return pd.DataFrame()

        # Chuyển Time sang seconds
        if "Time" in weather.columns:
            weather["Time_s"] = weather["Time"].apply(to_seconds)

        return weather.reset_index(drop=True)
    except Exception:
        return pd.DataFrame()


def get_pitstops(session):
    """
    Tổng hợp pit stop info từ lap data.
    Trả về: Driver, Stops (số lần pit), PitLaps (danh sách lap pit).
    """
    laps = session.laps.copy()
    if laps.empty:
        return pd.DataFrame()

    pit_rows = []
    for driver in laps["Driver"].unique():
        drv_laps = laps[laps["Driver"] == driver]
        pit_laps = drv_laps[drv_laps["PitInTime"].notna()]["LapNumber"].tolist()
        if pit_laps:
            pit_rows.append({
                "Driver"  : driver,
                "Stops"   : len(pit_laps),
                "PitLaps" : pit_laps,
            })

    return pd.DataFrame(pit_rows).sort_values("Driver").reset_index(drop=True)


def get_fastest_race_laps(session):
    """
    Fastest race lap của mỗi driver (dùng cho race session).
    """
    laps = session.laps.copy()
    if laps.empty:
        return pd.DataFrame()

    rows = []
    for driver in laps["Driver"].unique():
        drv_laps   = laps[laps["Driver"] == driver]
        valid_laps = drv_laps[drv_laps["LapTime"].notna()]
        if valid_laps.empty:
            continue
        fast_lap = valid_laps.loc[valid_laps["LapTime"].idxmin()]
        lt_s     = to_seconds(fast_lap["LapTime"])
        rows.append({
            "Driver"      : driver,
            "DriverNumber": fast_lap["DriverNumber"],
            "Team"        : fast_lap["Team"],
            "LapTime"     : lt_s,
            "SpeedFL"     : fast_lap["SpeedFL"],
            "Compound"    : fast_lap["Compound"],
            "LapTime_fmt" : f"{int(lt_s//60)}:{lt_s%60:06.3f}" if lt_s else None,
        })

    df = pd.DataFrame(rows).sort_values("LapTime").reset_index(drop=True)
    return df


# ── Main crawler ──────────────────────────────────────────────────────────────

summary_data = {}

for session_name, key in SESSIONS:
    print(f"{'─'*60}")
    print(f"  Loading: {session_name}")
    print(f"{'─'*60}")

    try:
        # Load session
        session = fastf1.get_session(YEAR, GP_NAME, session_name)
        session.load(telemetry=False, messages=False)
        print(f"  ✅ Session loaded: {session.event['EventName']} — {session_name}")

        saved_files = []

        # ── 1. Fastest laps ──────────────────────────────────
        # FP1, Sprint Qualifying, Sprint, Qualifying đều có fastest lap
        # Race: dùng fastest race lap riêng
        if session_name != "Race":
            df_fast = get_fastest_laps(session, key)
            if not df_fast.empty:
                path = f"{OUTPUT_DIR}/{key}_{key if key != 'sprint' else 'sprint'}_fastest.csv"
                # Đặt tên file rõ ràng hơn
                fname = f"{OUTPUT_DIR}/{key}_fastest.csv"
                df_fast.to_csv(fname)
                saved_files.append(fname)
                print(f"  📄 Fastest laps : {len(df_fast)} drivers → {fname}")
                summary_data[f"{key}_fastest"] = df_fast

        # ── 2. Lap-by-lap data ───────────────────────────────
        df_laps = get_laps(session, key)
        if not df_laps.empty:
            fname = f"{OUTPUT_DIR}/{key}_laps.csv"
            df_laps.to_csv(fname, index=False)
            saved_files.append(fname)
            print(f"  📄 Laps          : {len(df_laps)} laps → {fname}")
            summary_data[f"{key}_laps"] = df_laps

        # ── 3. Results (Qualifying & Race) ───────────────────
        if session_name in ("Sprint Qualifying", "Qualifying", "Sprint", "Race"):
            df_results = get_results(session)
            if not df_results.empty:
                fname = f"{OUTPUT_DIR}/{key}_results.csv"
                df_results.to_csv(fname, index=False)
                saved_files.append(fname)
                print(f"  📄 Results       : {len(df_results)} drivers → {fname}")
                summary_data[f"{key}_results"] = df_results

        # ── 4. Race-specific: fastest laps + pit stops ───────
        if session_name == "Race":
            # Fastest lap per driver
            df_race_fast = get_fastest_race_laps(session)
            if not df_race_fast.empty:
                fname = f"{OUTPUT_DIR}/r_fastest_laps.csv"
                df_race_fast.to_csv(fname, index=False)
                saved_files.append(fname)
                print(f"  📄 Race fastest  : {len(df_race_fast)} drivers → {fname}")
                summary_data["r_fastest"] = df_race_fast

            # Pit stops
            df_pit = get_pitstops(session)
            if not df_pit.empty:
                fname = f"{OUTPUT_DIR}/r_pitstops.csv"
                df_pit.to_csv(fname, index=False)
                saved_files.append(fname)
                print(f"  📄 Pit stops     : {len(df_pit)} drivers → {fname}")
                summary_data["r_pitstops"] = df_pit

        # ── 5. Sprint-specific: pit stops ────────────────────
        if session_name == "Sprint":
            df_sprint_pit = get_pitstops(session)
            if not df_sprint_pit.empty:
                fname = f"{OUTPUT_DIR}/sprint_pitstops.csv"
                df_sprint_pit.to_csv(fname, index=False)
                saved_files.append(fname)
                print(f"  📄 Sprint pits   : {len(df_sprint_pit)} drivers → {fname}")

        # ── 6. Weather ───────────────────────────────────────
        df_weather = get_weather(session)
        if not df_weather.empty:
            fname = f"{OUTPUT_DIR}/{key}_weather.csv"
            df_weather.to_csv(fname, index=False)
            saved_files.append(fname)
            print(f"  📄 Weather       : {len(df_weather)} rows → {fname}")
            summary_data[f"{key}_weather"] = df_weather

        print(f"  ✅ {len(saved_files)} files saved")

    except Exception as e:
        print(f"  ❌ Error loading {session_name}: {e}")
        print(f"     (Session có thể chưa diễn ra hoặc chưa có data)")

    print()


# ── Export Excel summary ──────────────────────────────────────────────────────
print("=" * 60)
print("  Exporting Excel summary...")
print("=" * 60)

excel_path = f"{OUTPUT_DIR}/F1_{YEAR}_ChineseGP_Summary.xlsx"

try:
    with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
        sheet_map = {
            "fp1_fastest"    : "FP1 Fastest",
            "sq_fastest"     : "SQ Fastest",
            "sprint_fastest" : "Sprint Fastest",
            "q_fastest"      : "Quali Fastest",
            "q_results"      : "Quali Results",
            "sprint_results" : "Sprint Results",
            "r_results"      : "Race Results",
            "r_fastest"      : "Race Fastest Laps",
            "r_pitstops"     : "Race Pit Stops",
        }
        sheets_written = 0
        for key, sheet_name in sheet_map.items():
            if key in summary_data and not summary_data[key].empty:
                summary_data[key].to_excel(writer, sheet_name=sheet_name, index=False)
                sheets_written += 1

    print(f"  ✅ Excel saved: {excel_path} ({sheets_written} sheets)")

except Exception as e:
    print(f"  ❌ Excel export error: {e}")


# ── Final summary ─────────────────────────────────────────────────────────────
print()
print("=" * 60)
print("  CRAWL COMPLETE")
print("=" * 60)

all_files = []
for root, dirs, files in os.walk(OUTPUT_DIR):
    for f in sorted(files):
        if f.endswith(".csv") or f.endswith(".xlsx"):
            fpath = os.path.join(root, f)
            size  = os.path.getsize(fpath)
            all_files.append((f, size))

print(f"\n  📁 {OUTPUT_DIR}/")
for fname, size in sorted(all_files):
    size_str = f"{size/1024:.1f} KB" if size > 1024 else f"{size} B"
    print(f"     {fname:<45s} {size_str:>10s}")

print(f"\n  Total: {len(all_files)} files")
print()