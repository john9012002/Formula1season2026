import os
import fastf1
import pandas as pd
import warnings
warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────
# CẤU HÌNH
# ─────────────────────────────────────────────
YEAR        = 2026
GRAND_PRIX  = "Australia"
CACHE_DIR   = "./f1_cache"          # Thư mục cache (tránh download lại)
OUTPUT_DIR  = "./AustralianGrandPrix" # Thư mục lưu file CSV / Excel

SESSIONS = {
    "FP1": "Practice 1",
    "FP2": "Practice 2",
    "FP3": "Practice 3",
    "Q"  : "Qualifying",
    "R"  : "Race",
}

# ─────────────────────────────────────────────
# KHỞI TẠO
# ─────────────────────────────────────────────
os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)
fastf1.Cache.enable_cache(CACHE_DIR)

print("=" * 60)
print(f"  🏎️  F1 {YEAR} {GRAND_PRIX.upper()} GRAND PRIX - DATA CRAWLER")
print("=" * 60)


# ─────────────────────────────────────────────
# HÀM TIỆN ÍCH
# ─────────────────────────────────────────────

def safe_to_seconds(td_series):
    """Chuyển timedelta sang giây (float), an toàn với NaT."""
    return pd.to_timedelta(td_series, errors="coerce").dt.total_seconds()


def format_laptime(seconds):
    """Chuyển giây sang chuỗi m:ss.mmm."""
    try:
        m = int(seconds // 60)
        s = seconds % 60
        return f"{m}:{s:06.3f}"
    except Exception:
        return None


def crawl_practice(session_key: str, session_label: str):
    """Crawl dữ liệu một phiên Free Practice."""
    print(f"\n{'─'*55}")
    print(f"  📋  {session_label} ({session_key})")
    print(f"{'─'*55}")

    session = fastf1.get_session(YEAR, GRAND_PRIX, session_key)
    session.load(telemetry=False, weather=True, messages=False)

    laps = session.laps.copy()
    if laps.empty:
        print(f"  ⚠️  Không có dữ liệu lap cho {session_label}")
        return None, None

    # ── Lap data ──────────────────────────────────────────────
    lap_cols = [
        "Driver", "DriverNumber", "Team",
        "LapNumber", "LapTime",
        "Sector1Time", "Sector2Time", "Sector3Time",
        "SpeedI1", "SpeedI2", "SpeedFL", "SpeedST",
        "Compound", "TyreLife", "FreshTyre",
        "PitInTime", "PitOutTime", "IsPersonalBest",
        "TrackStatus",
    ]
    lap_cols = [c for c in lap_cols if c in laps.columns]
    laps_df = laps[lap_cols].copy()

    # Chuyển timedelta → giây
    for col in ["LapTime", "Sector1Time", "Sector2Time", "Sector3Time",
                "PitInTime", "PitOutTime"]:
        if col in laps_df.columns:
            laps_df[col + "_s"] = safe_to_seconds(laps_df[col])
            laps_df.drop(columns=[col], inplace=True)

    # ── Bảng tổng hợp fastest lap mỗi driver ──────────────────
    best = (
        laps_df
        .dropna(subset=["LapTime_s"])
        .sort_values("LapTime_s")
        .groupby("Driver", as_index=False)
        .first()
        [["Driver", "DriverNumber", "Team", "LapTime_s",
          "Sector1Time_s", "Sector2Time_s", "Sector3Time_s",
          "SpeedFL", "Compound"]]
        .sort_values("LapTime_s")
        .reset_index(drop=True)
    )
    best.index += 1
    best["LapTime"] = best["LapTime_s"].apply(format_laptime)

    # Gap với P1
    p1_time = best["LapTime_s"].iloc[0]
    best["Gap_to_P1_s"] = (best["LapTime_s"] - p1_time).round(3)

    print(best[["Driver", "Team", "LapTime", "Gap_to_P1_s",
                "Compound", "SpeedFL"]].to_string())

    # ── Weather ────────────────────────────────────────────────
    weather_df = session.weather_data.copy() if hasattr(session, "weather_data") else pd.DataFrame()

    # ── Lưu file ──────────────────────────────────────────────
    prefix = f"{session_key.lower()}_{session_label.replace(' ', '_').lower()}"
    laps_df.to_csv(f"{OUTPUT_DIR}/{prefix}_laps.csv", index=False)
    best.to_csv(f"{OUTPUT_DIR}/{prefix}_fastest.csv", index=True)
    if not weather_df.empty:
        weather_df.to_csv(f"{OUTPUT_DIR}/{prefix}_weather.csv", index=False)

    print(f"\n  ✅  Đã lưu: {prefix}_laps.csv | {prefix}_fastest.csv")
    return laps_df, best


def crawl_qualifying():
    """Crawl dữ liệu phiên Qualifying (Q1, Q2, Q3)."""
    print(f"\n{'─'*55}")
    print(f"  🏁  QUALIFYING")
    print(f"{'─'*55}")

    session = fastf1.get_session(YEAR, GRAND_PRIX, "Q")
    session.load(telemetry=False, weather=True, messages=False)

    laps = session.laps.copy()
    results = session.results.copy() if hasattr(session, "results") else pd.DataFrame()

    # ── Fastest per driver per Q segment ──────────────────────
    q_segments = {}
    for seg in ["Q1", "Q2", "Q3"]:
        seg_laps = laps[laps["IsAccurate"] == True] if "IsAccurate" in laps.columns else laps
        if "DeletedReason" in seg_laps.columns:
            seg_laps = seg_laps[seg_laps["DeletedReason"].isna()]
        # Lọc các lap trong từng segment (dùng TrackStatus hoặc tất cả)
        q_segments[seg] = seg_laps

    # Fastest lap per driver overall
    best_q = (
        laps
        .dropna(subset=["LapTime"])
        .sort_values("LapTime")
        .groupby("Driver", as_index=False)
        .first()
        [["Driver", "DriverNumber", "Team", "LapTime",
          "Sector1Time", "Sector2Time", "Sector3Time",
          "SpeedFL", "Compound"]]
    )
    # Chuyển timedelta
    for col in ["LapTime", "Sector1Time", "Sector2Time", "Sector3Time"]:
        if col in best_q.columns:
            best_q[col + "_s"] = safe_to_seconds(best_q[col])
            best_q.drop(columns=[col], inplace=True)

    best_q = best_q.sort_values("LapTime_s").reset_index(drop=True)
    best_q.index += 1
    best_q["LapTime"] = best_q["LapTime_s"].apply(format_laptime)

    p1_time = best_q["LapTime_s"].iloc[0]
    best_q["Gap_to_Pole_s"] = (best_q["LapTime_s"] - p1_time).round(3)

    print(best_q[["Driver", "Team", "LapTime",
                  "Gap_to_Pole_s", "Compound", "SpeedFL"]].to_string())

    # ── Official results (grid positions) ─────────────────────
    if not results.empty:
        print("\n  📊  Kết quả chính thức (Starting Grid):")
        res_cols = [c for c in ["Position", "DriverNumber", "Abbreviation",
                                "FullName", "TeamName", "Q1", "Q2", "Q3",
                                "GridPosition"] if c in results.columns]
        print(results[res_cols].to_string(index=False))

    # ── Lưu file ──────────────────────────────────────────────
    # Convert timedelta columns in laps
    laps_save = laps.copy()
    for col in laps_save.select_dtypes(include=["timedelta64[ns]"]).columns:
        laps_save[col] = laps_save[col].dt.total_seconds()

    laps_save.to_csv(f"{OUTPUT_DIR}/q_qualifying_laps.csv", index=False)
    best_q.to_csv(f"{OUTPUT_DIR}/q_qualifying_fastest.csv", index=True)
    if not results.empty:
        results.to_csv(f"{OUTPUT_DIR}/q_qualifying_results.csv", index=False)

    weather_df = session.weather_data.copy() if hasattr(session, "weather_data") else pd.DataFrame()
    if not weather_df.empty:
        weather_df.to_csv(f"{OUTPUT_DIR}/q_qualifying_weather.csv", index=False)

    print(f"\n  ✅  Đã lưu: q_qualifying_*.csv")
    return best_q, results


def crawl_race():
    """Crawl dữ liệu phiên Race chính."""
    print(f"\n{'─'*55}")
    print(f"  🏆  RACE")
    print(f"{'─'*55}")

    session = fastf1.get_session(YEAR, GRAND_PRIX, "R")
    session.load(telemetry=False, weather=True, messages=False)

    laps  = session.laps.copy()
    results = session.results.copy() if hasattr(session, "results") else pd.DataFrame()

    # ── Kết quả cuộc đua ──────────────────────────────────────
    if not results.empty:
        print("\n  🏆  KẾT QUẢ CUỘC ĐUA:")
        res_cols = [c for c in [
            "Position", "ClassifiedPosition", "DriverNumber",
            "Abbreviation", "FullName", "TeamName",
            "GridPosition", "Status", "Points",
            "Time", "FastestLap", "FastestLapTime"
        ] if c in results.columns]
        print(results[res_cols].to_string(index=False))

    # ── Lap data ──────────────────────────────────────────────
    lap_cols = [
        "Driver", "DriverNumber", "Team",
        "LapNumber", "LapTime",
        "Sector1Time", "Sector2Time", "Sector3Time",
        "SpeedI1", "SpeedI2", "SpeedFL", "SpeedST",
        "Compound", "TyreLife", "FreshTyre",
        "PitInTime", "PitOutTime",
        "Position", "IsPersonalBest", "TrackStatus",
    ]
    lap_cols = [c for c in lap_cols if c in laps.columns]
    laps_df  = laps[lap_cols].copy()

    for col in laps_df.select_dtypes(include=["timedelta64[ns]"]).columns:
        laps_df[col] = laps_df[col].dt.total_seconds()

    # ── Pit stop summary ───────────────────────────────────────
    pit_laps = laps[laps["PitInTime"].notna()].copy() if "PitInTime" in laps.columns else pd.DataFrame()
    pit_summary = pd.DataFrame()
    if not pit_laps.empty:
        pit_summary = (
            pit_laps
            .groupby("Driver")
            .agg(
                Stops=("LapNumber", "count"),
                PitLaps=("LapNumber", lambda x: list(x)),
            )
            .reset_index()
        )
        print("\n  🔧  PIT STOP SUMMARY:")
        print(pit_summary.to_string(index=False))

    # ── Fastest lap per driver ─────────────────────────────────
    best_r = (
        laps_df
        .dropna(subset=["LapTime"])
        .sort_values("LapTime")
        .groupby("Driver", as_index=False)
        .first()
        [["Driver", "DriverNumber", "Team", "LapTime", "SpeedFL", "Compound"]]
        .sort_values("LapTime")
        .reset_index(drop=True)
    )
    best_r["LapTime_fmt"] = best_r["LapTime"].apply(format_laptime)
    print("\n  ⚡  FASTEST LAPS (per driver):")
    print(best_r[["Driver", "Team", "LapTime_fmt", "Compound", "SpeedFL"]].to_string(index=False))

    # ── Weather ────────────────────────────────────────────────
    weather_df = session.weather_data.copy() if hasattr(session, "weather_data") else pd.DataFrame()

    # ── Lưu file ──────────────────────────────────────────────
    laps_df.to_csv(f"{OUTPUT_DIR}/r_race_laps.csv", index=False)
    if not results.empty:
        results.to_csv(f"{OUTPUT_DIR}/r_race_results.csv", index=False)
    if not pit_summary.empty:
        pit_summary.to_csv(f"{OUTPUT_DIR}/r_race_pitstops.csv", index=False)
    best_r.to_csv(f"{OUTPUT_DIR}/r_race_fastest_laps.csv", index=False)
    if not weather_df.empty:
        weather_df.to_csv(f"{OUTPUT_DIR}/r_race_weather.csv", index=False)

    print(f"\n  ✅  Đã lưu: r_race_*.csv")
    return laps_df, results


def export_excel_summary():
    """Gom tất cả CSV thành 1 file Excel nhiều sheet."""
    print(f"\n{'─'*55}")
    print(f"  📊  XUẤT FILE EXCEL TỔNG HỢP")
    print(f"{'─'*55}")

    excel_path = f"{OUTPUT_DIR}/F1_2026_AustralianGP_Summary.xlsx"

    sheet_map = {
        "FP1_Fastest"       : "fp1_practice_1_fastest.csv",
        "FP2_Fastest"       : "fp2_practice_2_fastest.csv",
        "FP3_Fastest"       : "fp3_practice_3_fastest.csv",
        "Qualifying_Fastest": "q_qualifying_fastest.csv",
        "Qualifying_Grid"   : "q_qualifying_results.csv",
        "Race_Results"      : "r_race_results.csv",
        "Race_PitStops"     : "r_race_pitstops.csv",
        "Race_FastestLaps"  : "r_race_fastest_laps.csv",
    }

    with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
        for sheet_name, filename in sheet_map.items():
            filepath = f"{OUTPUT_DIR}/{filename}"
            if os.path.exists(filepath):
                df = pd.read_csv(filepath)
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                print(f"  ✅  Sheet: {sheet_name}")
            else:
                print(f"  ⚠️  Bỏ qua (không tìm thấy): {filename}")

    print(f"\n  📁  Excel đã lưu tại: {excel_path}")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
if __name__ == "__main__":

    # 1. Free Practice sessions
    crawl_practice("FP1", "Practice 1")
    crawl_practice("FP2", "Practice 2")
    crawl_practice("FP3", "Practice 3")

    # 2. Qualifying
    crawl_qualifying()

    # 3. Race
    crawl_race()

    # 4. Xuất Excel tổng hợp
    export_excel_summary()

    print("\n" + "=" * 60)
    print("  🎉  HOÀN THÀNH! Dữ liệu đã được lưu vào thư mục:")
    print(f"       {os.path.abspath(OUTPUT_DIR)}")
    print("=" * 60)