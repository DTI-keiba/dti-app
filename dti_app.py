import streamlit as st
import pandas as pd
import re

def format_time(seconds):
    if seconds is None: return ""
    m = int(seconds // 60)
    s = seconds % 60
    return f"{m}:{s:04.1f}"

def calculate_pace_info(lap_text):
    laps = re.findall(r'(\d{2}\.\d)', lap_text)
    if len(laps) < 4: return 0.0, "平均ペース"
    laps_f = [float(l) for l in laps]
    front_3f = sum(laps_f[:3])
    back_3f = sum(laps_f[-3:])
    diff = front_3f - back_3f
    if diff < -0.8: cat = "超スロー"
    elif diff < -0.3: cat = "スロー"
    elif diff > 0.8: cat = "ハイペース"
    elif diff > 0.3: cat = "ミドルハイ"
    else: cat = "平均"
    return diff, cat

def calculate_ultimate_rtc(actual_sec, corner, weight, cushion, slope, bias_val, rank, pace_diff, avg_top_corner, water_4c, water_goal, track_type):
    try:
        # 基本物理補正
        dist_loss = (corner - 1) * 0.15 
        w_penalty = (weight - 56.0) * 0.2
        s_penalty = 0.2 if slope else 0.0 
        
        # 種別による馬場ロジックの切り替え
        if track_type == "芝":
            # 芝はクッション値が低い（柔らかい）ほどパワーが必要
            turf_impact = (9.5 - cushion) * 0.15
            water_impact = (water_4c + water_goal - 30.0) * 0.03 # 芝は濡れると重くなる
        else:
            # ダートはクッション値設定がないため無視
            turf_impact = 0.0
            # ダートは含水率が高いほど砂が固まり「脚抜き」が良くなってタイムが速くなる
            water_impact = (15.0 - (water_4c + water_goal) / 2) * -0.12 
        
        # 逆行判定ボーナス
        reversal_notes = []
        pace_bonus = 0.0
        if pace_diff < -0.5 and corner >= 8 and rank <= 5:
            reversal_notes.append("ペース逆行(追)")
            pace_bonus += 0.3
        elif pace_diff > 0.5 and corner <= 3 and rank <= 5:
            reversal_notes.append("ペース逆行(粘)")
            pace_bonus += 0.4

        if avg_top_corner <= 4.0 and corner >= 10 and rank <= 5:
            reversal_notes.append("バイアス逆行(外)")
            pace_bonus += 0.3
        elif avg_top_corner >= 10.0 and corner <= 3 and rank <= 5:
            reversal_notes.append("バイアス逆行(内)")
            pace_bonus += 0.4

        rtc_sec = actual_sec - dist_loss - w_penalty - s_penalty - turf_impact - water_impact + bias_val - pace_bonus
        return rtc_sec, reversal_notes
    except:
        return None, None

st.set_page_config(page_title="DTI Hybrid Analytics", layout="wide")
st.title("🚀 DTI - Hybrid Surface Analyzer")

# --- サイドバー：馬場詳細設定 ---
st.sidebar.header("🏇 レース種別")
track_type = st.sidebar.radio("トラック種別", ["芝", "ダート"])

st.sidebar.header("📝 馬場環境設定")
if track_type == "芝":
    cushion_val = st.sidebar.slider("クッション値", 7.0, 12.0, 9.5, 0.1)
else:
    st.sidebar.info("ダートは含水率を重視します")
    cushion_val = 9.5 # ダート時は定数化

water_4c = st.sidebar.slider("含水率（4角）%", 0.0, 30.0, 10.0, 0.1)
water_goal = st.sidebar.slider("含水率（ゴール前）%", 0.0, 30.0, 10.0, 0.1)
track_bias = st.sidebar.slider("馬場補正 (秒)", -1.0, 1.0, 0.0, 0.1)
slope_exists = st.sidebar.checkbox("直線の急坂あり")

col1, col2 = st.columns(2)
with col1:
    lap_data = st.text_area("レースラップ", placeholder="12.5 - 11.2...", height=100)
with col2:
    raw_data = st.text_area("JRA成績表", height=100)

if st.button("🚀 総合解析実行"):
    if raw_data and lap_data:
        p_diff, p_cat = calculate_pace_info(lap_data)
        clean_text = re.sub(r'\s+', ' ', raw_data)
        matches = list(re.finditer(r'(\d{1,2}:\d{2}\.\d)', clean_text))
        
        pre_data = []
        top_corners = []
        
        for i, m in enumerate(matches):
            time_str = m.group(1)
            before = clean_text[max(0, m.start()-100):m.start()]
            after = clean_text[m.end():min(len(clean_text), m.end()+100)]
            
            rank_m = re.search(r'\b([1-9]|1[0-8])\b', before)
            rank = int(rank_m.group(1)) if rank_m else 10
            m_p, s_p = map(float, time_str.split(':'))
            actual_sec = m_p * 60 + s_p
            
            actual_3f = 0.0
            floats_after = re.findall(r'(\d{2}\.\d)', after)
            for f in floats_after:
                if 25.0 <= float(f) <= 48.0:
                    actual_3f = float(f)
                    break
            corner = 1
            if actual_3f > 0:
                mid = after.split(str(actual_3f))[0]
                c_nums = re.findall(r'\b\d{1,2}\b', mid)
                if c_nums: corner = int(c_nums[-1])
            
            name_m = re.findall(r'([ァ-ヶー]{2,})', before)
            name = name_m[-1] if name_m else "不明"
            w_m = re.findall(r'(\d{2}\.\d)', before)
            weight = float(w_m[-1]) if w_m else 56.0
            
            if rank <= 3: top_corners.append(corner)
            pre_data.append([name, corner, weight, actual_sec, actual_3f, rank])

        avg_top = sum(top_corners) / len(top_corners) if top_corners else 5.0
        st.info(f"📊 {track_type} | 展開: {p_cat} | 上位平均: {avg_top:.1f}番手")

        results = []
        for d in pre_data:
            rtc, notes = calculate_ultimate_rtc(d[3], d[1], d[2], cushion_val, slope_exists, track_bias, d[5], p_diff, avg_top, water_4c, water_goal, track_type)
            if rtc:
                results.append({
                    "着順": d[5], "馬名": d[0], "4角": f"{d[1]}番手",
                    "実上がり": d[4] if d[4]>0 else "---", "RTC": format_time(rtc), "判定": notes, "rtc_raw": rtc
                })
        
        if results:
            df = pd.DataFrame(results).sort_values(by="rtc_raw").reset_index(drop=True)
            df.index += 1
            st.table(df.drop(columns=['rtc_raw']))
            st.success(f"✅ {track_type}専用ロジックで解析を完了しました。")
