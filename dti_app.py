import streamlit as st
import pandas as pd
import re

def format_time(seconds):
    if seconds is None: return ""
    m = int(seconds // 60)
    s = seconds % 60
    return f"{m}:{s:04.1f}"

# 競馬場物理データ
COURSE_DATA = {
    "東京": {"curve_penalty": 0.10, "slope_bonus": 0.2, "note": "広大な直線、コーナーロス中"},
    "中山": {"curve_penalty": 0.25, "slope_bonus": 0.5, "note": "急坂と小回り、コーナーロス大"},
    "京都": {"curve_penalty": 0.15, "slope_bonus": 0.0, "note": "平坦、3角の坂での加速性能重視"},
    "阪神": {"curve_penalty": 0.18, "slope_bonus": 0.4, "note": "外回りは長く、内回りは急坂"},
    "中京": {"curve_penalty": 0.20, "slope_bonus": 0.4, "note": "スパイラルカーブ、急坂あり"},
    "新潟": {"curve_penalty": 0.05, "slope_bonus": 0.0, "note": "日本最大の直線、外枠有利傾向"},
    "小倉": {"curve_penalty": 0.30, "slope_bonus": 0.1, "note": "超小回り、遠心力負荷が最大"},
    "福島": {"curve_penalty": 0.28, "slope_bonus": 0.2, "note": "小回り、スパイラルカーブ"},
    "札幌": {"curve_penalty": 0.22, "slope_bonus": 0.0, "note": "全周洋芝、コーナーきつい"},
    "函館": {"curve_penalty": 0.25, "slope_bonus": 0.1, "note": "洋芝、高低差あり"}
}

def calculate_ultimate_rtc(actual_sec, corner, weight, cushion, bias_val, rank, pace_diff, avg_top_corner, water_4c, water_goal, track_type, course_name, distance):
    try:
        c_info = COURSE_DATA[course_name]
        stamina_factor = distance / 1600.0
        dist_loss = (corner - 1) * c_info["curve_penalty"] * stamina_factor
        weight_impact = (weight - 56.0) * 0.2 * stamina_factor
        slope_impact = c_info["slope_bonus"] if rank <= 5 else 0.0
        
        if track_type == "芝":
            turf_impact = (9.5 - cushion) * 0.15
            water_impact = (water_4c + water_goal - 30.0) * 0.03
        else:
            turf_impact = 0.0
            water_impact = (15.0 - (water_4c + water_goal) / 2) * -0.12 
        
        reversal_notes = []
        pace_bonus = 0.0
        corner_bonus_val = 0.5 if c_info["curve_penalty"] >= 0.25 else 0.3

        if pace_diff < -0.5 and corner >= 8 and rank <= 5:
            reversal_notes.append("ペース逆行(追)")
            pace_bonus += corner_bonus_val
        elif pace_diff > 0.5 and corner <= 3 and rank <= 5:
            reversal_notes.append("ペース逆行(粘)")
            pace_bonus += 0.4

        if avg_top_corner <= 4.0 and corner >= 10 and rank <= 5:
            reversal_notes.append("バイアス逆行(外)")
            pace_bonus += corner_bonus_val
        elif avg_top_corner >= 10.0 and corner <= 3 and rank <= 5:
            reversal_notes.append("バイアス逆行(内)")
            pace_bonus += 0.4

        rtc_sec = actual_sec - dist_loss - weight_impact - slope_impact - turf_impact - water_impact + bias_val - pace_bonus
        return rtc_sec, reversal_notes
    except:
        return None, None

# --- UI Layout ---
st.set_page_config(page_title="DTI Multi-Scout", layout="wide")
st.title("🚀 DTI - Multi-Scout System")

with st.sidebar:
    st.header("🏁 レース基本設定")
    race_name = st.text_input("レース名", placeholder="例：中山金杯")
    course_name = st.selectbox("競馬場", list(COURSE_DATA.keys()))
    track_type = st.radio("トラック種別", ["芝", "ダート"])
    distance = st.number_input("距離 (m)", min_value=800, max_value=4000, value=1600, step=100)
    
    st.header("📝 環境パラメータ")
    cushion_val = st.slider("クッション値", 7.0, 12.0, 9.5, 0.1) if track_type == "芝" else 9.5
    water_4c = st.slider("含水率（4角）%", 0.0, 30.0, 10.0, 0.1)
    water_goal = st.slider("含水率（ゴール前）%", 0.0, 30.0, 10.0, 0.1)
    track_bias = st.slider("馬場補正 (秒)", -1.0, 1.0, 0.0, 0.1)

col1, col2 = st.columns(2)
with col1:
    lap_data = st.text_area("レースラップを入力", height=150)
with col2:
    raw_data = st.text_area("JRA成績表を貼り付け", height=150)

if st.button("🚀 全頭一斉スカウト開始"):
    if raw_data and lap_data:
        # パース処理
        laps = re.findall(r'(\d{2}\.\d)', lap_data)
        p_diff = 0.0
        if len(laps) >= 4:
            laps_f = [float(l) for l in laps]
            p_diff = (sum(laps_f[:3])/3) - (sum(laps_f[-3:])/3)

        clean_text = re.sub(r'\s+', ' ', raw_data)
        matches = list(re.finditer(r'(\d{1,2}:\d{2}\.\d)', clean_text))
        
        pre_data = []
        top_corners = []
        for m in matches:
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
        
        results = []
        pickup_horses = []
        for d in pre_data:
            rtc, notes = calculate_ultimate_rtc(d[3], d[1], d[2], cushion_val, track_bias, d[5], p_diff, avg_top, water_4c, water_goal, track_type, course_name, distance)
            if rtc:
                res = {"着順": d[5], "馬名": d[0], "4角": d[1], "RTC": format_time(rtc), "判定": " / ".join(notes) if notes else "---", "rtc_raw": rtc}
                results.append(res)
                # ピックアップ条件
                if d[5] >= 3 and notes:
                    pickup_horses.append({"馬名": d[0], "RTC": format_time(rtc), "理由": " & ".join(notes)})

        # 結果表示
        st.subheader(f"🏁 {race_name if race_name else '解析結果'}")
        df = pd.DataFrame(results).sort_values(by="rtc_raw").reset_index(drop=True)
        st.table(df.drop(columns=['rtc_raw']))

        # --- 複数穴馬ピックアップ表示 ---
        if pickup_horses:
            st.success(f"🎯 **【次走注目】{len(pickup_horses)}頭の逆行穴馬を検知しました**")
            for horse in pickup_horses:
                with st.expander(f"📌 {horse['馬名']} (RTC: {horse['RTC']})"):
                    st.write(f"**評価理由:** {horse['理由']}")
                    st.write("この馬は展開やコースの不利を物理的に克服したRTCを記録しています。次走、条件が好転すれば激走の可能性があります。")
        else:
            st.info("物理的な不利を跳ね返した穴馬は検知されませんでした。")
