import streamlit as st
import pandas as pd
import re

# --- 基本設定 ---
st.set_page_config(page_title="DTI Pro Scout & Simulator", layout="wide")

# 過去データの保存用（ブラウザを閉じるとリセットされますが、タブを開いている間は保持されます）
if "horse_db" not in st.session_state:
    st.session_state.horse_db = {}

def format_time(seconds):
    if seconds is None: return ""
    m = int(seconds // 60)
    s = seconds % 60
    return f"{m}:{s:04.1f}"

COURSE_DATA = {
    "東京": {"curve_penalty": 0.10, "slope_bonus": 0.2, "note": "広大な直線"},
    "中山": {"curve_penalty": 0.25, "slope_bonus": 0.5, "note": "急坂と小回り"},
    "京都": {"curve_penalty": 0.15, "slope_bonus": 0.0, "note": "平坦"},
    "阪神": {"curve_penalty": 0.18, "slope_bonus": 0.4, "note": "外長く内急坂"},
    "中京": {"curve_penalty": 0.20, "slope_bonus": 0.4, "note": "スパイラル・急坂"},
    "新潟": {"curve_penalty": 0.05, "slope_bonus": 0.0, "note": "日本最大の直線"},
    "小倉": {"curve_penalty": 0.30, "slope_bonus": 0.1, "note": "超小回り"},
    "福島": {"curve_penalty": 0.28, "slope_bonus": 0.2, "note": "小回り"},
    "札幌": {"curve_penalty": 0.22, "slope_bonus": 0.0, "note": "全周洋芝"},
    "函館": {"curve_penalty": 0.25, "slope_bonus": 0.1, "note": "洋芝・高低差"}
}

def calculate_rtc_core(actual_sec, corner, weight, cushion, bias_val, rank, pace_diff, avg_top_corner, water_4c, water_goal, track_type, course_name, distance):
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
        if pace_diff < -0.5 and corner >= 8 and rank <= 5:
            reversal_notes.append("ペース逆行(追)")
            pace_bonus += 0.4
        elif pace_diff > 0.5 and corner <= 3 and rank <= 5:
            reversal_notes.append("ペース逆行(粘)")
            pace_bonus += 0.4

        rtc_sec = actual_sec - dist_loss - weight_impact - slope_impact - turf_impact - water_impact + bias_val - pace_bonus
        return rtc_sec, reversal_notes
    except:
        return None, None

# --- UIタブ ---
tab1, tab2 = st.tabs(["📝 レース解析 & 保存", "🎯 次走シミュレーター"])

with tab1:
    st.title("🚀 レース解析 & データベース保存")
    with st.sidebar:
        race_name = st.text_input("解析レース名")
        c_name = st.selectbox("競馬場", list(COURSE_DATA.keys()), key="c1")
        t_type = st.radio("種別", ["芝", "ダート"], key="t1")
        dist = st.number_input("距離", 800, 4000, 1600, 100, key="d1")
        st.divider()
        cush = st.slider("クッション値", 7.0, 12.0, 9.5, 0.1) if t_type == "芝" else 9.5
        w4 = st.slider("含水4角", 0.0, 30.0, 10.0, 0.1)
        wg = st.slider("含水ゴール", 0.0, 30.0, 10.0, 0.1)
        bias = st.slider("馬場補正", -1.0, 1.0, 0.0, 0.1)

    col1, col2 = st.columns(2)
    with col1: lap_input = st.text_area("ラップ", height=100)
    with col2: raw_input = st.text_area("JRA成績表", height=100)

    if st.button("🚀 解析して保存"):
        if raw_input and lap_input:
            laps = re.findall(r'(\d{2}\.\d)', lap_input)
            p_diff = 0.0
            if len(laps) >= 4:
                laps_f = [float(l) for l in laps]
                p_diff = (sum(laps_f[:3])/3) - (sum(laps_f[-3:])/3)

            clean_text = re.sub(r'\s+', ' ', raw_input)
            matches = list(re.finditer(r'(\d{1,2}:\d{2}\.\d)', clean_text))
            
            for m in matches:
                time_str = m.group(1)
                before = clean_text[max(0, m.start()-100):m.start()]
                after = clean_text[m.end():min(len(clean_text), m.end()+100)]
                weight_m = re.search(r'(\d{2}\.\d)', before)
                name = "不明"; weight = 56.0
                if weight_m:
                    weight = float(weight_m.group(1))
                    parts = re.findall(r'([ァ-ヶー]{2,})', before[:weight_m.start()])
                    if parts: name = parts[-1]
                
                rank_m = re.search(r'\b([1-9]|1[0-8])\b', before)
                rank = int(rank_m.group(1)) if rank_m else 10
                m_p, s_p = map(float, time_str.split(':'))
                sec = m_p * 60 + s_p
                
                # 4角位置
                actual_3f = 0.0
                f_after = re.findall(r'(\d{2}\.\d)', after)
                for f in f_after:
                    if 25.0 <= float(f) <= 48.0: actual_3f = float(f); break
                corner = 1
                if actual_3f > 0:
                    mid = after.split(str(actual_3f))[0]
                    c_nums = re.findall(r'\b\d{1,2}\b', mid)
                    if c_nums: corner = int(c_nums[-1])

                rtc, notes = calculate_rtc_core(sec, corner, weight, cush, bias, rank, p_diff, 5.0, w4, wg, t_type, c_name, dist)
                
                if rtc:
                    # 馬をデータベースに保存
                    st.session_state.horse_db[name] = {
                        "base_rtc": rtc,
                        "last_race": race_name,
                        "notes": notes
                    }
            st.success(f"✅ {len(matches)}頭の馬をデータベースに保存/更新しました！")

with tab2:
    st.title("🎯 次走シミュレーター")
    if not st.session_state.horse_db:
        st.info("まだ保存された馬がいません。まずは『レース解析』を行ってください。")
    else:
        st.write("過去に解析した馬の中から、今回の出走馬を選択してください。")
        selected_horses = st.multiselect("出走馬を選択", list(st.session_state.horse_db.keys()))
        
        col_s1, col_s2, col_s3 = st.columns(3)
        with col_s1: target_course = st.selectbox("次走競馬場", list(COURSE_DATA.keys()))
        with col_s2: target_type = st.radio("次走種別", ["芝", "ダート"], key="t2")
        with col_s3: target_dist = st.number_input("次走距離", 800, 4000, 1600, 100, key="d2")

        if st.button("🏁 シミュレーション実行"):
            if selected_horses:
                sim_results = []
                for h_name in selected_horses:
                    h_data = st.session_state.horse_db[h_name]
                    # 現在の条件でRTCを再計算（簡易シミュレーション）
                    # 前走のRTCをベースに、今回のコース・距離の物理負荷を適用
                    c_info = COURSE_DATA[target_course]
                    # コースのきつさと距離による「想定パフォーマンス」を算出
                    sim_penalty = (c_info["curve_penalty"] * (target_dist / 1600.0))
                    sim_rtc = h_data["base_rtc"] - sim_penalty
                    
                    sim_results.append({
                        "期待度順位": 0,
                        "馬名": h_name,
                        "前走RTC": format_time(h_data["base_rtc"]),
                        "今回想定RTC": format_time(sim_rtc),
                        "前走判定": " / ".join(h_data["notes"]),
                        "raw_rtc": sim_rtc
                    })
                
                sim_df = pd.DataFrame(sim_results).sort_values(by="raw_rtc").reset_index(drop=True)
                sim_df["期待度順位"] = sim_df.index + 1
                st.table(sim_df.drop(columns=["raw_rtc"]))
                st.success("🎯 想定RTCが速い順に表示しました。上位の馬が今回の条件での狙い馬です！")
