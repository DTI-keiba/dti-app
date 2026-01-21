import streamlit as st
import pandas as pd
import re
from streamlit_gsheets import GSheetsConnection
from datetime import datetime

# --- ページ設定 ---
st.set_page_config(page_title="DTI Ultimate DB", layout="wide")

# --- Google Sheets 接続 ---
conn = st.connection("gsheets", type=GSheetsConnection)

def get_db_data():
    all_cols = ["name", "base_rtc", "last_race", "course", "dist", "notes", "timestamp", "f3f", "l3f", "load"]
    try:
        df = conn.read(ttl="0")
        if df is None or df.empty:
            return pd.DataFrame(columns=all_cols)
        for col in all_cols:
            if col not in df.columns:
                df[col] = None
        return df
    except:
        return pd.DataFrame(columns=all_cols)

def format_time(seconds):
    if seconds is None or seconds <= 0: return ""
    m = int(seconds // 60)
    s = seconds % 60
    return f"{m}:{s:04.1f}"

COURSE_DATA = {
    "東京": 0.10, "中山": 0.25, "京都": 0.15, "阪神": 0.18, "中京": 0.20,
    "新潟": 0.05, "小倉": 0.30, "福島": 0.28, "札幌": 0.22, "函館": 0.25
}

# --- メイン UI ---
tab1, tab2, tab3, tab4, tab5 = st.tabs(["📝 解析・保存", "🐎 馬別履歴", "🏁 レース別履歴", "🎯 シミュレーター", "🗑 データ管理"])

with tab1:
    st.header("🚀 レース解析 & 自動保存")
    with st.sidebar:
        r_name = st.text_input("レース名")
        c_name = st.selectbox("競馬場", list(COURSE_DATA.keys()))
        t_type = st.radio("種別", ["芝", "ダート"])
        dist_options = list(range(1000, 3700, 100))
        dist = st.selectbox("距離 (m)", dist_options, index=dist_options.index(1600))
        st.divider()
        st.write("💧 馬場・バイアス")
        cush = st.number_input("クッション値", 7.0, 12.0, 9.5, step=0.1) if t_type == "芝" else 9.5
        w_4c = st.slider("含水率：4角 (%)", 0.0, 30.0, 10.0)
        w_goal = st.slider("含水率：ゴール前 (%)", 0.0, 30.0, 10.0)
        bias_val = st.slider("馬場バイアス (内有利 -1.0 ↔ 外有利 +1.0)", -1.0, 1.0, 0.0)

    col1, col2 = st.columns(2)
    with col1: 
        lap_input = st.text_area("JRAレースラップ (例: 12.5-11.0-12.0...)")
        f3f_val = 0.0; l3f_val = 0.0; pace_status = "ミドルペース"
        if lap_input:
            laps = [float(x) for x in re.findall(r'\d+\.\d', lap_input)]
            if len(laps) >= 3:
                f3f_val = sum(laps[:3])
                l3f_val = sum(laps[-3:])
                pace_diff = f3f_val - l3f_val
                if pace_diff < -1.0: pace_status = "ハイペース"
                elif pace_diff > 1.0: pace_status = "スローペース"
                st.info(f"🏁 前後半3F比較: {f3f_val:.1f} - {l3f_val:.1f} ({pace_status})")

    with col2: raw_input = st.text_area("JRA成績表貼り付け")

    if st.button("🚀 解析してDBへ保存"):
        if raw_input and f3f_val > 0:
            clean_text = re.sub(r'\s+', ' ', raw_input)
            matches = list(re.finditer(r'(\d{1,2}:\d{2}\.\d)', clean_text))
            agari_list = re.findall(r'\s(\d{2}\.\d)\s', clean_text)
            pos_list = re.findall(r'\d{1,2}-\d{1,2}-\d{1,2}-\d{1,2}', clean_text)
            
            top3_pos = []
            for i in range(min(3, len(pos_list))):
                top3_pos.append(float(pos_list[i].split('-')[-1]))
            avg_top_pos = sum(top3_pos)/len(top3_pos) if top3_pos else 5.0
            race_bias = "前残り" if avg_top_pos <= 4.0 else "差し決着" if avg_top_pos >= 8.0 else "フラット"

            new_rows = []
            for idx, m in enumerate(matches):
                time_str = m.group(1)
                before = clean_text[max(0, m.start()-100):m.start()]
                weight_m = re.search(r'(\d{2}\.\d)', before)
                name = "不明"; weight = 56.0
                if weight_m:
                    weight = float(weight_m.group(1))
                    parts = re.findall(r'([ァ-ヶー]{2,})', before[:weight_m.start()])
                    if parts: name = parts[-1]
                
                m_p, s_p = map(float, time_str.split(':'))
                indiv_time = m_p * 60 + s_p
                
                try: indiv_l3f = float(agari_list[idx])
                except: indiv_l3f = l3f_val
                try: last_pos = float(pos_list[idx].split('-')[-1])
                except: last_pos = 5.0

                stamina_penalty = (dist - 1600) * 0.0005
                load_tags = []
                bonus_sec = 0.0
                
                if pace_status == "ハイペース" and last_pos <= 4:
                    load_tags.append("ペース逆行(粘)"); bonus_sec -= 0.3
                elif pace_status == "スローペース" and last_pos >= 10:
                    load_tags.append("ペース逆行(追)"); bonus_sec -= 0.3

                if race_bias == "前残り" and last_pos >= 8:
                    load_tags.append("バイアス逆行(差)"); bonus_sec -= 0.2
                elif race_bias == "差し決着" and last_pos <= 4:
                    load_tags.append("バイアス逆行(粘)"); bonus_sec -= 0.2
                else:
                    load_tags.append("バイアス相応")

                avg_water = (w_4c + w_goal) / 2
                water_impact = (avg_water - 10.0) * 0.05 if t_type == "芝" else (12.0 - avg_water) * -0.10
                cush_impact = (9.5 - cush) * 0.1 if t_type == "芝" else 0
                
                rtc = indiv_time + bonus_sec + bias_val - (weight-56)*0.1 - water_impact - cush_impact + stamina_penalty
                
                new_rows.append({
                    "name": name, "base_rtc": rtc, "last_race": r_name,
                    "course": c_name, "dist": dist, "notes": "/".join(load_tags),
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
                    "f3f": f3f_val, "l3f": indiv_l3f, "load": last_pos
                })
            
            if new_rows:
                existing_df = get_db_data()
                updated_df = pd.concat([existing_df, pd.DataFrame(new_rows)], ignore_index=True)
                conn.update(data=updated_df)
                st.success(f"✅ 解析完了。バイアス: {race_bias}")

with tab3:
    st.header("🏁 レース別履歴データベース")
    df = get_db_data()
    if not df.empty:
        race_list = sorted(df['last_race'].unique())
        selected_race = st.selectbox("表示するレースを選択", race_list)
        if selected_race:
            race_df = df[df['last_race'] == selected_race].copy()
            f3f_raw = race_df['f3f'].iloc[0] if 'f3f' in race_df.columns else 0
            l3f_raw = race_df['l3f'].iloc[0] if 'l3f' in race_df.columns else 0
            if f3f_raw and l3f_raw:
                p_stat = "ハイ" if (f3f_raw - l3f_raw) < -1.0 else "スロー" if (f3f_raw - l3f_raw) > 1.0 else "ミドル"
                avg_pos = race_df['load'].head(3).mean() if 'load' in race_df.columns else 0
                bias_info = "前残り" if avg_pos <= 4 else "差し決着" if avg_pos >= 8 else "フラット"
                st.info(f"📋 【{p_stat}ペース】かつ【上位平均{avg_pos:.1f}番手（{bias_info}）】のレース性質")
            race_df['base_rtc'] = race_df['base_rtc'].apply(format_time)
            st.dataframe(race_df.sort_values("base_rtc"), use_container_width=True)

with tab4:
    st.header("🎯 次走シミュレーター & 期待値ランキング")
    df = get_db_data()
    if not df.empty:
        selected = st.multiselect("出走予定馬を選択", df['name'].unique())
        if selected:
            target_c = st.selectbox("次走の競馬場", list(COURSE_DATA.keys()))
            if st.button("🏁 シミュレーション実行"):
                results = []
                for h in selected:
                    h_history = df[df['name'] == h].sort_values("timestamp")
                    h_latest = h_history.iloc[-1]
                    has_hard_grit = h_history['notes'].str.contains("逆行", na=False).any()
                    sim_rtc = h_latest['base_rtc'] + (COURSE_DATA[target_c] * (h_latest['dist']/1600.0))
                    results.append({"馬名": h, "想定RTC": sim_rtc, "last_f3f": h_latest['f3f'], "last_pos": h_latest['load'], "grit": has_hard_grit})

                front_runners = [r for r in results if r['last_pos'] <= 3]
                predicted_pace = "ハイペース" if len(front_runners) >= 3 else "スローペース" if len(front_runners) <= 1 else "ミドルペース"
                st.subheader(f"🔮 展開予測: 【{predicted_pace}】")

                final_list = []
                for r in results:
                    suitability = "普通"
                    if predicted_pace == "ハイペース": suitability = "✨ 展開利（差）" if r['last_pos'] >= 8 else "⚠️ 展開不利（前）"
                    elif predicted_pace == "スローペース": suitability = "✨ 展開利（前）" if r['last_pos'] <= 3 else "⚠️ 展開不利（後）"

                    status_note = suitability
                    expectancy_score = 2 # デフォルト(中)
                    expectancy_label = "中"
                    
                    if r['grit']:
                        status_note = f"{suitability} → 🛠 実績により割引不要" if "不利" in suitability else f"{suitability} (鉄板)"
                        expectancy_score = 3
                        expectancy_label = "高"
                    elif "利" in suitability:
                        expectancy_score = 3
                        expectancy_label = "高"
                    elif "不利" in suitability:
                        expectancy_score = 1
                        expectancy_label = "低"

                    final_list.append({
                        "馬名": r['馬名'], 
                        "想定タイム": format_time(r['想定RTC']), 
                        "展開適性": status_note, 
                        "期待値": expectancy_label, 
                        "score": expectancy_score,
                        "raw_rtc": r['想定RTC']
                    })

                # スコア(降順) > タイム(昇順) でランキング化
                res_df = pd.DataFrame(final_list).sort_values(by=["score", "raw_rtc"], ascending=[False, True])
                res_df.insert(0, "順位", range(1, len(res_df) + 1))
                
                st.subheader("🏆 期待値ターゲット・ランキング")
                st.table(res_df[["順位", "馬名", "想定タイム", "期待値", "展開適性"]])

# --- Tab 2, 5 はそのまま ---
with tab2:
    st.header("📊 馬別履歴データベース")
    df = get_db_data()
    if not df.empty:
        search_h = st.text_input("馬名で検索", key="search_h")
        display_df = df.copy()
        if search_h: display_df = display_df[display_df['name'].str.contains(search_h)]
        display_df['base_rtc'] = display_df['base_rtc'].apply(format_time)
        st.dataframe(display_df.sort_values(["name", "timestamp"], ascending=[True, False]), use_container_width=True)

with tab5:
    st.header("🗑 データの削除")
    df = get_db_data()
    if not df.empty:
        target_r = st.selectbox("削除対象レース", sorted(df['last_race'].unique()))
        if st.button("🚨 レース削除（実行）"):
            conn.update(data=df[df['last_race'] != target_r])
            st.success("削除完了"); st.rerun()
