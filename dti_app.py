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
    try:
        return conn.read(ttl="0") 
    except:
        return pd.DataFrame(columns=["name", "base_rtc", "last_race", "course", "dist", "notes", "timestamp", "f3f", "l3f", "load"])

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
        f3f_val = 0.0; l3f_val = 0.0; pace_status = ""
        if lap_input:
            laps = [float(x) for x in re.findall(r'\d+\.\d', lap_input)]
            if len(laps) >= 3:
                f3f_val = sum(laps[:3]) # 純粋な前半600m
                l3f_val = sum(laps[-3:]) # 純粋な上がり600m
                pace_diff = f3f_val - l3f_val
                if pace_diff < -1.0: pace_status = "ハイペース"
                elif pace_diff > 1.0: pace_status = "スローペース"
                else: pace_status = "ミドルペース"
                st.info(f"🏁 前後半3F比較: {f3f_val:.1f} - {l3f_val:.1f} ({pace_status})")

    with col2: 
        raw_input = st.text_area("JRA成績表貼り付け (着順・馬名・斤量・通過順・上がり3Fを含む)")

    if st.button("🚀 解析してDBへ保存"):
        if raw_input and f3f_val > 0:
            clean_text = re.sub(r'\s+', ' ', raw_input)
            matches = list(re.finditer(r'(\d{1,2}:\d{2}\.\d)', clean_text))
            agari_list = re.findall(r'\s(\d{2}\.\d)\s', clean_text)
            pos_list = re.findall(r'\d{1,2}-\d{1,2}-\d{1,2}-\d{1,2}', clean_text) # 通過順の抽出
            
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
                
                # 上がりと位置取り
                try: indiv_l3f = float(agari_list[idx])
                except: indiv_l3f = l3f_val
                
                try: 
                    last_pos = float(pos_list[idx].split('-')[-1]) # 4角位置
                except: 
                    last_pos = 5.0

                # --- 1. スタミナ補正 ---
                stamina_penalty = (dist - 1600) * 0.0005 # 距離が伸びるほどロス耐性が減る補正
                
                # --- 2. 逆行判定ダブルチェック ---
                load_tags = []
                bonus_sec = 0.0
                
                # ペース逆行チェック (例: ハイペースで逃げ/先行)
                if pace_status == "ハイペース" and last_pos <= 4:
                    load_tags.append("ペース逆行(粘)")
                    bonus_sec -= 0.3 # 根性評価としてタイムを短縮
                elif pace_status == "スローペース" and last_pos >= 10:
                    load_tags.append("ペース逆行(追)")
                    bonus_sec -= 0.3

                # バイアス逆行チェック (例: 内有利で外を回した/外有利で内で詰まった)
                if bias_val < -0.5: # 内有利バイアス時
                    load_tags.append("バイアス逆行(外)")
                    bonus_sec -= 0.2
                elif bias_val > 0.5: # 外有利バイアス時
                    load_tags.append("バイアス逆行(内)")
                    bonus_sec -= 0.2

                # --- 3. 正確なRTC算出 (物理補正 + 展開ボーナス) ---
                avg_water = (w_4c + w_goal) / 2
                water_impact = (avg_water - 10.0) * 0.05 if t_type == "芝" else (12.0 - avg_water) * -0.10
                cush_impact = (9.5 - cush) * 0.1 if t_type == "芝" else 0
                
                rtc = indiv_time + bonus_sec + bias_val - (weight-56)*0.1 - water_impact - cush_impact + stamina_penalty
                
                new_rows.append({
                    "name": name, "base_rtc": rtc, "last_race": r_name,
                    "course": c_name, "dist": dist, "notes": "/".join(load_tags) if load_tags else pace_status,
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
                    "f3f": f3f_val, "l3f": indiv_l3f, "load": last_pos
                })
            
            if new_rows:
                existing_df = get_db_data()
                updated_df = pd.concat([existing_df, pd.DataFrame(new_rows)], ignore_index=True)
                conn.update(data=updated_df)
                st.success(f"✅ 解析完了。逆行負荷をタイムに還元しました。")

with tab3:
    st.header("🏁 レース別履歴データベース")
    df = get_db_data()
    if not df.empty:
        race_list = sorted(df['last_race'].unique())
        selected_race = st.selectbox("表示するレースを選択", race_list)
        if selected_race:
            race_df = df[df['last_race'] == selected_race].copy()
            
            # --- 一目でわかるインフォメーション ---
            avg_f3f = race_df['f3f'].iloc[0]
            avg_l3f = race_df['l3f'].iloc[0]
            p_stat = "ハイ" if (avg_f3f - avg_l3f) < -1.0 else "スロー" if (avg_f3f - avg_l3f) > 1.0 else "ミドル"
            avg_pos = race_df.iloc[:3]['load'].mean() # 上位3頭の平均4角位置
            bias_info = "前残り" if avg_pos <= 4 else "差し決着" if avg_pos >= 8 else "フラット"
            
            st.info(f"📋 【{p_stat}ペース】かつ【上位平均{avg_pos:.1f}番手（{bias_info}）】のレース性質")
            
            st.subheader("🎯 次走狙い馬 (逆行克服馬)")
            targets = race_df[race_df['notes'].str.contains("逆行", na=False)]
            if not targets.empty:
                for _, t in targets.iterrows():
                    st.write(f"🌟 **{t['name']}** - {t['notes']} (補正済み判定)")
            else: st.write("該当なし")

            st.divider()
            race_df['base_rtc'] = race_df['base_rtc'].apply(format_time)
            st.dataframe(race_df.sort_values("base_rtc"), use_container_width=True)

with tab2:
    st.header("📊 馬別履歴データベース")
    df = get_db_data()
    if not df.empty:
        search_horse = st.text_input("馬名で検索", key="search_h")
        display_df = df.copy()
        if search_horse: display_df = display_df[display_df['name'].str.contains(search_horse)]
        display_df['base_rtc'] = display_df['base_rtc'].apply(format_time)
        st.dataframe(display_df.sort_values(["name", "timestamp"], ascending=[True, False]), use_container_width=True)

with tab4:
    st.header("🎯 次走シミュレーター")
    df = get_db_data()
    if not df.empty:
        selected = st.multiselect("出走馬を選択", df['name'].unique())
        if selected:
            target_c = st.selectbox("次走の競馬場", list(COURSE_DATA.keys()))
            if st.button("🏁 シミュレーション実行"):
                results = []
                for h in selected:
                    h_data = df[df['name'] == h].iloc[-1]
                    sim_rtc = h_data['base_rtc'] + (COURSE_DATA[target_c] * (h_data['dist']/1600.0))
                    results.append({"馬名": h, "想定RTC": format_time(sim_rtc), "raw": sim_rtc})
                res_df = pd.DataFrame(results).sort_values("raw")
                st.table(res_df[["馬名", "想定RTC"]])

with tab5:
    st.header("🗑 データの削除")
    df = get_db_data()
    if not df.empty:
        delete_mode = st.radio("削除単位", ["レース単位", "馬単位"])
        if delete_mode == "レース単位":
            target_race = st.selectbox("削除対象", sorted(df['last_race'].unique()))
            if st.button("🚨 削除（ダブルチェック実行）"):
                new_df = df[df['last_race'] != target_race]
                conn.update(data=new_df)
                st.success("削除成功"); st.rerun()
        else:
            target_horse = st.selectbox("削除対象", sorted(df['name'].unique()))
            if st.button("🚨 削除（ダブルチェック実行）"):
                new_df = df[df['name'] != target_horse]
                conn.update(data=new_df)
                st.success("削除成功"); st.rerun()
