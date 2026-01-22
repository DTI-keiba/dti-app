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
    all_cols = ["name", "base_rtc", "last_race", "course", "dist", "notes", "timestamp", "f3f", "l3f", "load", "memo", "date", "cushion", "water"]
    try:
        df = conn.read(ttl="0")
        if df is None or df.empty:
            return pd.DataFrame(columns=all_cols)
        for col in all_cols:
            if col not in df.columns:
                df[col] = None
        df['date'] = pd.to_datetime(df['date'])
        df = df.dropna(how='all')
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
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(["📝 解析・保存", "🐎 馬別履歴", "🏁 レース別履歴", "🎯 シミュレーター", "📈 馬場トレンド", "🗑 データ管理"])

with tab1:
    st.header("🚀 レース解析 & 自動保存")
    with st.sidebar:
        r_name = st.text_input("レース名")
        r_date = st.date_input("レース実施日", datetime.now())
        c_name = st.selectbox("競馬場", list(COURSE_DATA.keys()))
        t_type = st.radio("種別", ["芝", "ダート"])
        dist_options = list(range(1000, 3700, 100))
        dist = st.selectbox("距離 (m)", dist_options, index=dist_options.index(1600))
        st.divider()
        st.write("💧 馬場・バイアス")
        cush = st.number_input("クッション値", 7.0, 12.0, 9.5, step=0.1) if t_type == "芝" else 9.5
        w_4c = st.number_input("含水率：4角 (%)", 0.0, 50.0, 10.0, step=0.1)
        w_goal = st.number_input("含水率：ゴール前 (%)", 0.0, 50.0, 10.0, step=0.1)
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
            lines = [l.strip() for l in raw_input.split('\n') if len(l.strip()) > 20]
            agari_list = re.findall(r'\s(\d{2}\.\d)\s', raw_input)
            pos_list = re.findall(r'\d{1,2}-\d{1,2}-\d{1,2}-\d{1,2}', raw_input)
            
            top3_pos = []
            for i in range(min(3, len(pos_list))):
                top3_pos.append(float(pos_list[i].split('-')[-1]))
            avg_top_pos = sum(top3_pos)/len(top3_pos) if top3_pos else 5.0
            race_bias = "前残り" if avg_top_pos <= 4.0 else "差し決着" if avg_top_pos >= 8.0 else "フラット"

            new_rows = []
            for idx, line in enumerate(lines):
                time_match = re.search(r'(\d{1,2}:\d{2}\.\d)', line)
                if not time_match: continue
                time_str = time_match.group(1)
                m_p, s_p = map(float, time_str.split(':'))
                indiv_time = m_p * 60 + s_p
                
                weight_match = re.search(r'(\d{2}\.\d)', line)
                weight = 56.0; name = "不明"
                if weight_match:
                    weight = float(weight_match.group(1))
                    parts = re.findall(r'([ァ-ヶー]{2,})', line[:weight_match.start()])
                    if parts: name = parts[-1]
                
                try: indiv_l3f = float(agari_list[idx])
                except: indiv_l3f = l3f_val
                try: last_pos = float(pos_list[idx].split('-')[-1])
                except: last_pos = 5.0

                load_tags = []; bonus_sec = 0.0; eval_parts = []
                
                # 不利可視化ロジック（アガリ偏位評価）
                l3f_diff = f3f_val - indiv_l3f
                if l3f_diff > 2.0: eval_parts.append("🚀 アガリ優秀")
                elif l3f_diff < -2.0: eval_parts.append("📉 失速大")

                if pace_status == "ハイペース" and last_pos <= 4:
                    load_tags.append("ペース逆行(粘)"); bonus_sec -= 0.3
                    eval_parts.append("Hペース先行耐え")
                elif pace_status == "スローペース" and last_pos >= 10:
                    load_tags.append("ペース逆行(追)"); bonus_sec -= 0.3
                    eval_parts.append("Sペース後方から猛追")
                if race_bias == "前残り" and last_pos >= 8:
                    load_tags.append("バイアス逆行(差)"); bonus_sec -= 0.2
                    eval_parts.append("前残りバイアス外回し")
                elif race_bias == "差し決着" and last_pos <= 4:
                    load_tags.append("バイアス逆行(粘)"); bonus_sec -= 0.2
                    eval_parts.append("差し決着を前で粘り")
                
                auto_comment = f"【自動評価】{'/'.join(eval_parts) if eval_parts else 'バイアス相応'}"
                rtc = indiv_time + bonus_sec + bias_val - (weight-56)*0.1 - ((w_4c+w_goal)/2 - 10.0)*0.05 - (9.5-cush)*0.1 + (dist - 1600) * 0.0005
                
                new_rows.append({
                    "name": name, "base_rtc": rtc, "last_race": r_name,
                    "course": c_name, "dist": dist, "notes": "/".join(load_tags),
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
                    "f3f": f3f_val, "l3f": indiv_l3f, "load": last_pos, "memo": auto_comment,
                    "date": r_date.strftime("%Y-%m-%d"), "cushion": cush, "water": (w_4c+w_goal)/2
                })
            
            if new_rows:
                existing_df = get_db_data()
                updated_df = pd.concat([existing_df, pd.DataFrame(new_rows)], ignore_index=True)
                conn.update(data=updated_df)
                st.success(f"✅ 解析完了")

with tab5:
    st.header("📈 馬場トレンド解析")
    df = get_db_data()
    if not df.empty and 'cushion' in df.columns:
        target_c = st.selectbox("トレンドを確認する競馬場", list(COURSE_DATA.keys()), key="trend_c")
        trend_df = df[df['course'] == target_c].copy()
        if not trend_df.empty:
            trend_df = trend_df.sort_values("date")
            st.subheader(f"📊 {target_c}競馬場の馬場推移")
            st.line_chart(trend_df.set_index("date")[["cushion", "water"]])
            st.info("青線: クッション値、赤線: 平均含水率")

with tab4:
    st.header("🎯 シミュレーター & プロ分析")
    df = get_db_data()
    if not df.empty:
        valid_horses = df['name'].dropna().unique()
        selected = st.multiselect("出走予定馬を選択", sorted(list(valid_horses)))
        if selected:
            target_c = st.selectbox("次走の競馬場", list(COURSE_DATA.keys()), key="sim_c")
            current_cush = st.slider("想定クッション値", 7.0, 12.0, 9.5) # トレンド合致用
            if st.button("🏁 プロ分析実行"):
                results = []
                for h in selected:
                    h_history = df[df['name'] == h].sort_values("date")
                    h_latest = h_history.iloc[-1]
                    
                    # 1. バイアス合致アラート
                    best_past = h_history[h_history['base_rtc'] == h_history['base_rtc'].min()].iloc[0]
                    bias_match = "🔥 馬場合致" if abs(best_past['cushion'] - current_cush) <= 0.5 else ""
                    
                    # 2. ローテーション適性
                    interval_weeks = (datetime.now() - h_latest['date']).days // 7
                    rota_label = "⏳ 休み明け" if interval_weeks >= 10 else "🏃 叩き2戦目" if interval_weeks <= 4 else "通常"

                    sim_rtc = h_latest['base_rtc'] + (COURSE_DATA[target_c] * (h_latest['dist']/1600.0))
                    results.append({"馬名": h, "想定RTC": sim_rtc, "last_pos": h_latest['load'], "memo": h_latest['memo'], "アラート": bias_match, "ローテ": rota_label})
                
                final_list = []
                for r in results:
                    expectancy_score = 3 if r['アラート'] else 2
                    final_list.append({"馬名": r['馬名'], "想定タイム": format_time(r['想定RTC']), "ローテ": r['ローテ'], "合致": r['アラート'], "適正オッズ": "3.5倍以上" if r['アラート'] else "5.0倍以上", "メモ": r['memo'], "score": expectancy_score, "raw_rtc": r['想定RTC']})

                res_df = pd.DataFrame(final_list).sort_values(by=["score", "raw_rtc"], ascending=[False, True])
                st.table(res_df[["馬名", "想定タイム", "ローテ", "合致", "適正オッズ", "メモ"]])

with tab2:
    st.header("📊 馬別履歴 & 注目馬メモ")
    df = get_db_data()
    if not df.empty:
        col_s1, col_s2 = st.columns([1, 1])
        with col_s1: search_h = st.text_input("馬名で検索", key="search_h")
        display_df = df.copy()
        if search_h: display_df = display_df[display_df['name'].str.contains(search_h, na=False)]
        unique_horses = sorted(df['name'].dropna().unique())
        with col_s2: target_h = st.selectbox("メモを編集する馬を選択", ["未選択"] + unique_horses)
        if target_h != "未選択":
            current_memo = df[df['name'] == target_h]['memo'].iloc[-1] if not pd.isna(df[df['name'] == target_h]['memo'].iloc[-1]) else ""
            new_memo = st.text_area(f"【{target_h}】のメモ", value=current_memo)
            if st.button("📝 メモを保存"):
                df.loc[df['name'] == target_h, 'memo'] = new_memo
                conn.update(data=df); st.success("更新完了"); st.rerun()
        display_df['base_rtc'] = display_df['base_rtc'].apply(format_time)
        st.dataframe(display_df.sort_values(["name", "date"], ascending=[True, False]), use_container_width=True)

with tab3:
    st.header("🏁 レース別履歴データベース")
    df = get_db_data()
    if not df.empty and 'last_race' in df.columns:
        valid_races = df['last_race'].dropna().unique()
        race_list = sorted([str(x) for x in valid_races if str(x).strip() != ""])
        if race_list:
            selected_race = st.selectbox("表示するレースを選択", race_list)
            if selected_race:
                race_df = df[df['last_race'] == selected_race].copy()
                race_df['base_rtc'] = race_df['base_rtc'].apply(format_time)
                st.dataframe(race_df.sort_values("base_rtc"), use_container_width=True)

with tab6:
    st.header("🗑 データの管理・削除")
    df = get_db_data()
    if not df.empty:
        col_del1, col_del2 = st.columns(2)
        with col_del1:
            st.subheader("📍 レース単位の削除")
            valid_races = df['last_race'].dropna().unique()
            r_list = sorted([str(x) for x in valid_races if str(x).strip() != ""])
            if r_list:
                target_r = st.selectbox("削除対象レース", r_list)
                if st.button("🚨 選択したレースを削除", disabled=not st.checkbox("削除確認(単)", key="c1")):
                    conn.update(data=df[df['last_race'] != target_r]); st.rerun()
        with col_del2:
            st.subheader("⚠️ データベースの初期化")
            if st.button("💣 全削除", disabled=not st.checkbox("削除確認(全)", key="c2")):
                conn.update(data=pd.DataFrame(columns=["name", "base_rtc", "last_race", "course", "dist", "notes", "timestamp", "f3f", "l3f", "load", "memo", "date", "cushion", "water"])); st.rerun()
