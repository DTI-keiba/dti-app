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
    all_cols = ["name", "base_rtc", "last_race", "course", "dist", "notes", "timestamp", "f3f", "l3f", "load", "memo", "date", "cushion", "water", "result_pos", "result_pop", "next_buy_flag"]
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
            
            new_rows = []
            for idx, line in enumerate(lines):
                time_match = re.search(r'(\d{1,2}:\d{2}\.\d)', line)
                if not time_match: continue
                time_str = time_match.group(1); m_p, s_p = map(float, time_str.split(':'))
                indiv_time = m_p * 60 + s_p
                weight_match = re.search(r'(\d{2}\.\d)', line); weight = 56.0; name = "不明"
                if weight_match:
                    weight = float(weight_match.group(1))
                    parts = re.findall(r'([ァ-ヶー]{2,})', line[:weight_match.start()])
                    if parts: name = parts[-1]
                
                try: indiv_l3f = float(agari_list[idx])
                except: indiv_l3f = l3f_val
                try: last_pos = float(pos_list[idx].split('-')[-1])
                except: last_pos = 5.0

                load_tags = []; bonus_sec = 0.0; eval_parts = []
                l3f_diff = f3f_val - indiv_l3f
                if l3f_diff > 2.0: eval_parts.append("🚀 アガリ優秀")
                elif l3f_diff < -2.0: eval_parts.append("📉 失速大")
                
                auto_comment = f"【評価】{'/'.join(eval_parts) if eval_parts else 'バイアス相応'}"
                rtc = indiv_time + bonus_sec + bias_val - (weight-56)*0.1 - ((w_4c+w_goal)/2 - 10.0)*0.05 - (9.5-cush)*0.1 + (dist - 1600) * 0.0005
                
                new_rows.append({
                    "name": name, "base_rtc": rtc, "last_race": r_name, "course": c_name, "dist": dist, "notes": "/".join(load_tags),
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"), "f3f": f3f_val, "l3f": indiv_l3f, "load": last_pos, "memo": auto_comment,
                    "date": r_date.strftime("%Y-%m-%d"), "cushion": cush, "water": (w_4c+w_goal)/2, "next_buy_flag": ""
                })
            if new_rows:
                existing_df = get_db_data(); updated_df = pd.concat([existing_df, pd.DataFrame(new_rows)], ignore_index=True)
                conn.update(data=updated_df); st.success(f"✅ 解析完了")

with tab2:
    st.header("📊 馬別履歴 & 買い条件設定")
    df = get_db_data()
    if not df.empty:
        col_s1, col_s2 = st.columns([1, 1])
        with col_s1: search_h = st.text_input("馬名で検索", key="search_h")
        unique_horses = sorted(df['name'].dropna().unique())
        with col_s2: target_h = st.selectbox("条件を編集する馬を選択", ["未選択"] + unique_horses)
        if target_h != "未選択":
            h_idx = df[df['name'] == target_h].index[-1]
            current_memo = df.at[h_idx, 'memo'] if not pd.isna(df.at[h_idx, 'memo']) else ""
            current_flag = df.at[h_idx, 'next_buy_flag'] if not pd.isna(df.at[h_idx, 'next_buy_flag']) else ""
            with st.form("edit_horse_form"):
                st.write(f"🐎 {target_h} の個別設定")
                new_memo = st.text_area("メモ・評価（直線不利など映像的な内容）", value=current_memo)
                new_flag = st.text_input("次走への個別の「買い」条件", value=current_flag)
                if st.form_submit_button("設定を保存"):
                    df.at[h_idx, 'memo'] = new_memo
                    df.at[h_idx, 'next_buy_flag'] = new_flag
                    conn.update(data=df); st.success(f"{target_h} 更新完了"); st.rerun()
        display_df = df[df['name'].str.contains(search_h, na=False)] if search_h else df
        st.dataframe(display_df.sort_values("date", ascending=False), use_container_width=True)

with tab4:
    st.header("🎯 シミュレーター & 統合評価")
    df = get_db_data()
    if not df.empty:
        selected = st.multiselect("出走予定馬を選択", sorted(list(df['name'].dropna().unique())))
        if selected:
            col_cfg1, col_cfg2 = st.columns(2)
            with col_cfg1:
                target_c = st.selectbox("次走の競馬場", list(COURSE_DATA.keys()), key="sim_c")
                target_dist = st.selectbox("次走の距離 (m)", list(range(1000, 3700, 100)), index=6, key="sim_dist")
            with col_cfg2:
                current_cush = st.slider("想定クッション値", 7.0, 12.0, 9.5)
            
            if st.button("🏁 統合スコア算出"):
                results = []
                for h in selected:
                    h_history = df[df['name'] == h].sort_values("date")
                    h_latest = h_history.iloc[-1]
                    best_past = h_history[h_history['base_rtc'] == h_history['base_rtc'].min()].iloc[0]
                    
                    # 条件合致スコア
                    b_match = 1 if abs(best_past['cushion'] - current_cush) <= 0.5 else 0
                    interval = (datetime.now() - h_latest['date']).days // 7
                    rota_score = 1 if 4 <= interval <= 9 else 0
                    
                    # 次走距離に基づくシミュレーション
                    sim_rtc = h_latest['base_rtc'] + (COURSE_DATA[target_c] * (target_dist/1600.0))
                    total_score = b_match + rota_score + (1 if h_latest['next_buy_flag'] else 0)
                    grade = "S" if total_score >= 2 else "A" if total_score == 1 else "B"
                    
                    results.append({"評価": grade, "馬名": h, "想定タイム": format_time(sim_rtc), "馬場": "🔥" if b_match else "-", "手動メモ": h_latest['next_buy_flag'], "raw_rtc": sim_rtc})
                
                res_df = pd.DataFrame(results).sort_values(by=["評価", "raw_rtc"], ascending=[True, True])
                st.table(res_df[["評価", "馬名", "想定タイム", "馬場", "手動メモ"]])

with tab3:
    st.header("🏁 答え合わせ & レース別履歴")
    df = get_db_data()
    if not df.empty:
        race_list = sorted(list(df['last_race'].dropna().unique()))
        sel_race = st.selectbox("レース選択", race_list)
        if sel_race:
            race_df = df[df['last_race'] == sel_race].copy()
            with st.form("result_form"):
                for i, row in race_df.iterrows():
                    col_r1, col_r2 = st.columns(2)
                    with col_r1: race_df.at[i, 'result_pos'] = st.number_input(f"{row['name']} 着順", 0, 18, value=int(row['result_pos']) if row['result_pos'] else 0, key=f"pos_{i}")
                    with col_r2: race_df.at[i, 'result_pop'] = st.number_input(f"{row['name']} 人気", 0, 18, value=int(row['result_pop']) if row['result_pop'] else 0, key=f"pop_{i}")
                if st.form_submit_button("結果を保存"):
                    df.update(race_df); conn.update(data=df); st.success("保存完了")
            st.dataframe(race_df[["name", "base_rtc", "result_pos", "result_pop"]])

with tab5:
    st.header("📈 トレンド")
    df = get_db_data()
    if not df.empty and 'cushion' in df.columns:
        target_c = st.selectbox("競馬場", list(COURSE_DATA.keys()))
        trend_df = df[df['course'] == target_c].sort_values("date")
        if not trend_df.empty: st.line_chart(trend_df.set_index("date")[["cushion", "water"]])

with tab6:
    st.header("🗑 管理")
    df = get_db_data()
    if not df.empty:
        if st.button("💣 全削除", disabled=not st.checkbox("消去実行")):
            conn.update(data=pd.DataFrame(columns=["name", "base_rtc", "last_race", "course", "dist", "notes", "timestamp", "f3f", "l3f", "load", "memo", "date", "cushion", "water", "result_pos", "result_pop", "next_buy_flag"])); st.rerun()
