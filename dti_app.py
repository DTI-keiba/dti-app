import streamlit as st
import pandas as pd
import re
import time
from streamlit_gsheets import GSheetsConnection
from datetime import datetime

# --- ページ設定 ---
st.set_page_config(page_title="DTI Ultimate DB", layout="wide")

# --- Google Sheets 接続 ---
conn = st.connection("gsheets", type=GSheetsConnection)

# 🌟 API制限(429 Error)回避のためのキャッシュ設定
@st.cache_data(ttl=300)
def get_db_data_cached():
    all_cols = ["name", "base_rtc", "last_race", "course", "dist", "notes", "timestamp", "f3f", "l3f", "race_l3f", "load", "memo", "date", "cushion", "water", "result_pos", "result_pop", "next_buy_flag"]
    try:
        df = conn.read(ttl=0)
        if df is None or df.empty:
            return pd.DataFrame(columns=all_cols)
        for col in all_cols:
            if col not in df.columns:
                df[col] = None
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df['result_pos'] = pd.to_numeric(df['result_pos'], errors='coerce')
        df['result_pop'] = pd.to_numeric(df['result_pop'], errors='coerce')
        # 🌟 データ型を数値に安全に変換
        for c in ['f3f', 'l3f', 'race_l3f', 'load']:
            df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0.0)
        df = df.dropna(how='all')
        return df
    except:
        return pd.DataFrame(columns=all_cols)

def get_db_data():
    return get_db_data_cached()

# 🌟 API更新エラー対策のリトライ関数
def safe_update(df):
    max_retries = 3
    for i in range(max_retries):
        try:
            conn.update(data=df)
            st.cache_data.clear()
            return True
        except Exception as e:
            if i < max_retries - 1:
                time.sleep(5)
                continue
            else:
                st.error(f"Google Sheetsの更新に失敗しました: {e}")
                return False

def format_time(seconds):
    if seconds is None or seconds <= 0 or pd.isna(seconds): return ""
    if isinstance(seconds, str): return seconds
    m = int(seconds // 60)
    s = seconds % 60
    return f"{m}:{s:04.1f}"

def parse_time_str(time_str):
    try:
        if ":" in str(time_str):
            m, s = map(float, str(time_str).split(':'))
            return m * 60 + s
        return float(time_str)
    except:
        try: return float(time_str)
        except: return 0.0

COURSE_DATA = {
    "東京": 0.10, "中山": 0.25, "京都": 0.15, "阪神": 0.18, "中京": 0.20,
    "新潟": 0.05, "小倉": 0.30, "福島": 0.28, "札幌": 0.22, "函館": 0.25
}

# --- メイン UI ---
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(["📝 解析・保存", "🐎 馬別履歴", "🏁 レース別履歴", "🎯 シミュレーター", "📈 馬場トレンド", "🗑 データ管理"])

with tab1:
    df_pickup = get_db_data()
    if not df_pickup.empty:
        st.subheader("🎯 次走注目馬（逆行評価ピックアップ）")
        pickup_rows = []
        for i, row in df_pickup.iterrows():
            memo = str(row['memo'])
            b_flag = "💎" in memo
            p_flag = "🔥" in memo
            if b_flag or p_flag:
                detail = ""
                if b_flag and p_flag: detail = "【💥両方逆行】"
                elif b_flag: detail = "【💎バイアス逆行】"
                elif p_flag: detail = "【🔥ペース逆行】"
                pickup_rows.append({
                    "馬名": row['name'], "逆行タイプ": detail, "前走": row['last_race'],
                    "日付": row['date'].strftime('%Y-%m-%d') if not pd.isna(row['date']) else "", "解析メモ": memo
                })
        if pickup_rows:
            st.dataframe(pd.DataFrame(pickup_rows).sort_values("日付", ascending=False), use_container_width=True, hide_index=True)
        else:
            st.info("現在、逆行フラグの付いた注目馬はいません。")
    st.divider()

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
        track_index = st.number_input("馬場指数", -50, 50, 0, step=1)
        bias_val = st.slider("馬場バイアス (内有利 -1.0 ↔ 外有利 +1.0)", -1.0, 1.0, 0.0)

    col1, col2 = st.columns(2)
    with col1: 
        lap_input = st.text_area("JRAレースラップ (例: 12.5-11.0-12.0...)")
        f3f_val = 0.0; l3f_val = 0.0; pace_status = "ミドルペース"; pace_diff = 0.0
        if lap_input:
            laps = [float(x) for x in re.findall(r'\d+\.\d', lap_input)]
            if len(laps) >= 3:
                f3f_val = sum(laps[:3]); l3f_val = sum(laps[-3:]); pace_diff = f3f_val - l3f_val
                if pace_diff < -1.0: pace_status = "ハイペース"
                elif pace_diff > 1.0: pace_status = "スローペース"
                st.info(f"🏁 前後半3F比較: {f3f_val:.1f} - {l3f_val:.1f} ({pace_status})")
        l3f_val = st.number_input("レース上がり3F (自動計算値から修正可)", 0.0, 60.0, l3f_val, step=0.1)

    with col2: raw_input = st.text_area("JRA成績表貼り付け")

    if st.button("🚀 解析してDBへ保存"):
        if raw_input and f3f_val > 0:
            lines = [l.strip() for l in raw_input.split('\n') if len(l.strip()) > 15]
            parsed_data = []
            for line in lines:
                time_match = re.search(r'(\d{1,2}:\d{2}\.\d)', line)
                if not time_match: continue
                time_end_pos = time_match.end()
                res_pos_match = re.match(r'^(\d{1,2})', line)
                res_pos = int(res_pos_match.group(1)) if res_pos_match else 99
                after_time_str = line[time_end_pos:]
                pos_list = re.findall(r'\b([1-2]?\d)\b', after_time_str)
                four_c_pos = 7.0 
                if pos_list:
                    valid_positions = []
                    for p in pos_list:
                        if int(p) > 30 and len(valid_positions) > 0: break
                        valid_positions.append(float(p))
                    if valid_positions:
                        four_c_pos = valid_positions[-1]
                parsed_data.append({"line": line, "res_pos": res_pos, "four_c_pos": four_c_pos})
            
            top_3_pos = [d["four_c_pos"] for d in parsed_data if d["res_pos"] <= 3]
            avg_top_pos = sum(top_3_pos) / len(top_3_pos) if top_3_pos else 7.0
            bias_type = "前有利" if avg_top_pos <= 4.0 else "後有利" if avg_top_pos >= 10.0 else "フラット"
            new_rows = []
            for entry in parsed_data:
                line = entry["line"]; last_pos = entry["four_c_pos"]; result_pos = entry["res_pos"]
                time_match = re.search(r'(\d{1,2}:\d{2}\.\d)', line)
                time_str = time_match.group(1); m_p, s_p = map(float, time_str.split(':')); indiv_time = m_p * 60 + s_p
                weight_match = re.search(r'\s([4-6]\d\.\d)\s', line); weight = float(weight_match.group(1)) if weight_match else 0.0
                l3f_candidate = 0.0; l3f_match = re.search(r'(\d{2}\.\d)\s*\d{3}\(', line)
                if l3f_match: l3f_candidate = float(l3f_match.group(1))
                else:
                    decimal_finds = re.findall(r'(\d{2}\.\d)', line)
                    for d_val in decimal_finds:
                        f_val = float(d_val)
                        if 30.0 <= f_val <= 46.0 and abs(f_val - weight) > 0.5: l3f_candidate = f_val; break
                if l3f_candidate == 0.0: l3f_candidate = l3f_val 
                name = "不明"; parts = re.findall(r'([ァ-ヶー]{2,})', line)
                if parts: name = parts[0]
                load_score = 0.0
                if pace_status == "ハイペース": load_score += max(0, (10 - last_pos) * abs(pace_diff) * 0.2)
                elif pace_status == "スローペース": load_score += max(0, (last_pos - 5) * abs(pace_diff) * 0.1)
                
                eval_parts = []; is_counter_target = False
                if result_pos <= 5:
                    if (bias_type == "前有利" and last_pos >= 10.0) or (bias_type == "後有利" and last_pos <= 3.0):
                        eval_parts.append("💎 ﾊﾞｲｱｽ逆行"); is_counter_target = True
                if (pace_status == "ハイペース" and last_pos <= 3.0) or (pace_status == "スローペース" and last_pos >= 10.0 and (f3f_val - l3f_candidate) > 1.5):
                    eval_parts.append("🔥 展開逆行"); is_counter_target = True
                l3f_diff_vs_race = l3f_val - l3f_candidate
                if l3f_diff_vs_race >= 0.5: eval_parts.append("🚀 アガリ優秀")
                elif l3f_diff_vs_race <= -1.0: eval_parts.append("📉 失速大")
                    
                auto_comment = f"【{pace_status}/{bias_type}/負荷:{load_score:.1f}】{'/'.join(eval_parts) if eval_parts else '順境'}"
                weight_adj = (weight - 56.0) * 0.1
                actual_time_adj = track_index / 10.0
                load_time_adj = load_score / 10.0
                rtc = (indiv_time - weight_adj - actual_time_adj - load_time_adj) + bias_val - ((w_4c+w_goal)/2 - 10.0)*0.05 - (9.5-cush)*0.1 + (dist - 1600) * 0.0005
                new_rows.append({
                    "name": name, "base_rtc": rtc, "last_race": r_name, "course": c_name, "dist": dist, "notes": f"{weight}kg", 
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"), "f3f": f3f_val, "l3f": l3f_candidate, "race_l3f": l3f_val, "load": last_pos, "memo": auto_comment,
                    "date": r_date.strftime("%Y-%m-%d"), "cushion": cush, "water": (w_4c+w_goal)/2, "next_buy_flag": "★逆行狙い" if is_counter_target else "", "result_pos": result_pos
                })
            if new_rows:
                existing_df = get_db_data(); updated_df = pd.concat([existing_df, pd.DataFrame(new_rows)], ignore_index=True)
                if safe_update(updated_df): st.success(f"✅ 解析完了"); st.rerun()

with tab2:
    st.header("📊 馬別履歴 & 買い条件設定")
    df = get_db_data()
    if not df.empty:
        col_s1, col_s2 = st.columns([1, 1])
        with col_s1: search_h = st.text_input("馬名で検索", key="search_h")
        unique_horses = sorted([str(x) for x in df['name'].dropna().unique()])
        with col_s2: target_h = st.selectbox("条件を編集する馬を選択", ["未選択"] + unique_horses)
        if target_h != "未選択":
            h_idx = df[df['name'] == target_h].index[-1]
            with st.form("edit_horse_form"):
                new_memo = st.text_area("メモ・評価", value=df.at[h_idx, 'memo'] if not pd.isna(df.at[h_idx, 'memo']) else "")
                new_flag = st.text_input("次走への個別の「買い」条件", value=df.at[h_idx, 'next_buy_flag'] if not pd.isna(df.at[h_idx, 'next_buy_flag']) else "")
                if st.form_submit_button("設定を保存"):
                    df.at[h_idx, 'memo'], df.at[h_idx, 'next_buy_flag'] = new_memo, new_flag
                    if safe_update(df): st.success("更新完了"); st.rerun()
        display_df = df[df['name'].str.contains(search_h, na=False)] if search_h else df
        display_df = display_df.copy(); display_df['base_rtc'] = display_df['base_rtc'].apply(format_time)
        st.dataframe(display_df.sort_values("date", ascending=False)[["date", "name", "last_race", "base_rtc", "f3f", "l3f", "race_l3f", "load", "memo", "next_buy_flag"]], use_container_width=True)

with tab3:
    st.header("🏁 答え合わせ & レース別履歴")
    df = get_db_data()
    if not df.empty:
        race_list = sorted([str(x) for x in df['last_race'].dropna().unique()])
        sel_race = st.selectbox("レース選択", race_list)
        if sel_race:
            race_df = df[df['last_race'] == sel_race].copy()
            with st.form("result_form"):
                for i, row in race_df.iterrows():
                    val_pos = int(row['result_pos']) if not pd.isna(row['result_pos']) else 0
                    val_pop = int(row['result_pop']) if not pd.isna(row['result_pop']) else 0
                    col_r1, col_r2 = st.columns(2)
                    with col_r1: race_df.at[i, 'result_pos'] = st.number_input(f"{row['name']} 着順", 0, 100, value=min(max(0, val_pos), 100), key=f"pos_{i}")
                    with col_r2: race_df.at[i, 'result_pop'] = st.number_input(f"{row['name']} 人気", 0, 100, value=min(max(0, val_pop), 100), key=f"pop_{i}")
                if st.form_submit_button("結果を保存"):
                    for i, row in race_df.iterrows(): df.at[i, 'result_pos'], df.at[i, 'result_pop'] = row['result_pos'], row['result_pop']
                    if safe_update(df): st.success("保存完了"); st.rerun()
            display_race_df = race_df.copy(); display_race_df['base_rtc'] = display_race_df['base_rtc'].apply(format_time)
            st.dataframe(display_race_df[["name", "notes", "base_rtc", "f3f", "l3f", "race_l3f", "result_pos", "result_pop"]])

with tab4:
    st.header("🎯 シミュレーター & 統合評価")
    df = get_db_data()
    if not df.empty:
        selected = st.multiselect("出走予定馬を選択", sorted([str(x) for x in df['name'].dropna().unique()]))
        if selected:
            col_cfg1, col_cfg2 = st.columns(2)
            with col_cfg1: 
                target_c = st.selectbox("次走の競馬場", list(COURSE_DATA.keys()), key="sc")
                target_dist = st.selectbox("距離", list(range(1000, 3700, 100)), index=6)
            with col_cfg2: 
                current_cush = st.slider("想定クッション値", 7.0, 12.0, 9.5)
            
            if st.button("🏁 統合スコア算出"):
                results = []
                for h in selected:
                    h_history = df[df['name'] == h].sort_values("date")
                    # 🌟 1. 直近3走の平均ベース
                    last_3_runs = h_history.tail(3)
                    avg_base_rtc = last_3_runs['base_rtc'].mean()
                    h_latest = last_3_runs.iloc[-1]
                    
                    # 🌟 2. 距離換算 (前走距離 → 今回距離)
                    prev_dist = h_latest['dist']
                    if prev_dist and prev_dist > 0:
                        sim_rtc = (avg_base_rtc / prev_dist * target_dist)
                    else:
                        sim_rtc = avg_base_rtc
                    
                    # 🌟 3. コース実績加点 (同じ競馬場での好走歴があれば -0.2秒)
                    course_bonus = -0.2 if any((h_history['course'] == target_c) & (h_history['result_pos'] <= 3)) else 0.0
                    
                    # 最終的な想定タイム (コース係数 + 加点含む)
                    final_rtc = sim_rtc + (COURSE_DATA[target_c] * (target_dist/1600.0)) + course_bonus
                    
                    # 🌟 評価ロジック
                    b_match = 1 if abs(h_history[h_history['base_rtc'] == h_history['base_rtc'].min()].iloc[0]['cushion'] - current_cush) <= 0.5 else 0
                    interval = (datetime.now() - h_latest['date']).days // 7
                    rota_score = 1 if 4 <= interval <= 9 else 0
                    counter_score = 1 if "逆行" in str(h_latest['memo']) else 0
                    
                    results.append({
                        "評価": "S" if (b_match + rota_score + counter_score) >= 2 else "A" if (b_match + rota_score + counter_score) == 1 else "B",
                        "馬名": h, 
                        "想定タイム(3走平均換算)": format_time(final_rtc),
                        "前3F(最新)": h_latest['f3f'], 
                        "後3F(最新)": h_latest['l3f'], 
                        "馬場": "🔥" if b_match else "-", 
                        "実績": "⭐好走歴有" if course_bonus < 0 else "-",
                        "解析メモ": h_latest['memo'], 
                        "買いフラグ": h_latest['next_buy_flag'], 
                        "raw_rtc": final_rtc
                    })
                st.table(pd.DataFrame(results).sort_values(by=["評価", "raw_rtc"], ascending=[True, True])[["評価", "馬名", "想定タイム(3走平均換算)", "前3F(最新)", "後3F(最新)", "馬場", "実績", "解析メモ", "買いフラグ"]])

with tab5:
    st.header("📈 トレンド")
    df = get_db_data()
    if not df.empty:
        target_c = st.selectbox("競馬場", list(COURSE_DATA.keys()), key="trend_c")
        trend_df = df[df['course'] == target_c].sort_values("date")
        if not trend_df.empty: st.line_chart(trend_df.set_index("date")[["cushion", "water"]])

with tab6:
    st.header("🗑 データベース管理 & 手動修正")
    df = get_db_data()

    # 🌟 評価タグおよび next_buy_flag の再判定・更新ロジック
    def update_eval_tags_full(row):
        memo = str(row['memo']) if not pd.isna(row['memo']) else ""
        buy_flag = str(row['next_buy_flag']) if not pd.isna(row['next_buy_flag']) else ""
        
        tags = ["🚀 アガリ優秀", "📉 失速大", "🔥 展開逆行", "💎 ﾊﾞｲｱｽ逆行"]
        for t in tags: memo = memo.replace(t, "")
        memo = memo.replace("//", "/").strip("/")
        buy_flag = buy_flag.replace("★逆行狙い", "").strip()

        def to_f(val):
            try: return float(val) if not pd.isna(val) else 0.0
            except: return 0.0

        f3f = to_f(row['f3f'])
        l3f = to_f(row['l3f'])
        r_l3f = to_f(row['race_l3f'])
        res_pos = to_f(row['result_pos'])
        if res_pos == 0: res_pos = 99.0
        load_pos = to_f(row['load'])
        if load_pos == 0: load_pos = 7.0
        
        p_status = "ミドルペース"; b_type = "フラット"
        if "【" in memo and "】" in memo:
            header = memo.split("】")[0]
            if "ハイペース" in header: p_status = "ハイペース"
            elif "スローペース" in header: p_status = "スローペース"
            if "前有利" in header: b_type = "前有利"
            elif "後有利" in header: b_type = "後有利"

        new_tags = []; is_counter = False
        if r_l3f > 0:
            diff = r_l3f - l3f
            if diff >= 0.5: new_tags.append("🚀 アガリ優秀")
            elif diff <= -1.0: new_tags.append("📉 失速大")
        
        if res_pos <= 5:
            if (b_type == "前有利" and load_pos >= 10.0) or (b_type == "後有利" and load_pos <= 3.0):
                new_tags.append("💎 ﾊﾞｲｱｽ逆行"); is_counter = True
            if (p_status == "ハイペース" and load_pos <= 3.0) or (p_status == "スローペース" and load_pos >= 10.0 and (f3f - l3f) > 1.5):
                new_tags.append("🔥 展開逆行"); is_counter = True

        updated_buy_flag = ("★逆行狙い " + buy_flag).strip() if is_counter else buy_flag
        if "】" in memo:
            parts = memo.split("】")
            updated_memo = (parts[0] + "】" + "/".join(new_tags)).strip("/")
        else:
            updated_memo = "/".join(new_tags) if new_tags else "順境"
            
        return updated_memo, updated_buy_flag

    if st.button("🔄 スプレッドシート側の修正を読み込んで再解析"):
        st.cache_data.clear(); df = get_db_data()
        for i, row in df.iterrows():
            m, f = update_eval_tags_full(row)
            df.at[i, 'memo'], df.at[i, 'next_buy_flag'] = m, f
        if safe_update(df): st.success("反映完了"); st.rerun()

    if not df.empty:
        st.subheader("🛠️ データの手動修正")
        edit_display_df = df.copy(); edit_display_df['base_rtc'] = edit_display_df['base_rtc'].apply(format_time)
        edited_df = st.data_editor(edit_display_df.sort_values("date", ascending=False), num_rows="dynamic")
        if st.button("💾 修正を保存する"):
            save_df = edited_df.copy(); save_df['base_rtc'] = save_df['base_rtc'].apply(parse_time_str)
            for i, row in save_df.iterrows():
                m, f = update_eval_tags_full(row)
                save_df.at[i, 'memo'], save_df.at[i, 'next_buy_flag'] = m, f
            if safe_update(save_df): st.success("修正完了"); st.rerun()
        
        st.divider()
        st.subheader("❌ 特定データの削除")
        col_d1, col_d2 = st.columns(2)
        with col_d1:
            race_list = sorted([str(x) for x in df['last_race'].dropna().unique()])
            del_race = st.selectbox("削除するレースを選択", ["未選択"] + race_list)
            if del_race != "未選択":
                if st.button(f"🚨 「{del_race}」を完全に削除", type="primary"):
                    if safe_update(df[df['last_race'] != del_race]): st.success("削除しました"); st.rerun()
        with col_d2:
            horse_list = sorted([str(x) for x in df['name'].dropna().unique()])
            del_horse = st.selectbox("削除する馬を選択", ["未選択"] + horse_list)
            if del_horse != "未選択":
                if st.button(f"🚨 「{del_horse}」を完全に削除", type="primary"):
                    if safe_update(df[df['name'] != del_horse]): st.success("削除しました"); st.rerun()
