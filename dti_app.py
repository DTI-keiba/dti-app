import streamlit as st
import pandas as pd
import re
import time
from streamlit_gsheets import GSheetsConnection
from datetime import datetime

# ==========================================
# ページ基本設定
# ==========================================
st.set_page_config(page_title="DTI Ultimate DB", layout="wide", initial_sidebar_state="expanded")

# --- Google Sheets 接続設定 ---
conn = st.connection("gsheets", type=GSheetsConnection)

# 🌟 API制限(429 Error)回避のためのキャッシュ設定
# ttl=300 (5分間キャッシュ)
@st.cache_data(ttl=300)
def get_db_data_cached():
    # データベースの全カラム定義
    all_cols = ["name", "base_rtc", "last_race", "course", "dist", "notes", "timestamp", "f3f", "l3f", "race_l3f", "load", "memo", "date", "cushion", "water", "result_pos", "result_pop", "next_buy_flag"]
    try:
        df = conn.read(ttl=0)
        if df is None or df.empty:
            return pd.DataFrame(columns=all_cols)
        
        # 不足しているカラムがあれば初期値Noneで補填
        for col in all_cols:
            if col not in df.columns:
                df[col] = None
        
        # データの型変換と前処理
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df['result_pos'] = pd.to_numeric(df['result_pos'], errors='coerce')
        
        # 🌟 三段階ソートロジック: 日付(新しい順) → レース名(名前順) → 着順(1着から)
        df = df.sort_values(["date", "last_race", "result_pos"], ascending=[False, True, True])
        
        # 人気順の数値変換
        df['result_pop'] = pd.to_numeric(df['result_pop'], errors='coerce')
        
        # 数値計算に使うカラムの安全な変換 (NaNは0.0で埋める)
        for c in ['f3f', 'l3f', 'race_l3f', 'load']:
            df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0.0)
            
        # 全ての行が空のデータは除外
        df = df.dropna(how='all')
        return df
    except Exception as e:
        st.error(f"【警告】スプレッドシートの読み込みに失敗しました。API制限や通信環境を確認してください。詳細: {e}")
        return pd.DataFrame(columns=all_cols)

def get_db_data():
    return get_db_data_cached()

# 🌟 API更新エラー対策のリトライ関数 (安全な書き込み処理)
def safe_update(df):
    # 保存の直前にデータの整合性を保つためソートを行う
    if all(col in df.columns for col in ['date', 'last_race', 'result_pos']):
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df['result_pos'] = pd.to_numeric(df['result_pos'], errors='coerce')
        df = df.sort_values(["date", "last_race", "result_pos"], ascending=[False, True, True])
    
    # 失敗時にリトライする (APIの429エラー対策)
    max_retries = 3
    for i in range(max_retries):
        try:
            conn.update(data=df)
            st.cache_data.clear() # キャッシュをクリアして最新を反映
            return True
        except Exception as e:
            if i < max_retries - 1:
                st.warning(f"Google Sheets接続エラー(リトライ {i+1}/3回目): 5秒後に再試行します。接続を確認してください。")
                time.sleep(5)
                continue
            else:
                st.error(f"Google Sheetsの更新に失敗しました。手動でスプレッドシートを確認してください。エラー: {e}")
                return False

# --- 表示用ヘルパー関数 ---
def format_time(seconds):
    """秒数を mm:ss.f 形式の文字列に変換"""
    if seconds is None or seconds <= 0 or pd.isna(seconds): return ""
    if isinstance(seconds, str): return seconds
    m = int(seconds // 60)
    s = seconds % 60
    return f"{m}:{s:04.1f}"

def parse_time_str(time_str):
    """mm:ss.f 形式の文字列を秒数(float)に変換"""
    try:
        if ":" in str(time_str):
            m, s = map(float, str(time_str).split(':'))
            return m * 60 + s
        return float(time_str)
    except:
        try: return float(time_str)
        except: return 0.0

# 🌟 コース・馬場係数データ
COURSE_DATA = {
    "東京": 0.10, "中山": 0.25, "京都": 0.15, "阪神": 0.18, "中京": 0.20,
    "新潟": 0.05, "小倉": 0.30, "福島": 0.28, "札幌": 0.22, "函館": 0.25
}
DIRT_COURSE_DATA = {
    "東京": 0.40, "中山": 0.55, "京都": 0.45, "阪神": 0.48, "中京": 0.50,
    "新潟": 0.42, "小倉": 0.58, "福島": 0.60, "札幌": 0.62, "函館": 0.65
}
SLOPE_FACTORS = {
    "中山": 0.005, "中京": 0.004, "京都": 0.002, "阪神": 0.004, "東京": 0.003,
    "新潟": 0.001, "小倉": 0.002, "福島": 0.003, "札幌": 0.001, "函館": 0.002
}

# ==========================================
# メイン UI 構成
# ==========================================
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "📝 解析・保存", "🐎 馬別履歴", "🏁 レース別履歴", 
    "🎯 シミュレーター", "📈 馬場トレンド", "🗑 データ管理"
])

# --- Tab 1: 解析・保存 ---
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
            st.info("現在、バイアスや展開に逆行して好走・善戦した注目馬はいません。")
    st.divider()

    st.header("🚀 レース解析 & 自動保存システム")
    with st.sidebar:
        st.title("解析条件設定")
        r_name = st.text_input("レース名 (例: 有馬記念)")
        r_date = st.date_input("レース実施日", datetime.now())
        c_name = st.selectbox("競馬場選択", list(COURSE_DATA.keys()))
        t_type = st.radio("トラック種別", ["芝", "ダート"], horizontal=True)
        dist_options = list(range(1000, 3700, 100))
        dist = st.selectbox("距離 (m)", dist_options, index=dist_options.index(1600))
        st.divider()
        st.write("💧 馬場コンディション・バイアス")
        cush = st.number_input("クッション値 (芝のみ)", 7.0, 12.0, 9.5, step=0.1) if t_type == "芝" else 9.5
        w_4c = st.number_input("含水率：4角地点 (%)", 0.0, 50.0, 10.0, step=0.1)
        w_goal = st.number_input("含水率：ゴール前地点 (%)", 0.0, 50.0, 10.0, step=0.1)
        track_index = st.number_input("馬場指数 (JRA公式または独自)", -50, 50, 0, step=1)
        bias_val = st.slider("馬場バイアス (内有利 -1.0 ↔ 外有利 +1.0)", -1.0, 1.0, 0.0, step=0.1)
        track_week = st.number_input("開催週 (例: 1, 8)", 1, 12, 1)

    col1, col2 = st.columns(2)
    with col1: 
        st.markdown("##### 🏁 レースラップ入力")
        lap_input = st.text_area("JRAレースラップ (例: 12.5-11.0-12.0...)", height=150)
        f3f_val = 0.0; l3f_val = 0.0; pace_status = "ミドルペース"; pace_diff = 0.0
        if lap_input:
            laps = [float(x) for x in re.findall(r'\d+\.\d', lap_input)]
            if len(laps) >= 3:
                f3f_val = sum(laps[:3]); l3f_val = sum(laps[-3:]); pace_diff = f3f_val - l3f_val
                # 🌟 追加：距離別ペースしきい値
                dynamic_threshold = 1.0 * (dist / 1600.0)
                if pace_diff < -dynamic_threshold: pace_status = "ハイペース"
                elif pace_diff > dynamic_threshold: pace_status = "スローペース"
                st.success(f"解析完了: 前3F {f3f_val:.1f} / 後3F {l3f_val:.1f} ({pace_status})")
        l3f_val = st.number_input("レース上がり3F (自動計算から修正可)", 0.0, 60.0, l3f_val, step=0.1)

    with col2: 
        st.markdown("##### 🐎 成績表貼り付け")
        raw_input = st.text_area("JRA公式サイトの成績表をそのまま貼り付けてください", height=250)

    if st.button("🚀 解析を実行してデータベースへ保存"):
        if not r_name or not raw_input:
            st.error("レース名と成績表は必須入力項目です。")
        elif f3f_val <= 0:
            st.error("ラップタイムが正しく入力されていません。")
        else:
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
                    if valid_positions: four_c_pos = valid_positions[-1]
                parsed_data.append({"line": line, "res_pos": res_pos, "four_c_pos": four_c_pos})
            
            # --- 🌟 【指示反映】バイアス判定ロジック ---
            top_3_entries = sorted([d for d in parsed_data if d["res_pos"] <= 3], key=lambda x: x["res_pos"])
            outliers = [d for d in top_3_entries if d["four_c_pos"] >= 10.0 or d["four_c_pos"] <= 3.0]
            
            if len(outliers) == 1:
                base_entries = [d for d in top_3_entries if d != outliers[0]]
                fourth_place = [d for d in parsed_data if d["res_pos"] == 4]
                bias_calculation_entries = base_entries + fourth_place
            else:
                bias_calculation_entries = top_3_entries
            
            avg_top_pos = sum(d["four_c_pos"] for d in bias_calculation_entries) / len(bias_calculation_entries) if bias_calculation_entries else 7.0
            bias_type = "前有利" if avg_top_pos <= 4.0 else "後有利" if avg_top_pos >= 10.0 else "フラット"
            
            # 🌟 追加：出走頭数把握
            max_runners = max([d["res_pos"] for d in parsed_data]) if parsed_data else 16

            new_rows = []
            for entry in parsed_data:
                line = entry["line"]; last_pos = entry["four_c_pos"]; result_pos = entry["res_pos"]
                time_match = re.search(r'(\d{1,2}:\d{2}\.\d)', line)
                time_str = time_match.group(1); m_p, s_p = map(float, time_str.split(':')); indiv_time = m_p * 60 + s_p
                weight_match = re.search(r'\s([4-6]\d\.\d)\s', line); weight = float(weight_match.group(1)) if weight_match else 0.0
                h_weight_match = re.search(r'(\d{3})kg', line)
                h_weight_str = f"({h_weight_match.group(1)}kg)" if h_weight_match else ""

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
                
                # 🌟 追加：負荷スコアリング（頭数相対化）
                rel_pos_factor = last_pos / max_runners
                load_score = 0.0
                if pace_status == "ハイペース" and bias_type != "前有利":
                    load_score += max(0, (0.6 - rel_pos_factor) * abs(pace_diff) * 3.0)
                elif pace_status == "スローペース" and bias_type != "後有利":
                    load_score += max(0, (rel_pos_factor - 0.4) * abs(pace_diff) * 2.0)
                
                # 逆行フラグ判定
                eval_parts = []; is_counter_target = False
                if result_pos <= 5:
                    if (bias_type == "前有利" and last_pos >= 10.0) or (bias_type == "後有利" and last_pos <= 3.0):
                        eval_parts.append("💎 ﾊﾞｲｱｽ逆行"); is_counter_target = True
                
                is_favored_combination = (pace_status == "ハイペース" and bias_type == "前有利") or (pace_status == "スローペース" and bias_type == "後有利")
                if not is_favored_combination:
                    if (pace_status == "ハイペース" and last_pos <= 3.0) or (pace_status == "スローペース" and last_pos >= 10.0 and (f3f_val - l3f_candidate) > 1.5):
                        eval_parts.append("🔥 展開逆行"); is_counter_target = True
                
                l3f_diff_vs_race = l3f_val - l3f_candidate
                if l3f_diff_vs_race >= 0.5: eval_parts.append("🚀 アガリ優秀")
                elif l3f_diff_vs_race <= -1.0: eval_parts.append("📉 失速大")
                
                # 🌟 追加：中盤ラップ解析
                m_note = "平"
                if dist > 1200:
                    m_lap = (indiv_time - f3f_val - l3f_candidate) / ((dist - 1200) / 200)
                    if m_lap >= 12.8: m_note = "緩"
                    elif m_lap <= 11.8: m_note = "締"
                else: m_note = "短"

                auto_comment = f"【{pace_status}/{bias_type}/負荷:{load_score:.1f}/{m_note}】{'/'.join(eval_parts) if eval_parts else '順境'}"
                
                # 🌟 追加：開催週補正
                week_adj = (track_week - 1) * 0.05
                rtc = (indiv_time - (weight - 56.0) * 0.1 - track_index / 10.0 - load_score / 10.0 - week_adj) + bias_val - ((w_4c+w_goal)/2 - 10.0)*0.05 - (9.5-cush)*0.1 + (dist - 1600) * 0.0005
                
                new_rows.append({
                    "name": name, "base_rtc": rtc, "last_race": r_name, "course": c_name, "dist": dist, "notes": f"{weight}kg{h_weight_str}", 
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"), "f3f": f3f_val, "l3f": l3f_candidate, "race_l3f": l3f_val, "load": last_pos, "memo": auto_comment,
                    "date": r_date.strftime("%Y-%m-%d"), "cushion": cush, "water": (w_4c+w_goal)/2, "next_buy_flag": "★逆行狙い" if is_counter_target else "", "result_pos": result_pos
                })
            
            if new_rows:
                existing_df = get_db_data(); updated_df = pd.concat([existing_df, pd.DataFrame(new_rows)], ignore_index=True)
                if safe_update(updated_df): st.success(f"✅ 解析完了！{len(new_rows)}頭のデータをDBに保存しました。"); st.rerun()

# --- Tab 2: 馬別履歴 ---
with tab2:
    st.header("📊 馬別履歴 & 買い条件設定")
    df = get_db_data()
    if not df.empty:
        col_s1, col_s2 = st.columns([1, 1])
        with col_s1: search_h = st.text_input("馬名で絞り込み検索", key="search_h")
        unique_horses = sorted([str(x) for x in df['name'].dropna().unique()])
        with col_s2: target_h = st.selectbox("個別メモ・買い条件を編集する馬を選択", ["未選択"] + unique_horses)
        if target_h != "未選択":
            h_idx = df[df['name'] == target_h].index[-1]
            with st.form("edit_horse_form"):
                new_memo = st.text_area("メモ・特記評価", value=df.at[h_idx, 'memo'] if not pd.isna(df.at[h_idx, 'memo']) else "")
                new_flag = st.text_input("次走への個別買いフラグ", value=df.at[h_idx, 'next_buy_flag'] if not pd.isna(df.at[h_idx, 'next_buy_flag']) else "")
                if st.form_submit_button("設定内容を保存"):
                    df.at[h_idx, 'memo'], df.at[h_idx, 'next_buy_flag'] = new_memo, new_flag
                    if safe_update(df): st.success(f"{target_h} の設定を更新しました"); st.rerun()
        display_df = df[df['name'].str.contains(search_h, na=False)] if search_h else df
        display_df = display_df.copy(); display_df['base_rtc'] = display_df['base_rtc'].apply(format_time)
        st.dataframe(display_df.sort_values("date", ascending=False)[["date", "name", "last_race", "base_rtc", "f3f", "l3f", "race_l3f", "load", "memo", "next_buy_flag"]], use_container_width=True)

# --- Tab 3: レース別履歴 ---
with tab3:
    st.header("🏁 答え合わせ & レース別履歴")
    df = get_db_data()
    if not df.empty:
        race_list = sorted([str(x) for x in df['last_race'].dropna().unique()])
        sel_race = st.selectbox("表示するレースを選択してください", race_list)
        if sel_race:
            race_df = df[df['last_race'] == sel_race].copy()
            with st.form("result_form"):
                st.write(f"【{sel_race}】の結果・人気を入力")
                for i, row in race_df.iterrows():
                    val_pos = int(row['result_pos']) if not pd.isna(row['result_pos']) else 0
                    val_pop = int(row['result_pop']) if not pd.isna(row['result_pop']) else 0
                    col_r1, col_r2 = st.columns(2)
                    with col_r1: race_df.at[i, 'result_pos'] = st.number_input(f"{row['name']} 着順", 0, 100, value=min(max(0, val_pos), 100), key=f"pos_{i}")
                    with col_r2: race_df.at[i, 'result_pop'] = st.number_input(f"{row['name']} 人気", 0, 100, value=min(max(0, val_pop), 100), key=f"pop_{i}")
                if st.form_submit_button("レース結果を保存"):
                    for i, row in race_df.iterrows(): df.at[i, 'result_pos'], df.at[i, 'result_pop'] = row['result_pos'], row['result_pop']
                    if safe_update(df): st.success("レースの結果をDBに保存しました。"); st.rerun()
            display_race_df = race_df.copy(); display_race_df['base_rtc'] = display_race_df['base_rtc'].apply(format_time)
            st.dataframe(display_race_df[["name", "notes", "base_rtc", "f3f", "l3f", "race_l3f", "result_pos", "result_pop"]], use_container_width=True)

# --- Tab 4: シミュレーター ---
with tab4:
    st.header("🎯 次走シミュレーター & 統合評価")
    df = get_db_data()
    if not df.empty:
        all_unique_names = sorted([str(x) for x in df['name'].dropna().unique()])
        selected = st.multiselect("出走予定馬を選択してください", options=all_unique_names)
        
        selected_pops = {}; selected_gates = {}
        if selected:
            st.markdown("##### 📝 枠番・予想人気の入力")
            pop_cols = st.columns(min(len(selected), 4))
            for i, h in enumerate(selected):
                with pop_cols[i % 4]:
                    h_last = df[df['name'] == h].iloc[-1]
                    selected_gates[h] = st.number_input(f"{h} 枠番", 1, 18, value=1, key=f"gate_{h}")
                    selected_pops[h] = st.number_input(f"{h} 人気", 1, 18, value=int(h_last['result_pop']) if not pd.isna(h_last['result_pop']) else 10, key=f"epop_{h}")

            col_cfg1, col_cfg2 = st.columns(2)
            with col_cfg1: 
                target_c = st.selectbox("次走競馬場", list(COURSE_DATA.keys()), key="sc")
                target_dist = st.selectbox("距離 (m)", list(range(1000, 3700, 100)), index=6)
                sim_type = st.radio("次走トラック種別", ["芝", "ダート"], horizontal=True)
                target_weight = st.number_input("想定斤量 (kg)", 48.0, 62.0, 56.0, step=0.5)
            with col_cfg2: 
                current_cush = st.slider("想定クッション値", 7.0, 12.0, 9.5)
                current_water = st.slider("想定含水率 (%)", 0.0, 30.0, 10.0)
            
            if st.button("🏁 シミュレーション実行"):
                results = []
                styles_count = {"逃げ": 0, "先行": 0, "差し": 0, "追込": 0}
                for h in selected:
                    h_history = df[df['name'] == h].sort_values("date")
                    last_3_runs = h_history.tail(3)
                    converted_rtcs = []
                    
                    # 🌟 脚質判定
                    avg_load_3r = last_3_runs['load'].mean()
                    if avg_load_3r <= 3.5: style = "逃げ"
                    elif avg_load_3r <= 7.0: style = "先行"
                    elif avg_load_3r <= 11.0: style = "差し"
                    else: style = "追込"
                    styles_count[style] += 1

                    # 🌟 RTC安定度
                    std_rtc = h_history['base_rtc'].std() if len(h_history) >= 3 else 0.0
                    stability_tag = "⚖️安定" if 0 < std_rtc < 0.2 else "🎢ムラ" if std_rtc > 0.4 else "-"

                    # 🌟 馬場適性
                    best_run = h_history.loc[h_history['base_rtc'].idxmin()]
                    aptitude_tag = "🎯馬場◎" if abs(best_run['cushion'] - current_cush) <= 0.5 and abs(best_run['water'] - current_water) <= 2.0 else "-"

                    for idx, row in last_3_runs.iterrows():
                        p_dist = row['dist']; p_rtc = row['base_rtc']; p_course = row['course']
                        p_load = row['load']; p_notes = str(row['notes'])
                        p_weight = 56.0; h_body_weight = 480.0
                        w_match = re.search(r'([4-6]\d\.\d)', p_notes); p_weight = float(w_match.group(1)) if w_match else 56.0
                        hb_match = re.search(r'\((\d{3})kg\)', p_notes); h_body_weight = float(hb_match.group(1)) if hb_match else 480.0
                        
                        if p_dist and p_dist > 0:
                            load_adj = (p_load - 7.0) * 0.02
                            # 🌟 斤量感応度
                            sensitivity = 0.15 if h_body_weight <= 440 else 0.08 if h_body_weight >= 500 else 0.1
                            weight_diff_adj = (target_weight - p_weight) * sensitivity
                            base_conv = (p_rtc + load_adj + weight_diff_adj) / p_dist * target_dist
                            slope_adj = (SLOPE_FACTORS.get(target_c, 0.002) - SLOPE_FACTORS.get(p_course, 0.002)) * target_dist
                            converted_rtcs.append(base_conv + slope_adj)
                    
                    avg_converted_rtc = sum(converted_rtcs) / len(converted_rtcs) if converted_rtcs else 0
                    
                    # 🌟 距離の弾力性
                    best_dist = h_history.loc[h_history['base_rtc'].idxmin(), 'dist']
                    dist_penalty = (abs(target_dist - best_dist) / 100) * 0.05
                    avg_converted_rtc += dist_penalty

                    # 🌟 RTCモメンタム
                    momentum_tag = "-"
                    if len(h_history) >= 2:
                        if h_history.iloc[-1]['base_rtc'] < h_history.iloc[-2]['base_rtc'] - 0.2: 
                            momentum_tag = "📈上昇"; avg_converted_rtc -= 0.15

                    # 🌟 レースレベル
                    last_race_name = h_history.iloc[-1]['last_race']
                    race_avg_rtc = df[df['last_race'] == last_race_name]['base_rtc'].mean()
                    overall_avg = df['base_rtc'].mean()
                    level_tag = "🔥強ﾒﾝﾂ" if race_avg_rtc < overall_avg - 0.2 else "-"

                    # 🌟 枠順シナジー
                    gate = selected_gates[h]
                    synergy_adj = -0.2 if (gate <= 4 and bias_val <= -0.5) or (gate >= 13 and bias_val >= 0.5) else 0
                    avg_converted_rtc += synergy_adj

                    h_latest = last_3_runs.iloc[-1]
                    course_bonus = -0.2 if any((h_history['course'] == target_c) & (h_history['result_pos'] <= 3)) else 0.0
                    water_adj = (current_water - 10.0) * 0.05
                    c_dict = DIRT_COURSE_DATA if sim_type == "ダート" else COURSE_DATA
                    if sim_type == "ダート": water_adj = -water_adj
                    final_rtc = (avg_converted_rtc + (c_dict[target_c] * (target_dist/1600.0)) + course_bonus + water_adj - (9.5 - current_cush) * 0.1)
                    
                    interval = (datetime.now() - h_latest['date']).days // 7
                    results.append({
                        "馬名": h, "脚質": style, "想定タイム": final_rtc, "過去の逆行履歴": " / ".join([f"{r['date'].strftime('%m/%d')}{r['last_race']}" for _, r in h_history[h_history['memo'].str.contains("💎|🔥", na=False)].iterrows()]) if not h_history[h_history['memo'].str.contains("💎|🔥", na=False)].empty else "-", 
                        "load": h_latest['load'], "適性": aptitude_tag, "安定": stability_tag, "偏差": "⤴️覚醒期待" if final_rtc < h_history['base_rtc'].min() - 0.3 else "-", 
                        "上昇": momentum_tag, "レベル": level_tag, "解析メモ": h_latest['memo'], "買いフラグ": h_latest['next_buy_flag'], 
                        "状態": "💤休み明け" if interval >= 12 else "-", "raw_rtc": final_rtc
                    })
                
                # 🌟 展開予想
                pace_pred = "ミドルペース"
                if styles_count["逃げ"] >= 2 or (styles_count["逃げ"] + styles_count["先行"]) >= len(selected) * 0.6: pace_pred = "ハイペース傾向"
                elif styles_count["逃げ"] == 0 and styles_count["先行"] <= 1: pace_pred = "スローペース傾向"
                
                res_df = pd.DataFrame(results)
                # 🌟 【指示反映】脚質・展開シナジー反映
                def apply_synergy(row):
                    adj = 0.0
                    if "ハイ" in pace_pred:
                        if row['脚質'] in ["差し", "追込"]: adj = -0.2
                        elif row['脚質'] == "逃げ": adj = 0.2
                    elif "スロー" in pace_pred:
                        if row['脚質'] in ["逃げ", "先行"]: adj = -0.2
                        elif row['脚質'] in ["差し", "追込"]: adj = 0.2
                    return row['raw_rtc'] + adj

                res_df['synergy_rtc'] = res_df.apply(apply_synergy, axis=1)
                res_df = res_df.sort_values("synergy_rtc")
                res_df['RTC順位'] = range(1, len(res_df) + 1)
                top_time = res_df.iloc[0]['raw_rtc']
                res_df['差'] = res_df['raw_rtc'] - top_time
                res_df['予想人気'] = res_df['馬名'].map(selected_pops)
                res_df['妙味スコア'] = res_df['予想人気'] - res_df['RTC順位']
                
                res_df['役割'] = "-"
                res_df.loc[res_df['RTC順位'] == 1, '役割'] = "◎"
                res_df.loc[res_df['RTC順位'] == 2, '役割'] = "〇"
                res_df.loc[res_df['RTC順位'] == 3, '役割'] = "▲"
                potential_bombs = res_df[res_df['RTC順位'] > 1].sort_values("妙味スコア", ascending=False)
                if not potential_bombs.empty: res_df.loc[res_df['馬名'] == potential_bombs.iloc[0]['馬名'], '役割'] = "★"
                
                res_df['想定タイム'] = res_df['raw_rtc'].apply(format_time)
                res_df['差'] = res_df['差'].apply(lambda x: f"+{x:.1f}" if x > 0 else "±0.0")

                st.markdown("---")
                st.subheader(f"🏁 展開予想：{pace_pred}")
                st.write(f"【構成】 逃げ:{styles_count['逃げ']} / 先行:{styles_count['先行']} / 差し:{styles_count['差し']} / 追込:{styles_count['追込']}")
                
                fav_h = res_df[res_df['役割'] == "◎"].iloc[0]['馬名'] if not res_df[res_df['役割'] == "◎"].empty else ""
                opp_h = res_df[res_df['役割'] == "〇"].iloc[0]['馬名'] if not res_df[res_df['役割'] == "〇"].empty else ""
                bomb_h = res_df[res_df['役割'] == "★"].iloc[0]['馬名'] if not res_df[res_df['役割'] == "★"].empty else ""
                
                col_rec1, col_rec2 = st.columns(2)
                with col_rec1: st.info(f"**🎯 馬連・ワイド1点勝負**\n\n◎ {fav_h} － 〇 {opp_h}")
                with col_rec2: 
                    if bomb_h: st.warning(f"**💣 妙味狙いワイド1点**\n\n◎ {fav_h} － ★ {bomb_h} (展開×妙味)")
                
                def highlight(row):
                    if row['役割'] == "★": return ['background-color: #ffe4e1; font-weight: bold'] * len(row)
                    if row['役割'] == "◎": return ['background-color: #fff700; font-weight: bold; color: black'] * len(row)
                    return [''] * len(row)
                st.table(res_df[["役割", "馬名", "脚質", "想定タイム", "差", "妙味スコア", "適性", "安定", "上昇", "レベル", "偏差", "load", "状態", "解析メモ"]].style.apply(highlight, axis=1))

# --- Tab 5: トレンド ---
with tab5:
    st.header("📈 トレンド")
    df = get_db_data()
    if not df.empty:
        target_c = st.selectbox("トレンド競馬場", list(COURSE_DATA.keys()), key="trend_c")
        trend_df = df[df['course'] == target_c].sort_values("date")
        if not trend_df.empty:
            st.subheader("💧 馬場推移")
            st.line_chart(trend_df.set_index("date")[["cushion", "water"]])
            st.subheader("🏁 直近4角平均通過")
            st.bar_chart(trend_df.groupby('last_race').agg({'load':'mean', 'date':'max'}).sort_values('date', ascending=False).head(15)['load'])

# --- Tab 6: データ管理（全機能維持） ---
with tab6:
    st.header("🗑 データベース保守 & 管理")
    df = get_db_data()

    def update_eval_tags_full(row, df_context=None):
        memo = str(row['memo']) if not pd.isna(row['memo']) else ""; buy_flag = str(row['next_buy_flag']).replace("★逆行狙い", "").strip()
        memo = re.sub(r'【.*?】', '', memo).strip("/")
        def to_f(val):
            try: return float(val) if not pd.isna(val) else 0.0
            except: return 0.0
        f3f, l3f, r_l3f, res_pos, load_pos, dist, rtc_val = map(to_f, [row['f3f'], row['l3f'], row['race_l3f'], row['result_pos'], row['load'], row['dist'], row['base_rtc']])
        m_note = "平"
        if dist > 1200 and f3f > 0:
            m_lap = (rtc_val - f3f - l3f) / ((dist - 1200) / 200)
            if m_lap >= 12.8: m_note = "緩"
            elif m_lap <= 11.8: m_note = "締"
        b_type = "フラット"; max_r = 16
        if df_context is not None and not pd.isna(row['last_race']):
            race_h = df_context[df_context['last_race'] == row['last_race']]; max_r = race_h['result_pos'].max() if not race_h.empty else 16
            top_3_r = race_h[race_h['result_pos'] <= 3].copy()
            outliers = top_3_r[(top_3_r['load'] >= 10.0) | (top_3_r['load'] <= 3.0)]
            bias_set = top_3_r if len(outliers) != 1 else pd.concat([top_3_r[top_3_r['name'] != outliers.iloc[0]['name']], race_h[race_h['result_pos'] == 4]])
            if not bias_set.empty: b_type = "前有利" if bias_set['load'].mean() <= 4.0 else "後有利" if bias_set['load'].mean() >= 10.0 else "フラット"
        p_status = "ハイペース" if "ハイ" in str(row['memo']) else "スローペース" if "スロー" in str(row['memo']) else "ミドルペース"
        p_diff = 1.5 if p_status != "ミドルペース" else 0.0; rel_p = load_pos / max_r; new_load = 0.0
        if p_status == "ハイペース" and b_type != "前有利": new_load = max(0, (0.6 - rel_p) * p_diff * 3.0)
        elif p_status == "スローペース" and b_type != "後有利": new_load = max(0, (rel_p - 0.4) * p_diff * 2.0)
        tags = []; is_c = False
        if r_l3f > 0:
            if (r_l3f - l3f) >= 0.5: tags.append("🚀 アガリ優秀")
            elif (r_l3f - l3f) <= -1.0: tags.append("📉 失速大")
        if res_pos <= 5:
            if (b_type == "前有利" and load_pos >= 10.0) or (b_type == "後有利" and load_pos <= 3.0): tags.append("💎 ﾊﾞｲｱｽ逆行"); is_c = True
            if not ((p_status == "ハイペース" and b_type == "前有利") or (p_status == "スローペース" and b_type == "後有利")):
                if (p_status == "ハイペース" and load_pos <= 3.0) or (p_status == "スローペース" and load_pos >= 10.0 and (f3f - l3f) > 1.5): tags.append("🔥 展開逆行"); is_c = True
        return (f"【{p_status}/{b_type}/負荷:{new_load:.1f}/{m_note}】" + "/".join(tags)).strip("/"), ("★逆行狙い " + buy_flag).strip() if is_c else buy_flag

    st.subheader("🗓 過去レースの開催週を一括設定")
    if not df.empty:
        race_m = df[['last_race', 'date']].drop_duplicates(subset=['last_race']).copy(); race_m['track_week'] = 1
        ed_w = st.data_editor(race_m, hide_index=True)
        if st.button("🔄 一括適用"):
            w_dict = dict(zip(ed_w['last_race'], ed_w['track_week']))
            for i, row in df.iterrows():
                if row['last_race'] in w_dict:
                    df.at[i, 'base_rtc'] = row['base_rtc'] - (w_dict[row['last_race']] - 1) * 0.05
                    m, f = update_eval_tags_full(df.iloc[i], df); df.at[i, 'memo'], df.at[i, 'next_buy_flag'] = m, f
            if safe_update(df): st.success("完了"); st.rerun()

    st.subheader("🛠️ 一括処理メニュー")
    col_adm1, col_adm2 = st.columns(2)
    with col_adm1:
        if st.button("🔄 DB再解析"):
            for i, row in df.iterrows(): m, f = update_eval_tags_full(row, df); df.at[i, 'memo'], df.at[i, 'next_buy_flag'] = m, f
            if safe_update(df): st.success("完了"); st.rerun()
    with col_adm2:
        if st.button("🧼 重複削除"):
            df = df.drop_duplicates(subset=['name', 'date', 'last_race'], keep='first')
            if safe_update(df): st.success("完了"); st.rerun()

    if not df.empty:
        st.subheader("🛠️ データ編集エディタ")
        ed_df = st.data_editor(df.copy().assign(base_rtc=df['base_rtc'].apply(format_time)).sort_values("date", ascending=False), num_rows="dynamic", use_container_width=True)
        if st.button("💾 反映"):
            save_df = ed_df.copy(); save_df['base_rtc'] = save_df['base_rtc'].apply(parse_time_str)
            if safe_update(save_df): st.success("完了"); st.rerun()
        
        st.divider(); st.subheader("❌ 削除設定"); col_d1, col_d2 = st.columns(2)
        with col_d1:
            del_race = st.selectbox("削除対象レース", ["未選択"] + sorted(df['last_race'].unique().tolist()))
            if del_race != "未選択" and st.button(f"🚨 {del_race} 削除"):
                if safe_update(df[df['last_race'] != del_race]): st.rerun()
        with col_d2:
            del_horse = st.selectbox("削除対象馬", ["未選択"] + sorted(df['name'].unique().tolist()))
            if del_horse != "未選択" and st.button(f"🚨 {del_horse} 削除"):
                if safe_update(df[df['name'] != del_horse]): st.rerun()
        st.divider(); with st.expander("☢️ リセット"):
            if st.button("🧨 完全初期化"):
                if safe_update(pd.DataFrame(columns=df.columns)): st.rerun()
