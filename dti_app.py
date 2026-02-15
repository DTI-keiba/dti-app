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

    col1, col2 = st.columns(2)
    with col1: 
        st.markdown("##### 🏁 レースラップ入力")
        lap_input = st.text_area("JRAレースラップ (例: 12.5-11.0-12.0...)", height=150)
        f3f_val = 0.0; l3f_val = 0.0; pace_status = "ミドルペース"; pace_diff = 0.0
        if lap_input:
            laps = [float(x) for x in re.findall(r'\d+\.\d', lap_input)]
            if len(laps) >= 3:
                f3f_val = sum(laps[:3]); l3f_val = sum(laps[-3:]); pace_diff = f3f_val - l3f_val
                if pace_diff < -1.0: pace_status = "ハイペース"
                elif pace_diff > 1.0: pace_status = "スローペース"
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
            
            # --- 🌟 【修正反映】バイアス判定ロジックの変更 ---
            top_3_entries = sorted([d for d in parsed_data if d["res_pos"] <= 3], key=lambda x: x["res_pos"])
            # 4角通過順が10番手以下 or 3番手以内の馬を抽出
            outliers = [d for d in top_3_entries if d["four_c_pos"] >= 10.0 or d["four_c_pos"] <= 3.0]
            
            if len(outliers) == 1:
                # 指示通り、該当する1頭を除き、4着の馬(res_pos=4)を加えた3頭で判定
                base_entries = [d for d in top_3_entries if d != outliers[0]]
                fourth_place = [d for d in parsed_data if d["res_pos"] == 4]
                bias_calculation_entries = base_entries + fourth_place
            else:
                # 2頭以上、または0頭の場合は現状維持（3着以内の3頭）で判定
                bias_calculation_entries = top_3_entries
            
            avg_top_pos = sum(d["four_c_pos"] for d in bias_calculation_entries) / len(bias_calculation_entries) if bias_calculation_entries else 7.0
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
                
                # 負荷計算
                load_score = 0.0
                if pace_status == "ハイペース" and bias_type != "前有利":
                    load_score += max(0, (10 - last_pos) * abs(pace_diff) * 0.2)
                elif pace_status == "スローペース" and bias_type != "後有利":
                    load_score += max(0, (last_pos - 5) * abs(pace_diff) * 0.1)
                
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
                    
                auto_comment = f"【{pace_status}/{bias_type}/負荷:{load_score:.1f}】{'/'.join(eval_parts) if eval_parts else '順境'}"
                
                # RTC計算ロジック
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
        
        if selected:
            col_cfg1, col_cfg2 = st.columns(2)
            with col_cfg1: 
                target_c = st.selectbox("次走の競馬場", list(COURSE_DATA.keys()), key="sc")
                target_dist = st.selectbox("距離 (m)", list(range(1000, 3700, 100)), index=6)
                sim_type = st.radio("次走トラック種別", ["芝", "ダート"], horizontal=True)
            with col_cfg2: 
                current_cush = st.slider("想定クッション値", 7.0, 12.0, 9.5)
                current_water = st.slider("想定含水率 (%)", 0.0, 30.0, 10.0)
            
            if st.button("🏁 シミュレーション実行"):
                results = []
                for h in selected:
                    h_history = df[df['name'] == h].sort_values("date")
                    last_3_runs = h_history.tail(3)
                    converted_rtcs = []
                    
                    for idx, row in last_3_runs.iterrows():
                        p_dist = row['dist']; p_rtc = row['base_rtc']; p_course = row['course']
                        p_load = row['load']
                        
                        if p_dist and p_dist > 0:
                            # 🌟 【修正反映】シミュレーション時のRTC計算に load(4角通過順) を組み込む
                            # 前走位置取り(load)が外/後ろ(数値大)ほどタイムロスがあるため、補正として加算調整
                            load_adj = (p_load - 7.0) * 0.02
                            base_conv = (p_rtc + load_adj) / p_dist * target_dist
                            s_from = SLOPE_FACTORS.get(p_course, 0.002); s_to = SLOPE_FACTORS.get(target_c, 0.002)
                            slope_adj = (s_to - s_from) * target_dist
                            converted_rtcs.append(base_conv + slope_adj)
                        else:
                            converted_rtcs.append(p_rtc)
                    
                    avg_converted_rtc = sum(converted_rtcs) / len(converted_rtcs) if converted_rtcs else 0
                    h_latest = last_3_runs.iloc[-1]
                    course_bonus = -0.2 if any((h_history['course'] == target_c) & (h_history['result_pos'] <= 3)) else 0.0
                    
                    water_adj = (current_water - 10.0) * 0.05
                    c_dict = DIRT_COURSE_DATA if sim_type == "ダート" else COURSE_DATA
                    if sim_type == "ダート": water_adj = -water_adj
                    
                    final_rtc = (avg_converted_rtc + (c_dict[target_c] * (target_dist/1600.0)) + course_bonus + water_adj - (9.5 - current_cush) * 0.1)
                    
                    good_runs = h_history[h_history['result_pos'] <= 3]
                    b_match = 1 if not good_runs.empty and ((abs(good_runs['cushion'] - current_cush) <= 0.5) & (abs(good_runs['water'] - current_water) <= 2.0)).any() else 0
                    interval = (datetime.now() - h_latest['date']).days // 7
                    rota_score = 1 if 4 <= interval <= 9 else 0
                    counter_score = 1 if "逆行" in str(h_latest['memo']) else 0
                    
                    sp_score = 0; sp_reasons = []
                    counter_history = [f"{i+1}走前" for i, r in enumerate(reversed(last_3_runs.to_dict('records'))) if "💎" in str(r['memo']) or "🔥" in str(r['memo'])]
                    if counter_history: sp_score += 1; sp_reasons.append(f"{'/'.join(counter_history)}逆行")
                    if not h_history.empty and not h_history[(h_history['result_pos'] == 1) & (abs(h_history['cushion'] - current_cush) <= 0.5) & (abs(h_history['water'] - current_water) <= 2.0)].empty:
                        sp_score += 1; sp_reasons.append("馬場適性◎")

                    results.append({
                        "評価ランク": "S" if (b_match + rota_score + counter_score) >= 2 else "A" if (b_match + rota_score + counter_score) == 1 else "B",
                        "馬名": h, "想定タイム": format_time(final_rtc), "load": h_latest['load'], 
                        "前3F(最新)": h_latest['f3f'], "後3F(最新)": h_latest['l3f'], "馬場": "🔥" if b_match else "-", 
                        "実績": "⭐好走歴有" if course_bonus < 0 else "-", "解析メモ": h_latest['memo'], "買いフラグ": h_latest['next_buy_flag'], 
                        "raw_rtc": final_rtc, "sp_score": sp_score, "sp_reason": f"({','.join(sp_reasons)})" if sp_reasons else ""
                    })
                
                res_df = pd.DataFrame(results)
                res_df['評価'] = res_df['評価ランク']
                s_group = res_df[res_df['評価ランク'] == "S"].copy()
                if not s_group.empty:
                    s_avg = s_group['raw_rtc'].mean()
                    res_df.loc[res_df['評価ランク'] == "S", 'sp_score'] += (res_df['raw_rtc'] <= s_avg - 0.3).astype(int)
                    top_sp = res_df[res_df['評価ランク'] == "S"].sort_values(['sp_score', 'raw_rtc'], ascending=[False, True]).head(2).index
                    res_df.loc[top_sp, '評価'] = "特S" + res_df.loc[top_sp, 'sp_reason']

                rank_map = {"特S": 0, "S": 1, "A": 2, "B": 3}
                res_df['rank_val'] = res_df['評価'].apply(lambda x: rank_map.get(x[:2], 99))
                res_df = res_df.sort_values(by=['rank_val', 'raw_rtc'])

                def highlight(row):
                    is_sp = "特S" in str(row['評価'])
                    is_high = row['評価'][:1] in ['S', 'A'] and "逆行" in str(row['買いフラグ'])
                    if is_sp: return ['background-color: #fff700; font-weight: bold'] * len(row)
                    return ['background-color: #fffdc2' if is_high else '' for _ in row]

                st.table(res_df[["評価", "馬名", "想定タイム", "load", "前3F(最新)", "後3F(最新)", "馬場", "実績", "解析メモ", "買いフラグ"]].style.apply(highlight, axis=1))

# --- Tab 5: トレンド解析 ---
with tab5:
    st.header("📈 馬場トレンド & 統計解析")
    df = get_db_data()
    if not df.empty:
        target_c = st.selectbox("トレンドを確認する競馬場を選択", list(COURSE_DATA.keys()), key="trend_c")
        trend_df = df[df['course'] == target_c].sort_values("date")
        if not trend_df.empty:
            st.subheader("💧 クッション値 & 含水率の時系列推移")
            st.line_chart(trend_df.set_index("date")[["cushion", "water"]])
            
            st.subheader("🏁 直近のレース傾向 (4角平均通過順位)")
            recent_races = trend_df.groupby('last_race').agg({'load':'mean', 'date':'max'}).sort_values('date', ascending=False).head(15)
            st.bar_chart(recent_races['load'])
            
            st.subheader("📊 直近の上がり3F（レース時計）推移")
            st.line_chart(trend_df.set_index("date")["race_l3f"])
            
            st.subheader("💎 この場での逆行狙い対象馬 履歴")
            bias_horses = trend_df[trend_df['memo'].str.contains("💎|🔥", na=False)]
            if not bias_horses.empty:
                st.dataframe(bias_horses[["date", "last_race", "name", "load", "memo", "result_pos"]].sort_values("date", ascending=False), use_container_width=True)
            else:
                st.info("この競馬場での逆行馬データはまだ蓄積されていません。")
        else:
            st.info("選択された競馬場のデータがまだ登録されていません。")

# --- Tab 6: データ管理 ---
with tab6:
    st.header("🗑 データベース保守 & 高度な管理機能")
    df = get_db_data()

    def update_eval_tags_full(row, df_context=None):
        """データの再検証用ロジック"""
        memo = str(row['memo']) if not pd.isna(row['memo']) else ""; buy_flag = str(row['next_buy_flag']) if not pd.isna(row['next_buy_flag']) else ""
        tags = ["🚀 アガリ優秀", "📉 失速大", "🔥 展開逆行", "💎 ﾊﾞｲｱｽ逆行"]
        for t in tags: memo = memo.replace(t, "")
        memo = memo.replace("//", "/").strip("/")
        buy_flag = buy_flag.replace("★逆行狙い", "").strip()

        def to_f(val):
            try: return float(val) if not pd.isna(val) else 0.0
            except: return 0.0

        f3f = to_f(row['f3f']); l3f = to_f(row['l3f']); r_l3f = to_f(row['race_l3f'])
        res_pos = to_f(row['result_pos']); load_pos = to_f(row['load'])
        if res_pos == 0: res_pos = 99.0
        if load_pos == 0: load_pos = 7.0
        
        b_type = "フラット"
        if df_context is not None and not pd.isna(row['last_race']):
            race_horses = df_context[df_context['last_race'] == row['last_race']]
            
            # 🌟 【修正反映】バイアス再判定の特異個体除外 & 4着補充
            top_3_race = race_horses[race_horses['result_pos'] <= 3].sort_values('result_pos')
            outliers = top_3_race[(top_3_race['load'].astype(float) >= 10.0) | (top_3_race['load'].astype(float) <= 3.0)]
            
            if len(outliers) == 1:
                # 該当1頭を除き、4着を加える
                base_entries = top_3_race[top_3_race['name'] != outliers.iloc[0]['name']]
                fourth_horse = race_horses[race_horses['result_pos'] == 4]
                bias_set = pd.concat([base_entries, fourth_horse])
            else:
                bias_set = top_3_race
                
            if not bias_set.empty:
                avg_top_pos = bias_set['load'].astype(float).mean()
                b_type = "前有利" if avg_top_pos <= 4.0 else "後有利" if avg_top_pos >= 10.0 else "フラット"

        p_status = "ハイペース" if "ハイペース" in memo else "スローペース" if "スローペース" in memo else "ミドルペース"
        new_tags = []; is_counter = False
        if r_l3f > 0:
            diff = r_l3f - l3f
            if diff >= 0.5: new_tags.append("🚀 アガリ優秀")
            elif diff <= -1.0: new_tags.append("📉 失速大")
        
        is_favored = (p_status == "ハイペース" and b_type == "前有利") or (p_status == "スローペース" and b_type == "後有利")
        if res_pos <= 5:
            if (b_type == "前有利" and load_pos >= 10.0) or (b_type == "後有利" and load_pos <= 3.0):
                new_tags.append("💎 ﾊﾞｲｱｽ逆行"); is_counter = True
            if not is_favored:
                if (p_status == "ハイペース" and load_pos <= 3.0) or (p_status == "スローペース" and load_pos >= 10.0 and (f3f - l3f) > 1.5):
                    new_tags.append("🔥 展開逆行"); is_counter = True

        updated_buy_flag = ("★逆行狙い " + buy_flag).strip() if is_counter else buy_flag
        if "】" in memo:
            # 負荷の再計算
            p_diff = 1.5 if p_status != "ミドルペース" else 0.0
            new_load_score = 0.0
            if p_status == "ハイペース" and b_type != "前有利": new_load_score = max(0, (10 - load_pos) * p_diff * 0.2)
            elif p_status == "スローペース" and b_type != "後有利": new_load_score = max(0, (load_pos - 5) * p_diff * 0.1)
            updated_memo = (f"【{p_status}/{b_type}/負荷:{new_load_score:.1f}】" + "/".join(new_tags)).strip("/")
        else:
            updated_memo = "/".join(new_tags) if new_tags else "順境"
        return updated_memo, updated_buy_flag

    st.subheader("🛠️ 一括処理メニュー")
    col_adm1, col_adm2 = st.columns(2)
    with col_adm1:
        if st.button("🔄 DB再解析 (現在の全データに対しロジックを再適用)"):
            st.cache_data.clear(); df = get_db_data()
            for i, row in df.iterrows():
                m, f = update_eval_tags_full(row, df)
                df.at[i, 'memo'], df.at[i, 'next_buy_flag'] = m, f
            if safe_update(df): st.success("全データの再解析・フラグ更新が完了しました。"); st.rerun()
    with col_adm2:
        if st.button("🧼 重複削除 (同名・同日・同レースの重複を除去)"):
            c_before = len(df)
            df = df.drop_duplicates(subset=['name', 'date', 'last_race'], keep='first')
            if len(df) < c_before:
                if safe_update(df): st.success(f"{c_before - len(df)}件の重複データを整理しました。"); st.rerun()
            else: st.info("重複データは見つかりませんでした。")

    if not df.empty:
        st.subheader("🛠️ データ編集エディタ")
        edit_display_df = df.copy(); edit_display_df['base_rtc'] = edit_display_df['base_rtc'].apply(format_time)
        edited_df = st.data_editor(edit_display_df.sort_values("date", ascending=False), num_rows="dynamic", use_container_width=True)
        if st.button("💾 エディタの変更内容をDBに反映"):
            save_df = edited_df.copy(); save_df['base_rtc'] = save_df['base_rtc'].apply(parse_time_str)
            for i, row in save_df.iterrows():
                m, f = update_eval_tags_full(row, save_df)
                save_df.at[i, 'memo'], save_df.at[i, 'next_buy_flag'] = m, f
            if safe_update(save_df): st.success("データベースの修正保存が完了しました。"); st.rerun()
        
        st.divider()
        st.subheader("❌ データ削除設定")
        col_d1, col_d2 = st.columns(2)
        with col_d1:
            race_list = sorted([str(x) for x in df['last_race'].dropna().unique()])
            del_race = st.selectbox("削除対象レース", ["未選択"] + race_list)
            if del_race != "未選択":
                if st.button(f"🚨 {del_race} の全データを削除", type="secondary"):
                    if safe_update(df[df['last_race'] != del_race]): st.success("削除成功"); st.rerun()
        with col_d2:
            horse_list = sorted([str(x) for x in df['name'].dropna().unique()])
            del_horse = st.selectbox("削除対象馬", ["未選択"] + horse_list)
            if del_horse != "未選択":
                if st.button(f"🚨 {del_horse} の全履歴を削除", type="secondary"):
                    if safe_update(df[df['name'] != del_horse]): st.success("削除成功"); st.rerun()

        st.divider()
        with st.expander("☢️ システム初期化（管理者専用）"):
            st.warning("この操作は取り消せません。スプレッドシートの全データが消去されます。")
            if st.button("🧨 データベースを完全にリセット"):
                empty_df = pd.DataFrame(columns=df.columns)
                if safe_update(empty_df): st.success("データベースを初期化しました。"); st.rerun()
