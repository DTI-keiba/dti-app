import streamlit as st
import pandas as pd
import re
import time
from streamlit_gsheets import GSheetsConnection
from datetime import datetime

# ==========================================
# ページ基本設定
# ==========================================
st.set_page_config(
    page_title="DTI Ultimate DB",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Google Sheets 接続設定 ---
conn = st.connection("gsheets", type=GSheetsConnection)

# 🌟 API制限(429 Error)回避のためのキャッシュ設定
# 5分間キャッシュを行い、頻繁な読み込みエラーを防止します
@st.cache_data(ttl=300)
def get_db_data_cached():
    # データベースの全カラム定義（初期から一貫した定義を維持）
    all_cols = [
        "name", "base_rtc", "last_race", "course", "dist", "notes", 
        "timestamp", "f3f", "l3f", "race_l3f", "load", "memo", 
        "date", "cushion", "water", "result_pos", "result_pop", "next_buy_flag"
    ]
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
        df['f3f'] = pd.to_numeric(df['f3f'], errors='coerce').fillna(0.0)
        df['l3f'] = pd.to_numeric(df['l3f'], errors='coerce').fillna(0.0)
        df['race_l3f'] = pd.to_numeric(df['race_l3f'], errors='coerce').fillna(0.0)
        df['load'] = pd.to_numeric(df['load'], errors='coerce').fillna(0.0)
            
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
    if seconds is None or seconds <= 0 or pd.isna(seconds):
        return ""
    if isinstance(seconds, str):
        return seconds
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
        try:
            return float(time_str)
        except:
            return 0.0

# 🌟 【完全復元】コース・馬場負荷係数データ
# 各競馬場の基礎補正値を詳細に定義
COURSE_DATA = {
    "東京": 0.10, 
    "中山": 0.25, 
    "京都": 0.15, 
    "阪神": 0.18, 
    "中京": 0.20,
    "新潟": 0.05, 
    "小倉": 0.30, 
    "福島": 0.28, 
    "札幌": 0.22, 
    "函館": 0.25
}
DIRT_COURSE_DATA = {
    "東京": 0.40, 
    "中山": 0.55, 
    "京都": 0.45, 
    "阪神": 0.48, 
    "中京": 0.50,
    "新潟": 0.42, 
    "小倉": 0.58, 
    "福島": 0.60, 
    "札幌": 0.62, 
    "函館": 0.65
}
SLOPE_FACTORS = {
    "中山": 0.005, 
    "中京": 0.004, 
    "京都": 0.002, 
    "阪神": 0.004, 
    "東京": 0.003,
    "新潟": 0.001, 
    "小倉": 0.002, 
    "福島": 0.003, 
    "札幌": 0.001, 
    "函館": 0.002
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
            memo_str = str(row['memo'])
            b_flag = "💎" in memo_str
            p_flag = "🔥" in memo_str
            if b_flag or p_flag:
                detail = ""
                if b_flag and p_flag:
                    detail = "【💥両方逆行】"
                elif b_flag:
                    detail = "【💎バイアス逆行】"
                elif p_flag:
                    detail = "【🔥ペース逆行】"
                
                pickup_rows.append({
                    "馬名": row['name'], 
                    "逆行タイプ": detail, 
                    "前走": row['last_race'],
                    "日付": row['date'].strftime('%Y-%m-%d') if not pd.isna(row['date']) else "", 
                    "解析メモ": memo_str
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
        # 開催週入力
        track_week = st.number_input("開催週 (例: 1, 8)", 1, 12, 1)

    col1, col2 = st.columns(2)
    with col1: 
        st.markdown("##### 🏁 レースラップ入力")
        lap_input = st.text_area("JRAレースラップ (例: 12.5-11.0-12.0...)", height=150)
        f3f_val = 0.0
        l3f_val = 0.0
        pace_status = "ミドルペース"
        pace_diff = 0.0
        if lap_input:
            laps = [float(x) for x in re.findall(r'\d+\.\d', lap_input)]
            if len(laps) >= 3:
                f3f_val = sum(laps[:3])
                l3f_val = sum(laps[-3:])
                pace_diff = f3f_val - l3f_val
                # 距離別ペースしきい値の動的計算
                dynamic_threshold = 1.0 * (dist / 1600.0)
                if pace_diff < -dynamic_threshold:
                    pace_status = "ハイペース"
                elif pace_diff > dynamic_threshold:
                    pace_status = "スローペース"
                st.success(f"解析完了: 前3F {f3f_val:.1f} / 後3F {l3f_val:.1f} ({pace_status})")
        l3f_val = st.number_input("レース上がり3F (自動計算から修正可)", 0.0, 60.0, l3f_val, step=0.1)

    with col2: 
        st.markdown("##### 🐎 成績表貼り付け")
        raw_input = st.text_area("JRA公式サイトの成績表をそのまま貼り付けてください", height=250)

    # 🌟 【完全復旧】解析前に斤量を手動確認・修正するための詳細プレビューセクション
    if raw_input and f3f_val > 0:
        st.markdown("##### ⚖️ 解析プレビュー（斤量の確認・修正）")
        preview_lines = [l.strip() for l in raw_input.split('\n') if len(l.strip()) > 15]
        preview_list = []
        for line in preview_lines:
            name_parts = re.findall(r'([ァ-ヶー]{2,})', line)
            if not name_parts:
                continue
            # 斤量の自動抽出ロジック
            weight_match = re.search(r'\s([4-6]\d\.\d)\s', line)
            extracted_w = float(weight_match.group(1)) if weight_match else 56.0
            preview_list.append({
                "馬名": name_parts[0], 
                "斤量": extracted_w, 
                "raw_line": line
            })
        
        # 編集可能なテーブルを詳細に表示
        edited_preview_df = st.data_editor(pd.DataFrame(preview_list), use_container_width=True, hide_index=True)

        if st.button("🚀 この内容で解析を実行してデータベースへ保存"):
            parsed_data = []
            for i, row in edited_preview_df.iterrows():
                current_line = row["raw_line"]
                time_match = re.search(r'(\d{1,2}:\d{2}\.\d)', current_line)
                if not time_match:
                    continue
                
                # 着順の取得
                res_pos_match = re.match(r'^(\d{1,2})', current_line)
                res_pos = int(res_pos_match.group(1)) if res_pos_match else 99
                
                # 4角通過順位の取得
                after_time_str = current_line[time_match.end():]
                pos_list = re.findall(r'\b([1-2]?\d)\b', after_time_str)
                four_c_pos = 7.0 
                if pos_list:
                    valid_positions = []
                    for p in pos_list:
                        if int(p) > 30 and len(valid_positions) > 0:
                            break
                        valid_positions.append(float(p))
                    if valid_positions:
                        four_c_pos = valid_positions[-1]
                
                parsed_data.append({
                    "line": current_line, 
                    "res_pos": res_pos, 
                    "four_c_pos": four_c_pos, 
                    "name": row["馬名"], 
                    "weight": row["斤量"]
                })
            
            # --- 【指示反映】バイアス判定ロジック（4着補充特例を詳細記述） ---
            top_3_entries = sorted([d for d in parsed_data if d["res_pos"] <= 3], key=lambda x: x["res_pos"])
            # 極端な位置取り（10番手以下 or 3番手以内）の馬を特定
            outliers = [d for d in top_3_entries if d["four_c_pos"] >= 10.0 or d["four_c_pos"] <= 3.0]
            
            if len(outliers) == 1:
                # 1頭だけ極端な場合は、その馬を除外して4着馬を補充して判定
                base_entries = [d for d in top_3_entries if d != outliers[0]]
                fourth_place = [d for d in parsed_data if d["res_pos"] == 4]
                bias_calculation_entries = base_entries + fourth_place
            else:
                # 0頭または2頭以上の場合は通常通り上位3頭で判定
                bias_calculation_entries = top_3_entries
            
            avg_top_pos = sum(d["four_c_pos"] for d in bias_calculation_entries) / len(bias_calculation_entries) if bias_calculation_entries else 7.0
            bias_type = "前有利" if avg_top_pos <= 4.0 else "後有利" if avg_top_pos >= 10.0 else "フラット"
            
            # 出走頭数の把握（相対化用）
            max_runners = max([d["res_pos"] for d in parsed_data]) if parsed_data else 16

            new_rows_to_save = []
            for entry in parsed_data:
                line_text = entry["line"]
                last_pos = entry["four_c_pos"]
                result_pos = entry["res_pos"]
                indiv_weight = entry["weight"] 
                
                # タイムの秒数換算
                time_match_obj = re.search(r'(\d{1,2}:\d{2}\.\d)', line_text)
                time_str = time_match_obj.group(1)
                m_p, s_p = map(float, time_str.split(':'))
                indiv_time_seconds = m_p * 60 + s_p
                
                # 馬体重の抽出
                h_weight_match = re.search(r'(\d{3})kg', line_text)
                h_weight_str = f"({h_weight_match.group(1)}kg)" if h_weight_match else ""

                # 個別上がり3Fの抽出
                l3f_indiv = 0.0
                l3f_match = re.search(r'(\d{2}\.\d)\s*\d{3}\(', line_text)
                if l3f_match:
                    l3f_indiv = float(l3f_match.group(1))
                else:
                    decimal_finds = re.findall(r'(\d{2}\.\d)', line_text)
                    for d_val in decimal_finds:
                        f_val = float(d_val)
                        if 30.0 <= f_val <= 46.0 and abs(f_val - indiv_weight) > 0.5:
                            l3f_indiv = f_val
                            break
                if l3f_indiv == 0.0:
                    l3f_indiv = l3f_val 
                
                # --- 【指示反映】解析用負荷スコアリング（頭数・非線形補正を詳細記述） ---
                rel_pos_factor = last_pos / max_runners
                # 16頭を基準とした強度補正係数
                field_intensity = max_runners / 16.0
                load_score_val = 0.0
                if pace_status == "ハイペース" and bias_type != "前有利":
                    load_score_val += max(0, (0.6 - rel_pos_factor) * abs(pace_diff) * 3.0) * field_intensity
                elif pace_status == "スローペース" and bias_type != "後有利":
                    load_score_val += max(0, (rel_pos_factor - 0.4) * abs(pace_diff) * 2.0) * field_intensity
                
                # 逆行フラグ詳細判定
                eval_tags = []
                is_counter_target_flag = False
                if result_pos <= 5:
                    if (bias_type == "前有利" and last_pos >= 10.0) or (bias_type == "後有利" and last_pos <= 3.0):
                        # 多頭数時の逆行タグ格上げ
                        upgrade_tag = "💎💎 ﾊﾞｲｱｽ極限逆行" if max_runners >= 16 else "💎 ﾊﾞｲｱｽ逆行"
                        eval_tags.append(upgrade_tag)
                        is_counter_target_flag = True
                
                is_favored_pattern = (pace_status == "ハイペース" and bias_type == "前有利") or (pace_status == "スローペース" and bias_type == "後有利")
                if not is_favored_pattern:
                    if (pace_status == "ハイペース" and last_pos <= 3.0):
                        # 激流被害の判定
                        eval_tags.append("📉 激流被害" if max_runners >= 14 else "🔥 展開逆行")
                        is_counter_target_flag = True
                    elif (pace_status == "スローペース" and last_pos >= 10.0 and (f3f_val - l3f_indiv) > 1.5):
                        eval_tags.append("🔥 展開逆行")
                        is_counter_target_flag = True
                
                # 少頭数展開恩恵の判定
                if max_runners <= 10 and pace_status == "スローペース" and result_pos <= 2:
                    eval_tags.append("🟢 展開恩恵")

                # 上がり性能評価
                if (l3f_val - l3f_indiv) >= 0.5:
                    eval_tags.append("🚀 アガリ優秀")
                elif (l3f_val - l3f_indiv) <= -1.0:
                    eval_tags.append("📉 失速大")
                
                # --- 中盤ラップ解析 ---
                mid_pace_note = "平"
                if dist > 1200:
                    mid_lap_val = (indiv_time_seconds - f3f_val - l3f_indiv) / ((dist - 1200) / 200)
                    if mid_lap_val >= 12.8:
                        mid_pace_note = "緩"
                    elif mid_lap_val <= 11.8:
                        mid_pace_note = "締"
                else:
                    mid_pace_note = "短"

                field_attribute = "多" if max_runners >= 16 else "少" if max_runners <= 10 else "中"
                final_memo = f"【{pace_status}/{bias_type}/負荷:{load_score_val:.1f}({field_attribute})/{mid_pace_note}】{'/'.join(eval_tags) if eval_tags else '順境'}"
                
                # 開催週補正の計算
                week_adjustment_val = (track_week - 1) * 0.05
                
                # 🌟 RTC指数の決定（斤量・馬場・負荷をすべて反映）
                final_rtc_val = (indiv_time_seconds - (indiv_weight - 56.0) * 0.1 - track_index / 10.0 - load_score_val / 10.0 - week_adjustment_val) + bias_val - ((w_4c+w_goal)/2 - 10.0)*0.05 - (9.5-cush)*0.1 + (dist - 1600) * 0.0005
                
                new_rows_to_save.append({
                    "name": entry["name"], 
                    "base_rtc": final_rtc_val, 
                    "last_race": r_name, 
                    "course": c_name, 
                    "dist": dist, 
                    "notes": f"{indiv_weight}kg{h_weight_str}", 
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"), 
                    "f3f": f3f_val, 
                    "l3f": l3f_indiv, 
                    "race_l3f": l3f_val, 
                    "load": last_pos, 
                    "memo": final_memo,
                    "date": r_date.strftime("%Y-%m-%d"), 
                    "cushion": cush, 
                    "water": (w_4c+w_goal)/2, 
                    "next_buy_flag": "★逆行狙い" if is_counter_target_flag else "", 
                    "result_pos": result_pos
                })
            
            if new_rows_to_save:
                current_db_df = get_db_data()
                updated_full_df = pd.concat([current_db_df, pd.DataFrame(new_rows_to_save)], ignore_index=True)
                if safe_update(updated_full_df):
                    st.success(f"✅ 解析完了！{len(new_rows_to_save)}頭のデータをDBに保存しました。")
                    st.rerun()

# --- Tab 2: 馬別履歴 ---
with tab2:
    st.header("📊 馬別履歴 & 買い条件設定")
    db_df_tab2 = get_db_data()
    if not db_df_tab2.empty:
        col_search1, col_search2 = st.columns([1, 1])
        with col_search1:
            search_name_query = st.text_input("馬名で絞り込み検索", key="search_horse_tab2")
        
        horse_list_sorted = sorted([str(x) for x in db_df_tab2['name'].dropna().unique()])
        with col_search2:
            selected_target_horse = st.selectbox("個別メモ・買い条件を編集する馬を選択", ["未選択"] + horse_list_sorted)
        
        if selected_target_horse != "未選択":
            horse_latest_index = db_df_tab2[db_df_tab2['name'] == selected_target_horse].index[-1]
            with st.form("edit_horse_detail_form"):
                current_horse_memo = db_df_tab2.at[horse_latest_index, 'memo'] if not pd.isna(db_df_tab2.at[horse_latest_index, 'memo']) else ""
                new_horse_memo = st.text_area("メモ・特記評価", value=current_horse_memo)
                
                current_buy_flag = db_df_tab2.at[horse_latest_index, 'next_buy_flag'] if not pd.isna(db_df_tab2.at[horse_latest_index, 'next_buy_flag']) else ""
                new_buy_flag = st.text_input("次走への個別買いフラグ", value=current_buy_flag)
                
                if st.form_submit_button("設定内容を保存"):
                    db_df_tab2.at[horse_latest_index, 'memo'] = new_horse_memo
                    db_df_tab2.at[horse_latest_index, 'next_buy_flag'] = new_buy_flag
                    if safe_update(db_df_tab2):
                        st.success(f"{selected_target_horse} の設定を更新しました")
                        st.rerun()
        
        if search_name_query:
            display_horse_df = db_df_tab2[db_df_tab2['name'].str.contains(search_name_query, na=False)]
        else:
            display_horse_df = db_df_tab2
        
        display_horse_df_formatted = display_horse_df.copy()
        display_horse_df_formatted['base_rtc'] = display_horse_df_formatted['base_rtc'].apply(format_time)
        st.dataframe(display_horse_df_formatted.sort_values("date", ascending=False)[["date", "name", "last_race", "base_rtc", "f3f", "l3f", "race_l3f", "load", "memo", "next_buy_flag"]], use_container_width=True)

# --- Tab 3: レース別履歴 ---
with tab3:
    st.header("🏁 答え合わせ & レース別履歴")
    db_df_tab3 = get_db_data()
    if not db_df_tab3.empty:
        full_race_list = sorted([str(x) for x in db_df_tab3['last_race'].dropna().unique()])
        selected_race_name = st.selectbox("表示するレースを選択してください", full_race_list)
        if selected_race_name:
            race_history_df = db_df_tab3[db_df_tab3['last_race'] == selected_race_name].copy()
            with st.form("race_result_entry_form"):
                st.write(f"【{selected_race_name}】の結果・人気を入力してください")
                for idx, row in race_history_df.iterrows():
                    current_res_pos = int(row['result_pos']) if not pd.isna(row['result_pos']) else 0
                    current_res_pop = int(row['result_pop']) if not pd.isna(row['result_pop']) else 0
                    
                    col_res1, col_res2 = st.columns(2)
                    with col_res1:
                        race_history_df.at[idx, 'result_pos'] = st.number_input(f"{row['name']} 着順", 0, 100, value=min(max(0, current_res_pos), 100), key=f"pos_input_{idx}")
                    with col_res2:
                        race_history_df.at[idx, 'result_pop'] = st.number_input(f"{row['name']} 人気", 0, 100, value=min(max(0, current_res_pop), 100), key=f"pop_input_{idx}")
                
                if st.form_submit_button("レース結果を保存"):
                    for idx, row in race_history_df.iterrows():
                        db_df_tab3.at[idx, 'result_pos'] = row['result_pos']
                        db_df_tab3.at[idx, 'result_pop'] = row['result_pop']
                    if safe_update(db_df_tab3):
                        st.success("レースの結果をスプレッドシートに保存しました。")
                        st.rerun()
            
            display_race_history_formatted = race_history_df.copy()
            display_race_history_formatted['base_rtc'] = display_race_history_formatted['base_rtc'].apply(format_time)
            st.dataframe(display_race_history_formatted[["name", "notes", "base_rtc", "f3f", "l3f", "race_l3f", "result_pos", "result_pop"]], use_container_width=True)

# --- Tab 4: シミュレーター ---
with tab4:
    st.header("🎯 次走シミュレーター & 統合評価")
    db_df_tab4 = get_db_data()
    if not db_df_tab4.empty:
        all_horse_names_list = sorted([str(x) for x in db_df_tab4['name'].dropna().unique()])
        selected_horses_sim = st.multiselect("出走予定馬を選択してください", options=all_horse_names_list)
        
        sim_input_pops = {}
        sim_input_gates = {}
        sim_input_weights = {}
        
        if selected_horses_sim:
            st.markdown("##### 📝 枠番・予想人気・想定斤量の個別入力")
            sim_pop_cols = st.columns(min(len(selected_horses_sim), 4))
            for i, h_name in enumerate(selected_horses_sim):
                with sim_pop_cols[i % 4]:
                    h_latest_data = db_df_tab4[db_df_tab4['name'] == h_name].iloc[-1]
                    sim_input_gates[h_name] = st.number_input(f"{h_name} 枠番", 1, 18, value=1, key=f"sim_gate_{h_name}")
                    sim_input_pops[h_name] = st.number_input(f"{h_name} 人気", 1, 18, value=int(h_latest_data['result_pop']) if not pd.isna(h_latest_data['result_pop']) else 10, key=f"sim_pop_{h_name}")
                    # 🌟 【完全復旧】個別斤量入力
                    sim_input_weights[h_name] = st.number_input(f"{h_name} 斤量", 48.0, 62.0, 56.0, step=0.5, key=f"sim_weight_{h_name}")

            col_sim_cfg1, col_sim_cfg2 = st.columns(2)
            with col_sim_cfg1: 
                sim_target_course = st.selectbox("次走の競馬場", list(COURSE_DATA.keys()), key="sim_target_course_select")
                sim_target_dist = st.selectbox("距離 (m)", list(range(1000, 3700, 100)), index=6)
                sim_target_track = st.radio("次走トラック種別", ["芝", "ダート"], horizontal=True)
            with col_sim_cfg2: 
                sim_current_cushion = st.slider("想定クッション値", 7.0, 12.0, 9.5)
                sim_current_water = st.slider("想定含水率 (%)", 0.0, 30.0, 10.0)
            
            if st.button("🏁 シミュレーション実行"):
                sim_results_list = []
                num_sim_horses = len(selected_horses_sim)
                sim_styles_count = {"逃げ": 0, "先行": 0, "差し": 0, "追込": 0}
                sim_overall_l3f_avg = db_df_tab4['l3f'].mean()

                for h_name in selected_horses_sim:
                    h_full_history = db_df_tab4[db_df_tab4['name'] == h_name].sort_values("date")
                    sim_last_3_runs = h_full_history.tail(3)
                    sim_converted_rtc_list = []
                    
                    # 脚質判定
                    avg_load_val_3r = sim_last_3_runs['load'].mean()
                    if avg_load_val_3r <= 3.5: 
                        h_style_type = "逃げ"
                    elif avg_load_val_3r <= 7.0: 
                        h_style_type = "先行"
                    elif avg_load_val_3r <= 11.0: 
                        h_style_type = "差し"
                    else: 
                        h_style_type = "追込"
                    sim_styles_count[h_style_type] += 1

                    # 🌟 頭数連動：渋滞リスク判定
                    traffic_jam_tag = "⚠️詰まり注意" if num_sim_horses >= 15 and h_style_type in ["差し", "追込"] and sim_input_gates[h_name] <= 4 else "-"

                    # 🌟 頭数連動：スロー適性判定
                    sim_slow_aptitude_tag = "-"
                    if num_sim_horses <= 10:
                        h_best_past_l3f = h_full_history['l3f'].min()
                        if h_best_past_l3f < sim_overall_l3f_avg - 0.5:
                            sim_slow_aptitude_tag = "⚡スロー特化"
                        elif h_best_past_l3f > sim_overall_l3f_avg + 0.5:
                            sim_slow_aptitude_tag = "📉瞬発力不足"

                    # 各種タグ判定
                    h_rtc_std_val = h_full_history['base_rtc'].std() if len(h_full_history) >= 3 else 0.0
                    h_stability_label = "⚖️安定" if 0 < h_rtc_std_val < 0.2 else "🎢ムラ" if h_rtc_std_val > 0.4 else "-"
                    
                    h_best_run_data = h_full_history.loc[h_full_history['base_rtc'].idxmin()]
                    h_aptitude_label = "🎯馬場◎" if abs(h_best_run_data['cushion'] - sim_current_cushion) <= 0.5 and abs(h_best_run_data['water'] - sim_current_water) <= 2.0 else "-"

                    # 🌟 【完全復旧】過去3走すべてにおける個別斤量補正ループ
                    for idx, row in sim_last_3_runs.iterrows():
                        p_race_dist = row['dist']
                        p_race_rtc = row['base_rtc']
                        p_race_course = row['course']
                        p_race_load = row['load']
                        p_race_notes = str(row['notes'])
                        
                        # 前走時点の斤量と馬体重の抽出
                        p_race_weight = 56.0
                        h_body_weight_sim = 480.0
                        w_match_sim = re.search(r'([4-6]\d\.\d)', p_race_notes)
                        if w_match_sim:
                            p_race_weight = float(w_match_sim.group(1))
                        
                        hb_match_sim = re.search(r'\((\d{3})kg\)', p_race_notes)
                        if hb_match_sim:
                            h_body_weight_sim = float(hb_match_sim.group(1))
                        
                        if p_race_dist > 0:
                            p_load_adj = (p_race_load - 7.0) * 0.02
                            # 🌟 斤量感応度（馬体重に基づく詳細ロジック）
                            weight_sensitivity_factor = 0.15 if h_body_weight_sim <= 440 else 0.08 if h_body_weight_sim >= 500 else 0.1
                            
                            # 今回入力された個別斤量との差分を補正
                            weight_difference_adjustment = (sim_input_weights[h_name] - p_race_weight) * weight_sensitivity_factor
                            
                            # 指数変換計算
                            base_converted_rtc = (p_race_rtc + p_load_adj + weight_difference_adjustment) / p_race_dist * sim_target_dist
                            
                            # 坂・高低差補正
                            slope_adjustment_val = (SLOPE_FACTORS.get(sim_target_course, 0.002) - SLOPE_FACTORS.get(p_race_course, 0.002)) * sim_target_dist
                            sim_converted_rtc_list.append(base_converted_rtc + slope_adjustment_val)
                    
                    avg_sim_rtc_result = sum(sim_converted_rtc_list) / len(sim_converted_rtc_list) if sim_converted_rtc_list else 0
                    
                    # 距離実績の弾力性補正
                    h_best_dist_past = h_full_history.loc[h_full_history['base_rtc'].idxmin(), 'dist']
                    avg_sim_rtc_result += (abs(sim_target_dist - h_best_dist_past) / 100) * 0.05
                    
                    # 近影上昇（モメンタム）判定
                    h_momentum_label = "-"
                    if len(h_full_history) >= 2:
                        if h_full_history.iloc[-1]['base_rtc'] < h_full_history.iloc[-2]['base_rtc'] - 0.2:
                            h_momentum_label = "📈上昇"
                            avg_sim_rtc_result -= 0.15

                    # 枠順×バイアスのシナジー補正
                    synergy_bias_adj = -0.2 if (sim_input_gates[h_name] <= 4 and bias_val <= -0.5) or (sim_input_gates[h_name] >= 13 and bias_val >= 0.5) else 0
                    avg_sim_rtc_result += synergy_bias_adj

                    # コース相性ボーナス
                    h_course_bonus_val = -0.2 if any((h_full_history['course'] == sim_target_course) & (h_full_history['result_pos'] <= 3)) else 0.0
                    
                    # 含水率・クッション値の最終アジャスト
                    water_adjustment_final = (sim_current_water - 10.0) * 0.05
                    course_master_dict = DIRT_COURSE_DATA if sim_target_track == "ダート" else COURSE_DATA
                    if sim_target_track == "ダート":
                        water_adjustment_final = -water_adjustment_final # ダートは水を含んだ方が速い
                    
                    final_sim_rtc_computed = (avg_sim_rtc_result + (course_master_dict[sim_target_course] * (sim_target_dist/1600.0)) + h_course_bonus_val + water_adjustment_final - (9.5 - sim_current_cushion) * 0.1)
                    
                    h_latest_entry = sim_last_3_runs.iloc[-1]
                    sim_results_list.append({
                        "馬名": h_name, 
                        "脚質": h_style_type, 
                        "想定タイム": final_sim_rtc_computed, 
                        "渋滞": traffic_jam_tag, 
                        "スロー": sim_slow_aptitude_tag, 
                        "適性": h_aptitude_label, 
                        "安定": h_stability_label, 
                        "偏差": "⤴️覚醒期待" if final_sim_rtc_computed < h_full_history['base_rtc'].min() - 0.3 else "-", 
                        "上昇": h_momentum_label, 
                        "レベル": "🔥強ﾒﾝﾂ" if db_df_tab4[db_df_tab4['last_race'] == h_latest_entry['last_race']]['base_rtc'].mean() < db_df_tab4['base_rtc'].mean() - 0.2 else "-", 
                        "load": h_latest_entry['load'], 
                        "状態": "💤休み明け" if (datetime.now() - h_latest_entry['date']).days // 7 >= 12 else "-", 
                        "raw_rtc": final_sim_rtc_computed,
                        "解析メモ": h_latest_entry['memo']
                    })
                
                # 展開予想ロジック
                sim_pace_prediction = "ミドルペース"
                if sim_styles_count["逃げ"] >= 2 or (sim_styles_count["逃げ"] + sim_styles_count["先行"]) >= num_sim_horses * 0.6:
                    sim_pace_prediction = "ハイペース傾向"
                elif sim_styles_count["逃げ"] == 0 and sim_styles_count["先行"] <= 1:
                    sim_pace_prediction = "スローペース傾向"
                
                sim_final_df = pd.DataFrame(sim_results_list)
                
                # 🌟 脚質・展開シナジー反映（多頭数時は影響度1.5倍）
                sim_pace_multiplier = 1.5 if num_sim_horses >= 15 else 1.0
                def apply_pace_synergy_func(row):
                    synergy_adj_val = 0.0
                    if "ハイ" in sim_pace_prediction:
                        if row['脚質'] in ["差し", "追込"]:
                            synergy_adj_val = -0.2 * sim_pace_multiplier
                        elif row['脚質'] == "逃げ":
                            synergy_adj_val = 0.2 * sim_pace_multiplier
                    elif "スロー" in sim_pace_prediction:
                        if row['脚質'] in ["逃げ", "先行"]:
                            synergy_adj_val = -0.2 * sim_pace_multiplier
                        elif row['脚質'] in ["差し", "追込"]:
                            synergy_adj_val = 0.2 * sim_pace_multiplier
                    return row['raw_rtc'] + synergy_adj_val

                sim_final_df['synergy_rtc'] = sim_final_df.apply(apply_pace_synergy_func, axis=1)
                sim_final_df = sim_final_df.sort_values("synergy_rtc")
                sim_final_df['RTC順位'] = range(1, len(sim_final_df) + 1)
                sim_top_time = sim_final_df.iloc[0]['raw_rtc']
                sim_final_df['差'] = sim_final_df['raw_rtc'] - sim_top_time
                sim_final_df['予想人気'] = sim_final_df['馬名'].map(sim_input_pops)
                sim_final_df['妙味スコア'] = sim_final_df['予想人気'] - sim_final_df['RTC順位']
                
                # 印の割り当て
                sim_final_df['役割'] = "-"
                sim_final_df.loc[sim_final_df['RTC順位'] == 1, '役割'] = "◎"
                sim_final_df.loc[sim_final_df['RTC順位'] == 2, '役割'] = "〇"
                sim_final_df.loc[sim_final_df['RTC順位'] == 3, '役割'] = "▲"
                potential_bomb_horses = sim_final_df[sim_final_df['RTC順位'] > 1].sort_values("妙味スコア", ascending=False)
                if not potential_bomb_horses.empty:
                    sim_final_df.loc[sim_final_df['馬名'] == potential_bomb_horses.iloc[0]['馬名'], '役割'] = "★"
                
                # 表示用フォーマット
                sim_final_df['想定タイム'] = sim_final_df['raw_rtc'].apply(format_time)
                sim_final_df['差'] = sim_final_df['差'].apply(lambda x: f"+{x:.1f}" if x > 0 else "±0.0")

                st.markdown("---")
                st.subheader(f"🏁 展開予想：{sim_pace_prediction} ({num_sim_horses}頭立て)")
                st.write(f"【脚質構成】 逃げ:{sim_styles_count['逃げ']} / 先行:{sim_styles_count['先行']} / 差し:{sim_styles_count['差し']} / 追込:{sim_styles_count['追込']}")
                
                fav_h_sim = sim_final_df[sim_final_df['役割'] == "◎"].iloc[0]['馬名'] if not sim_final_df[sim_final_df['役割'] == "◎"].empty else ""
                opp_h_sim = sim_final_df[sim_final_df['役割'] == "〇"].iloc[0]['馬名'] if not sim_final_df[sim_final_df['役割'] == "〇"].empty else ""
                bomb_h_sim = sim_final_df[sim_final_df['役割'] == "★"].iloc[0]['馬名'] if not sim_final_df[sim_final_df['役割'] == "★"].empty else ""
                
                col_rec_sim1, col_rec_sim2 = st.columns(2)
                with col_rec_sim1:
                    st.info(f"**🎯 馬連・ワイド1点勝負**\n\n◎ {fav_h_sim} － 〇 {opp_h_sim}")
                with col_rec_sim2: 
                    if bomb_h_sim:
                        st.warning(f"**💣 妙味狙いワイド1点**\n\n◎ {fav_h_sim} － ★ {bomb_h_sim} (展開×妙味)")
                
                def highlight_sim_results(row):
                    if row['役割'] == "★":
                        return ['background-color: #ffe4e1; font-weight: bold'] * len(row)
                    if row['役割'] == "◎":
                        return ['background-color: #fff700; font-weight: bold; color: black'] * len(row)
                    return [''] * len(row)
                
                st.table(sim_final_df[["役割", "馬名", "脚質", "渋滞", "スロー", "想定タイム", "差", "妙味スコア", "適性", "安定", "上昇", "レベル", "偏差", "load", "状態", "解析メモ"]].style.apply(highlight_sim_results, axis=1))

# --- Tab 5: トレンド解析 ---
with tab5:
    st.header("📈 馬場トレンド & 統計解析")
    db_df_tab5 = get_db_data()
    if not db_df_tab5.empty:
        target_course_trend = st.selectbox("トレンドを確認する競馬場を選択", list(COURSE_DATA.keys()), key="trend_course_select")
        trend_analysis_df = db_df_tab5[db_df_tab5['course'] == target_course_trend].sort_values("date")
        if not trend_analysis_df.empty:
            st.subheader("💧 クッション値 & 含水率の時系列推移")
            st.line_chart(trend_analysis_df.set_index("date")[["cushion", "water"]])
            st.subheader("🏁 直近のレース傾向 (4角平均通過順位)")
            recent_races_trend = trend_analysis_df.groupby('last_race').agg({'load':'mean', 'date':'max'}).sort_values('date', ascending=False).head(15)
            st.bar_chart(recent_races_trend['load'])
            st.subheader("📊 直近上がり3F推移")
            st.line_chart(trend_analysis_df.set_index("date")["race_l3f"])

# --- Tab 6: データ管理 ---
with tab6:
    st.header("🗑 データベース保守 & 高度な管理機能")
    db_df_tab6 = get_db_data()

    def update_eval_tags_full_logic(row, df_context=None):
        """【完全復元】再解析・データの再検証用詳細ロジック"""
        current_memo = str(row['memo']) if not pd.isna(row['memo']) else ""
        current_buy_flag = str(row['next_buy_flag']).replace("★逆行狙い", "").strip()
        
        # 既存のメモからタグ部分を除去してベースを抽出
        base_memo_clean = re.sub(r'【.*?】', '', current_memo).strip("/")
        
        def safe_to_float(val):
            try:
                return float(val) if not pd.isna(val) else 0.0
            except:
                return 0.0
        
        f3f_v, l3f_v, r_l3f_v, res_pos_v, load_pos_v, dist_v, rtc_v = map(safe_to_float, [
            row['f3f'], row['l3f'], row['race_l3f'], row['result_pos'], row['load'], row['dist'], row['base_rtc']
        ])
        
        # 中盤ラップ判定
        m_note_v = "平"
        if dist_v > 1200 and f3f_v > 0:
            m_lap_v = (rtc_v - f3f_v - l3f_v) / ((dist_v - 1200) / 200)
            if m_lap_v >= 12.8:
                m_note_v = "緩"
            elif m_lap_v <= 11.8:
                m_note_v = "締"
        elif dist_v <= 1200:
            m_note_v = "短"

        # バイアス判定（4着補充特例を完全再現）
        b_type_v = "フラット"
        max_r_v = 16
        if df_context is not None and not pd.isna(row['last_race']):
            race_context_horses = df_context[df_context['last_race'] == row['last_race']]
            max_r_v = race_context_horses['result_pos'].max() if not race_context_horses.empty else 16
            top_3_race_v = race_context_horses[pd.to_numeric(race_context_horses['result_pos'], errors='coerce') <= 3].copy()
            top_3_race_v['load'] = pd.to_numeric(top_3_race_v['load'], errors='coerce').fillna(7.0)
            
            outliers_v = top_3_race_v[(top_3_race_v['load'] >= 10.0) | (top_3_race_v['load'] <= 3.0)]
            if len(outliers_v) == 1:
                bias_set_v = pd.concat([top_3_race_v[top_3_race_v['name'] != outliers_v.iloc[0]['name']], race_context_horses[pd.to_numeric(race_context_horses['result_pos'], errors='coerce') == 4]])
            else:
                bias_set_v = top_3_race_v
            
            if not bias_set_v.empty:
                avg_top_pos_v = bias_set_v['load'].mean()
                b_type_v = "前有利" if avg_top_pos_v <= 4.0 else "後有利" if avg_top_pos_v >= 10.0 else "フラット"

        # ペース判定（メモから抽出）
        p_status_v = "ハイペース" if "ハイ" in current_memo else "スローペース" if "スロー" in current_memo else "ミドルペース"
        p_diff_v = 1.5 if p_status_v != "ミドルペース" else 0.0
        rel_p_v = load_pos_v / max_r_v
        
        # 🌟 再解析時も頭数強度を反映（非線形負荷）
        field_intensity_v = max_r_v / 16.0
        new_load_score_v = 0.0
        if p_status_v == "ハイペース" and b_type_v != "前有利":
            new_load_score_v = max(0, (0.6 - rel_p_v) * p_diff_v * 3.0) * field_intensity_v
        elif p_status_v == "スローペース" and b_type_v != "後有利":
            new_load_score_v = max(0, (rel_p_v - 0.4) * p_diff_v * 2.0) * field_intensity_v
        
        # 新しいタグの構成
        new_tags_v = []
        is_counter_v = False
        if r_l3f_v > 0:
            diff_l3f_v = r_l3f_v - l3f_v
            if diff_l3f_v >= 0.5:
                new_tags_v.append("🚀 アガリ優秀")
            elif diff_l3f_v <= -1.0:
                new_tags_v.append("📉 失速大")
        
        if res_pos_v <= 5:
            if (b_type_v == "前有利" and load_pos_v >= 10.0) or (b_type_v == "後有利" and load_pos_v <= 3.0):
                new_tags_v.append("💎💎 ﾊﾞｲｱｽ極限逆行" if max_r_v >= 16 else "💎 ﾊﾞｲｱｽ逆行")
                is_counter_v = True
            
            is_favored_v = (p_status_v == "ハイペース" and b_type_v == "前有利") or (p_status_v == "スローペース" and b_type_v == "後有利")
            if not is_favored_v:
                if (p_status_v == "ハイペース" and load_pos_v <= 3.0):
                    new_tags_v.append("📉 激流被害" if max_r_v >= 14 else "🔥 展開逆行")
                    is_counter_v = True
                elif (p_status_v == "スローペース" and load_pos_v >= 10.0 and (f3f_v - l3f_v) > 1.5):
                    new_tags_v.append("🔥 展開逆行")
                    is_counter_v = True
        
        if max_r_v <= 10 and p_status_v == "スローペース" and res_pos_v <= 2:
            new_tags_v.append("🟢 展開恩恵")

        field_attr_v = "多" if max_r_v >= 16 else "少" if max_r_v <= 10 else "中"
        updated_memo_text = (f"【{p_status_v}/{b_type_v}/負荷:{new_load_score_v:.1f}({field_attr_v})/{m_note_v}】" + "/".join(new_tags_v)).strip("/")
        updated_buy_flag_text = ("★逆行狙い " + current_buy_flag).strip() if is_counter_v else current_buy_flag
        
        return updated_memo_text, updated_buy_flag_text

    # --- 🗓 過去レースの開催週を一括設定セクション ---
    st.subheader("🗓 過去レースの開催週を一括設定")
    if not db_df_tab6.empty:
        race_master_weeks = db_df_tab6[['last_race', 'date']].drop_duplicates(subset=['last_race']).copy()
        race_master_weeks['track_week'] = 1
        # データエディタで週数を一括入力可能に
        edited_weeks_df = st.data_editor(race_master_weeks, hide_index=True)
        
        if st.button("🔄 補正&再解析を一括適用"):
            week_lookup_dict = dict(zip(edited_weeks_df['last_race'], edited_weeks_df['track_week']))
            for idx_w, row_w in db_df_tab6.iterrows():
                if row_w['last_race'] in week_lookup_dict:
                    # RTC指数の遡り補正
                    db_df_tab6.at[idx_w, 'base_rtc'] = row_w['base_rtc'] - (week_lookup_dict[row_w['last_race']] - 1) * 0.05
                    # メモとフラグも最新ロジックで再生成
                    memo_re, flag_re = update_eval_tags_full_logic(db_df_tab6.iloc[idx_w], db_df_tab6)
                    db_df_tab6.at[idx_w, 'memo'] = memo_re
                    db_df_tab6.at[idx_w, 'next_buy_flag'] = flag_re
            
            if safe_update(db_df_tab6):
                st.success("全ての過去データの開催週補正と再解析が完了しました。")
                st.rerun()

    st.subheader("🛠️ 一括処理メニュー")
    col_adm_btn1, col_adm_btn2 = st.columns(2)
    with col_adm_btn1:
        if st.button("🔄 DB再解析（ロジックのみ一括更新）"):
            for idx_re, row_re in db_df_tab6.iterrows():
                m_re, f_re = update_eval_tags_full_logic(row_re, db_df_tab6)
                db_df_tab6.at[idx_re, 'memo'] = m_re
                db_df_tab6.at[idx_re, 'next_buy_flag'] = f_re
            if safe_update(db_df_tab6):
                st.success("DBの全履歴に対して最新ロジックを再適用しました。")
                st.rerun()
    with col_adm_btn2:
        if st.button("🧼 重複削除（完全一致行をクリーニング）"):
            count_before = len(db_df_tab6)
            db_df_tab6 = db_df_tab6.drop_duplicates(subset=['name', 'date', 'last_race'], keep='first')
            count_after = len(db_df_tab6)
            if safe_update(db_df_tab6):
                st.success(f"重複データ {count_before - count_after} 件を削除しました。")
                st.rerun()

    if not db_df_tab6.empty:
        st.subheader("🛠️ データ編集エディタ")
        edit_display_full_df = db_df_tab6.copy()
        edit_display_full_df['base_rtc'] = edit_display_full_df['base_rtc'].apply(format_time)
        final_edited_db_df = st.data_editor(
            edit_display_full_df.sort_values("date", ascending=False), 
            num_rows="dynamic", 
            use_container_width=True
        )
        
        if st.button("💾 エディタの変更内容をDBに反映"):
            converted_save_df = final_edited_db_df.copy()
            converted_save_df['base_rtc'] = converted_save_df['base_rtc'].apply(parse_time_str)
            if safe_update(converted_save_df):
                st.success("エディタでの修正内容をスプレッドシートに反映しました。")
                st.rerun()
        
        st.divider()
        st.subheader("❌ データ削除設定")
        col_del_final1, col_del_final2 = st.columns(2)
        with col_del_final1:
            all_races_for_del = sorted([str(x) for x in db_df_tab6['last_race'].dropna().unique()])
            target_race_to_del = st.selectbox("削除対象レースを選択", ["未選択"] + all_races_for_del)
            if target_race_to_del != "未選択":
                if st.button(f"🚨 レース【{target_race_to_del}】を全削除"):
                    filtered_del_race_df = db_df_tab6[db_df_tab6['last_race'] != target_race_to_del]
                    if safe_update(filtered_del_race_df):
                        st.rerun()
        
        with col_del_final2:
            all_horses_for_del = sorted([str(x) for x in db_df_tab6['name'].dropna().unique()])
            # 🌟 【完全復旧】マルチセレクト形式の馬名一括削除
            target_horses_to_del = st.multiselect("削除する馬を選択してください（複数選択可）", all_horses_for_del, key="multi_del_horses_admin")
            if target_horses_to_del:
                if st.button(f"🚨 選択した {len(target_horses_to_del)} 頭をDBから完全削除"):
                    filtered_del_horse_df = db_df_tab6[~db_df_tab6['name'].isin(target_horses_to_del)]
                    if safe_update(filtered_del_horse_df):
                        st.rerun()

        st.divider()
        with st.expander("☢️ システム初期化"):
            st.warning("この操作は取り消せません。全ての実績データが消去されます。")
            if st.button("🧨 データベースを完全にリセット"):
                reset_empty_df = pd.DataFrame(columns=db_df_tab6.columns)
                if safe_update(reset_empty_df):
                    st.success("データベースを初期化しました。")
                    st.rerun()
