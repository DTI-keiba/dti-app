import streamlit as st
import pandas as pd
import re
import time
from streamlit_gsheets import GSheetsConnection
from datetime import datetime

# ==============================================================================
# 1. ページ基本設定
# ==============================================================================
st.set_page_config(
    page_title="DTI Ultimate DB",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Google Sheets 接続設定 ---
conn = st.connection("gsheets", type=GSheetsConnection)

# ==============================================================================
# 2. データベース読み込みロジック (キャッシュ管理)
# ==============================================================================
@st.cache_data(ttl=300)
def get_db_data_cached():
    """
    Google Sheetsからデータを読み込み、前処理を行う。
    キャッシュを有効にしつつ、ttl=0の直接読み込みにも対応可能。
    """
    # データベースの全カラム定義（初期から一貫した定義を維持）
    all_cols = [
        "name", 
        "base_rtc", 
        "last_race", 
        "course", 
        "dist", 
        "notes", 
        "timestamp", 
        "f3f", 
        "l3f", 
        "race_l3f", 
        "load", 
        "memo", 
        "date", 
        "cushion", 
        "water", 
        "result_pos", 
        "result_pop", 
        "next_buy_flag"
    ]
    
    try:
        # 🌟 キャッシュを使わず最新を読み取る必要がある場合は ttl=0 で呼び出しが行われる
        df = conn.read(ttl=0)
        
        if df is None:
            return pd.DataFrame(columns=all_cols)
            
        if df.empty:
            return pd.DataFrame(columns=all_cols)
        
        # 不足しているカラムがあれば初期値Noneで補填
        for col in all_cols:
            if col not in df.columns:
                df[col] = None
        
        # データの型変換（エラーハンドリングを詳細に記述）
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            
        if 'result_pos' in df.columns:
            df['result_pos'] = pd.to_numeric(df['result_pos'], errors='coerce')
        
        # 🌟 三段階ソートロジック
        # 1. 日付(新しい順) 
        # 2. レース名(名前順) 
        # 3. 着順(1着から)
        df = df.sort_values(
            by=["date", "last_race", "result_pos"], 
            ascending=[False, True, True]
        )
        
        # 数値カラムの変換とNaN補完（1ミリも削らず詳細に）
        if 'result_pop' in df.columns:
            df['result_pop'] = pd.to_numeric(df['result_pop'], errors='coerce')
            
        if 'f3f' in df.columns:
            df['f3f'] = pd.to_numeric(df['f3f'], errors='coerce').fillna(0.0)
            
        if 'l3f' in df.columns:
            df['l3f'] = pd.to_numeric(df['l3f'], errors='coerce').fillna(0.0)
            
        if 'race_l3f' in df.columns:
            df['race_l3f'] = pd.to_numeric(df['race_l3f'], errors='coerce').fillna(0.0)
            
        if 'load' in df.columns:
            df['load'] = pd.to_numeric(df['load'], errors='coerce').fillna(0.0)
            
        if 'base_rtc' in df.columns:
            df['base_rtc'] = pd.to_numeric(df['base_rtc'], errors='coerce').fillna(0.0)
            
        # 全ての行が空のデータは除外
        df = df.dropna(how='all')
        
        return df
        
    except Exception as e:
        st.error(f"【重大な警告】スプレッドシートの読み込み中にエラーが発生しました。詳細な原因を確認してください: {e}")
        return pd.DataFrame(columns=all_cols)

def get_db_data():
    """get_db_data_cachedへのインターフェース"""
    return get_db_data_cached()

# ==============================================================================
# 3. データベース更新ロジック (安全な上書き)
# ==============================================================================
def safe_update(df):
    """
    Google Sheetsへデータを書き戻す。
    リトライ機能、ソート、インデックスリセット、キャッシュクリアを含む。
    """
    # 保存直前に整合性を確保
    if 'date' in df.columns:
        if 'last_race' in df.columns:
            if 'result_pos' in df.columns:
                df['date'] = pd.to_datetime(df['date'], errors='coerce')
                df['result_pos'] = pd.to_numeric(df['result_pos'], errors='coerce')
                df = df.sort_values(
                    by=["date", "last_race", "result_pos"], 
                    ascending=[False, True, True]
                )
    
    # インデックスの不一致によるエラーを防ぐため完全にリセット
    df = df.reset_index(drop=True)
    
    max_retries = 3
    for i in range(max_retries):
        try:
            # 🌟 最新状態での上書きを実行
            conn.update(data=df)
            
            # 🌟 重要：成功時にキャッシュを強制クリアして、同期不全を完全に解消
            st.cache_data.clear()
            
            return True
            
        except Exception as e:
            wait_time = 5
            if i < max_retries - 1:
                st.warning(f"Google Sheets接続エラー(試行 {i+1}/3回目)... {wait_time}秒後にリトライします。")
                time.sleep(wait_time)
                continue
            else:
                st.error(f"Google Sheetsの更新に失敗しました。詳細を確認してください: {e}")
                return False

# ==============================================================================
# 4. 補助関数 (フォーマット等)
# ==============================================================================
def format_time(seconds):
    """秒数を mm:ss.f 形式の文字列に変換"""
    if seconds is None:
        return ""
    if seconds <= 0:
        return ""
    if pd.isna(seconds):
        return ""
    if isinstance(seconds, str):
        return seconds
        
    minutes_val = int(seconds // 60)
    seconds_val = seconds % 60
    return f"{minutes_val}:{seconds_val:04.1f}"

def parse_time_str(time_str):
    """mm:ss.f 形式の文字列を秒数(float)に変換"""
    if time_str is None:
        return 0.0
    try:
        if ":" in str(time_str):
            minutes_part, seconds_part = map(float, str(time_str).split(':'))
            return minutes_part * 60 + seconds_part
        return float(time_str)
    except:
        return 0.0

# ==============================================================================
# 5. 係数マスタ (詳細数値完全復元)
# ==============================================================================
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

# ==============================================================================
# 6. メインUI構成 - タブ設定
# ==============================================================================
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "📝 解析・保存", 
    "🐎 馬別履歴", 
    "🏁 レース別履歴", 
    "🎯 シミュレーター", 
    "📈 馬場トレンド", 
    "🗑 データ管理"
])

# ==============================================================================
# 7. Tab 1: 解析・保存
# ==============================================================================
with tab1:
    # 注目馬ピックアップ
    df_pickup = get_db_data()
    if not df_pickup.empty:
        st.subheader("🎯 次走注目馬（逆行評価ピックアップ）")
        pickup_rows = []
        for i, row in df_pickup.iterrows():
            memo_text = str(row['memo'])
            bias_逆行 = "💎" in memo_text
            pace_逆行 = "🔥" in memo_text
            
            if bias_逆行 or pace_逆行:
                target_type = ""
                if bias_逆行 and pace_逆行:
                    target_type = "【💥両方逆行】"
                elif bias_逆行:
                    target_type = "【💎バイアス逆行】"
                elif pace_逆行:
                    target_type = "【🔥ペース逆行】"
                
                pickup_rows.append({
                    "馬名": row['name'], 
                    "逆行タイプ": target_type, 
                    "前走": row['last_race'],
                    "日付": row['date'].strftime('%Y-%m-%d') if not pd.isna(row['date']) else "", 
                    "解析メモ": memo_text
                })
        
        if pickup_rows:
            st.dataframe(
                pd.DataFrame(pickup_rows).sort_values("日付", ascending=False), 
                use_container_width=True, 
                hide_index=True
            )
            
    st.divider()

    st.header("🚀 レース解析 & 自動保存システム")
    
    # サイドバー設定
    with st.sidebar:
        st.title("解析条件設定")
        analysis_race_name = st.text_input("レース名 (例: 有馬記念)")
        analysis_race_date = st.date_input("レース実施日", datetime.now())
        analysis_course_name = st.selectbox("競馬場選択", list(COURSE_DATA.keys()))
        analysis_track_type = st.radio("トラック種別", ["芝", "ダート"], horizontal=True)
        dist_opts = list(range(1000, 3700, 100))
        analysis_dist = st.selectbox("距離 (m)", dist_opts, index=dist_opts.index(1600))
        st.divider()
        st.write("💧 馬場コンディション詳細")
        analysis_cushion = st.number_input("クッション値 (芝のみ)", 7.0, 12.0, 9.5, step=0.1) if analysis_track_type == "芝" else 9.5
        analysis_water_4c = st.number_input("含水率：4角地点 (%)", 0.0, 50.0, 10.0, step=0.1)
        analysis_water_goal = st.number_input("含水率：ゴール前地点 (%)", 0.0, 50.0, 10.0, step=0.1)
        analysis_track_idx = st.number_input("馬場指数", -50, 50, 0, step=1)
        analysis_bias_val = st.slider("馬場バイアス (内有利 -1.0 ↔ 外有利 +1.0)", -1.0, 1.0, 0.0, step=0.1)
        analysis_track_week = st.number_input("開催週 (例: 1, 8)", 1, 12, 1)

    col_analysis1, col_analysis2 = st.columns(2)
    
    with col_analysis1: 
        st.markdown("##### 🏁 レースラップ入力")
        analysis_lap_input = st.text_area("JRAレースラップ (例: 12.5-11.0-12.0...)", height=150)
        
        calc_f3f_val = 0.0
        calc_l3f_val = 0.0
        calc_pace_status = "ミドルペース"
        calc_pace_diff = 0.0
        
        if analysis_lap_input:
            found_laps = [float(x) for x in re.findall(r'\d+\.\d', analysis_lap_input)]
            if len(found_laps) >= 3:
                calc_f3f_val = sum(found_laps[:3])
                calc_l3f_val = sum(found_laps[-3:])
                calc_pace_diff = calc_f3f_val - calc_l3f_val
                
                # 距離別の動的ペースしきい値
                pace_threshold = 1.0 * (analysis_dist / 1600.0)
                
                if calc_pace_diff < -pace_threshold:
                    calc_pace_status = "ハイペース"
                elif calc_pace_diff > pace_threshold:
                    calc_pace_status = "スローペース"
                    
                st.success(f"ラップ解析完了: 前3F {calc_f3f_val:.1f} / 後3F {calc_l3f_val:.1f} ({calc_pace_status})")
        
        analysis_final_l3f = st.number_input("レース上がり3F (自動計算から修正可)", 0.0, 60.0, calc_l3f_val, step=0.1)

    with col_analysis2: 
        st.markdown("##### 🐎 成績表貼り付け")
        analysis_raw_input = st.text_area("JRA公式サイトの成績表をそのまま貼り付けてください", height=250)

    # 🌟 【完全復元】解析斤量プレビュー
    if analysis_raw_input and calc_f3f_val > 0:
        st.markdown("##### ⚖️ 解析プレビュー（斤量の確認・修正）")
        input_lines = [l.strip() for l in analysis_raw_input.split('\n') if len(l.strip()) > 15]
        
        list_for_preview = []
        for line in input_lines:
            match_name = re.findall(r'([ァ-ヶー]{2,})', line)
            if not match_name:
                continue
                
            # 斤量の抽出
            match_weight = re.search(r'\s([4-6]\d\.\d)\s', line)
            auto_extracted_w = float(match_weight.group(1)) if match_weight else 56.0
            
            list_for_preview.append({
                "馬名": match_name[0], 
                "斤量": auto_extracted_w, 
                "raw_line": line
            })
        
        # 詳細なエディタ表示
        df_editor_preview = st.data_editor(
            pd.DataFrame(list_for_preview), 
            use_container_width=True, 
            hide_index=True
        )

        if st.button("🚀 この内容で解析を実行してデータベースへ保存"):
            if not analysis_race_name:
                st.error("レース名を入力してください。")
            else:
                parsed_list = []
                for idx_pre, row_pre in df_editor_preview.iterrows():
                    current_line_text = row_pre["raw_line"]
                    
                    time_match_main = re.search(r'(\d{1,2}:\d{2}\.\d)', current_line_text)
                    if not time_match_main:
                        continue
                    
                    # 着順の取得
                    match_pos_rank = re.match(r'^(\d{1,2})', current_line_text)
                    res_pos_val = int(match_pos_rank.group(1)) if match_pos_rank else 99
                    
                    # 4角通過順位の詳細取得
                    string_after_time = current_line_text[time_match_main.end():]
                    list_of_positions = re.findall(r'\b([1-2]?\d)\b', string_after_time)
                    final_four_c_pos = 7.0 
                    
                    if list_of_positions:
                        valid_pos_collected = []
                        for p_str in list_of_positions:
                            p_num = int(p_str)
                            if p_num > 30: # 異常値（馬体重等）の混入防止
                                if len(valid_pos_collected) > 0:
                                    break
                            valid_pos_collected.append(float(p_num))
                        
                        if valid_pos_collected:
                            final_four_c_pos = valid_pos_collected[-1]
                    
                    parsed_list.append({
                        "line": current_line_text, 
                        "res_pos": res_pos_val, 
                        "four_c_pos": final_four_c_pos, 
                        "name": row_pre["馬名"], 
                        "weight": row_pre["斤量"]
                    })
                
                # --- バイアス判定ロジック（4着補充特例を完全展開） ---
                top_3_parsed = sorted(
                    [d for d in parsed_list if d["res_pos"] <= 3], 
                    key=lambda x: x["res_pos"]
                )
                
                # 10番手以下、あるいは3番手以内の極端な馬
                bias_outliers = [
                    d for d in top_3_parsed 
                    if d["four_c_pos"] >= 10.0 or d["four_c_pos"] <= 3.0
                ]
                
                if len(bias_outliers) == 1:
                    # 1頭のみ極端なケース：その馬を除き、4着を補充
                    bias_base_entries = [d for d in top_3_parsed if d != bias_outliers[0]]
                    fourth_horse = [d for d in parsed_list if d["res_pos"] == 4]
                    final_bias_set = bias_base_entries + fourth_horse
                else:
                    # それ以外：通常の上位3頭
                    final_bias_set = top_3_parsed
                
                if final_bias_set:
                    avg_c4_pos = sum(d["four_c_pos"] for d in final_bias_set) / len(final_bias_set)
                else:
                    avg_c4_pos = 7.0
                    
                if avg_c4_pos <= 4.0:
                    determined_bias_type = "前有利"
                elif avg_c4_pos >= 10.0:
                    determined_bias_type = "後有利"
                else:
                    determined_bias_type = "フラット"
                
                # 最大出走頭数の特定
                max_field_size = max([d["res_pos"] for d in parsed_list]) if parsed_list else 16

                final_new_rows = []
                for entry_p in parsed_list:
                    p_line = entry_p["line"]
                    p_last_pos = entry_p["four_c_pos"]
                    p_res_pos = entry_p["res_pos"]
                    p_weight = entry_p["weight"] 
                    
                    # タイム計算
                    t_match = re.search(r'(\d{1,2}:\d{2}\.\d)', p_line)
                    t_str = t_match.group(1)
                    m_val, s_val = map(float, t_str.split(':'))
                    total_seconds = m_val * 60 + s_val
                    
                    # 馬体重詳細
                    match_h_weight = re.search(r'(\d{3})kg', p_line)
                    string_h_weight = f"({match_h_weight.group(1)}kg)" if match_h_weight else ""

                    # 個別上がり
                    p_l3f_indiv = 0.0
                    match_l3f_bracket = re.search(r'(\d{2}\.\d)\s*\d{3}\(', p_line)
                    if match_l3f_bracket:
                        p_l3f_indiv = float(match_l3f_bracket.group(1))
                    else:
                        found_decimals = re.findall(r'(\d{2}\.\d)', p_line)
                        for d_val in found_decimals:
                            dv = float(d_val)
                            if 30.0 <= dv <= 46.0:
                                if abs(dv - p_weight) > 0.5:
                                    p_l3f_indiv = dv
                                    break
                    if p_l3f_indiv == 0.0:
                        p_l3f_indiv = analysis_final_l3f 
                    
                    # --- 【完全復元】頭数・非線形負荷スコアリング ---
                    relative_pos_ratio = p_last_pos / max_field_size
                    # 16頭基準の強度補正
                    intensity_coeff = max_field_size / 16.0
                    
                    computed_load_score = 0.0
                    if calc_pace_status == "ハイペース":
                        if determined_bias_type != "前有利":
                            load_val = (0.6 - relative_pos_ratio) * abs(pace_diff) * 3.0
                            computed_load_score += max(0.0, load_val) * intensity_coeff
                            
                    elif calc_pace_status == "スローペース":
                        if determined_bias_type != "後有利":
                            load_val = (relative_pos_ratio - 0.4) * abs(pace_diff) * 2.0
                            computed_load_score += max(0.0, load_val) * intensity_coeff
                    
                    # 逆行タグ詳細
                    tag_list = []
                    is_counter_flag = False
                    
                    if p_res_pos <= 5:
                        # バイアス逆行
                        if determined_bias_type == "前有利":
                            if p_last_pos >= 10.0:
                                t = "💎💎 ﾊﾞｲｱｽ極限逆行" if max_field_size >= 16 else "💎 ﾊﾞｲｱｽ逆行"
                                tag_list.append(t)
                                is_counter_flag = True
                        elif determined_bias_type == "後有利":
                            if p_last_pos <= 3.0:
                                t = "💎💎 ﾊﾞｲｱｽ極限逆行" if max_field_size >= 16 else "💎 ﾊﾞｲｱｽ逆行"
                                tag_list.append(t)
                                is_counter_flag = True
                                
                    # 展開逆行
                    favored_pace_bias = False
                    if calc_pace_status == "ハイペース":
                        if determined_bias_type == "前有利":
                            favored_pace_bias = True
                    elif calc_pace_status == "スローペース":
                        if determined_bias_type == "後有利":
                            favored_pace_bias = True
                            
                    if favored_pace_bias == False:
                        if calc_pace_status == "ハイペース":
                            if p_last_pos <= 3.0:
                                tag_list.append("📉 激流被害" if max_field_size >= 14 else "🔥 展開逆行")
                                is_counter_target_flag = True
                                is_counter_flag = True
                        elif calc_pace_status == "スローペース":
                            if p_last_pos >= 10.0:
                                if (calc_f3f_val - p_l3f_indiv) > 1.5:
                                    tag_list.append("🔥 展開逆行")
                                    is_counter_flag = True
                    
                    # 展開恩恵（少頭数）
                    if max_field_size <= 10:
                        if calc_pace_status == "スローペース":
                            if p_res_pos <= 2:
                                tag_list.append("🟢 展開恩恵")

                    # 上がり評価
                    l3f_diff_val = analysis_final_l3f - p_l3f_indiv
                    if l3f_diff_val >= 0.5:
                        tag_list.append("🚀 アガリ優秀")
                    elif l3f_diff_val <= -1.0:
                        tag_list.append("📉 失速大")
                    
                    # 中盤ラップ解析詳細
                    mid_label = "平"
                    if analysis_dist > 1200:
                        m_lap_calc = (total_seconds - calc_f3f_val - p_l3f_indiv) / ((analysis_dist - 1200) / 200)
                        if m_lap_calc >= 12.8:
                            mid_label = "緩"
                        elif m_lap_calc <= 11.8:
                            mid_label = "締"
                    else:
                        mid_label = "短"

                    field_size_tag = "多" if max_field_size >= 16 else "少" if max_field_size <= 10 else "中"
                    combined_memo = f"【{calc_pace_status}/{determined_bias_type}/負荷:{computed_load_score:.1f}({field_size_tag})/{mid_label}】{'/'.join(tag_list) if tag_list else '順境'}"
                    
                    # 指数計算
                    week_offset = (analysis_track_week - 1) * 0.05
                    water_avg = (analysis_water_4c + analysis_water_goal) / 2.0
                    
                    # 🌟 RTC指数の完全計算式
                    computed_rtc = (total_seconds - (p_weight - 56.0) * 0.1 - analysis_track_idx / 10.0 - computed_load_score / 10.0 - week_offset) + analysis_bias_val - (water_avg - 10.0) * 0.05 - (9.5 - analysis_cushion) * 0.1 + (analysis_dist - 1600) * 0.0005
                    
                    final_new_rows.append({
                        "name": entry_p["name"], 
                        "base_rtc": computed_rtc, 
                        "last_race": analysis_race_name, 
                        "course": analysis_course_name, 
                        "dist": analysis_dist, 
                        "notes": f"{p_weight}kg{string_h_weight}", 
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"), 
                        "f3f": calc_f3f_val, 
                        "l3f": p_l3f_indiv, 
                        "race_l3f": analysis_final_l3f, 
                        "load": p_last_pos, 
                        "memo": combined_memo,
                        "date": analysis_race_date.strftime("%Y-%m-%d"), 
                        "cushion": analysis_cushion, 
                        "water": water_avg, 
                        "next_buy_flag": "★逆行狙い" if is_counter_flag else "", 
                        "result_pos": p_res_pos
                    })
                
                if final_new_rows:
                    # 🌟 同期不全解消：保存直前にキャッシュを破棄して最新シートを読み直す
                    st.cache_data.clear()
                    current_sheet_df = conn.read(ttl=0)
                    
                    # 読み込んだデータのカラムをチェックして正規化
                    for c_name in all_cols:
                        if c_name not in current_sheet_df.columns:
                            current_sheet_df[c_name] = None
                            
                    combined_final_df = pd.concat([current_sheet_df, pd.DataFrame(final_new_rows)], ignore_index=True)
                    
                    if safe_update(combined_final_df):
                        st.success(f"✅ 解析完了！{len(final_new_rows)}頭の最新データをDBに保存し、同期しました。")
                        st.rerun()

# ==============================================================================
# 8. Tab 2: 馬別履歴
# ==============================================================================
with tab2:
    st.header("📊 馬別履歴 & 買い条件設定")
    df_tab2 = get_db_data()
    if not df_tab2.empty:
        col_t2_1, col_t2_2 = st.columns([1, 1])
        with col_t2_1:
            search_horse_name = st.text_input("馬名で絞り込み検索", key="search_horse_input_t2")
        
        unique_horse_list = sorted([str(x) for x in df_tab2['name'].dropna().unique()])
        with col_t2_2:
            target_horse_edit = st.selectbox("個別メモ・条件編集対象", ["未選択"] + unique_horse_list)
        
        if target_horse_edit != "未選択":
            idx_list = df_tab2[df_tab2['name'] == target_horse_edit].index
            target_idx = idx_list[-1]
            
            with st.form("edit_horse_form_tab2"):
                current_m = df_tab2.at[target_idx, 'memo'] if not pd.isna(df_tab2.at[target_idx, 'memo']) else ""
                new_m = st.text_area("メモ・評価", value=current_m)
                
                current_f = df_tab2.at[target_idx, 'next_buy_flag'] if not pd.isna(df_tab2.at[target_idx, 'next_buy_flag']) else ""
                new_f = st.text_input("個別買いフラグ", value=current_f)
                
                if st.form_submit_button("設定保存"):
                    df_tab2.at[target_idx, 'memo'] = new_m
                    df_tab2.at[target_idx, 'next_buy_flag'] = new_f
                    if safe_update(df_tab2):
                        st.success(f"{target_horse_edit} を更新しました")
                        st.rerun()
        
        if search_horse_name:
            df_tab2_display = df_tab2[df_tab2['name'].str.contains(search_horse_name, na=False)]
        else:
            df_tab2_display = df_tab2
            
        df_tab2_formatted = df_tab2_display.copy()
        df_tab2_formatted['base_rtc'] = df_tab2_formatted['base_rtc'].apply(format_time)
        st.dataframe(
            df_tab2_formatted.sort_values("date", ascending=False)[["date", "name", "last_race", "base_rtc", "f3f", "l3f", "race_l3f", "load", "memo", "next_buy_flag"]], 
            use_container_width=True
        )

# ==============================================================================
# 9. Tab 3: レース別履歴
# ==============================================================================
with tab3:
    st.header("🏁 答え合わせ & レース別履歴")
    df_tab3 = get_db_data()
    if not df_tab3.empty:
        race_list_all = sorted([str(x) for x in df_tab3['last_race'].dropna().unique()])
        selected_race_tab3 = st.selectbox("レース選択", race_list_all)
        
        if selected_race_tab3:
            df_race_tab3 = df_tab3[df_tab3['last_race'] == selected_race_tab3].copy()
            with st.form("race_result_form_tab3"):
                st.write(f"【{selected_race_tab3}】の結果を入力")
                for idx_r, row_r in df_race_tab3.iterrows():
                    v_pos = int(row_r['result_pos']) if not pd.isna(row_r['result_pos']) else 0
                    v_pop = int(row_r['result_pop']) if not pd.isna(row_r['result_pop']) else 0
                    
                    c_r1, c_r2 = st.columns(2)
                    with c_r1:
                        df_race_tab3.at[idx_r, 'result_pos'] = st.number_input(f"{row_r['name']} 着順", 0, 100, value=v_pos, key=f"pos_{idx_r}")
                    with c_r2:
                        df_race_tab3.at[idx_r, 'result_pop'] = st.number_input(f"{row_r['name']} 人気", 0, 100, value=v_pop, key=f"pop_{idx_r}")
                
                if st.form_submit_button("結果を一括保存"):
                    for idx_r, row_r in df_race_tab3.iterrows():
                        df_tab3.at[idx_r, 'result_pos'] = row_r['result_pos']
                        df_tab3.at[idx_r, 'result_pop'] = row_r['result_pop']
                    if safe_update(df_tab3):
                        st.success("結果を保存しました。")
                        st.rerun()
            
            df_race_formatted = df_race_tab3.copy()
            df_race_formatted['base_rtc'] = df_race_formatted['base_rtc'].apply(format_time)
            st.dataframe(df_race_formatted[["name", "notes", "base_rtc", "f3f", "l3f", "race_l3f", "result_pos", "result_pop"]], use_container_width=True)

# ==============================================================================
# 10. Tab 4: シミュレーター (個別斤量・過去3走ループ・頭数連動)
# ==============================================================================
with tab4:
    st.header("🎯 次走シミュレーター & 統合評価")
    df_tab4 = get_db_data()
    if not df_tab4.empty:
        horse_names_tab4 = sorted([str(x) for x in df_tab4['name'].dropna().unique()])
        selected_sim_horses = st.multiselect("出走予定馬を選択してください", options=horse_names_tab4)
        
        sim_input_pops = {}
        sim_input_gates = {}
        sim_input_weights = {}
        
        if selected_sim_horses:
            st.markdown("##### 📝 枠番・予想人気・想定斤量の個別入力")
            sim_cols = st.columns(min(len(selected_sim_horses), 4))
            for i, h_name in enumerate(selected_sim_horses):
                with sim_cols[i % 4]:
                    h_latest = df_tab4[df_tab4['name'] == h_name].iloc[-1]
                    sim_input_gates[h_name] = st.number_input(f"{h_name} 枠", 1, 18, value=1, key=f"sg_{h_name}")
                    sim_input_pops[h_name] = st.number_input(f"{h_name} 人気", 1, 18, value=int(h_latest['result_pop']) if not pd.isna(h_latest['result_pop']) else 10, key=f"sp_{h_name}")
                    # 個別斤量入力
                    sim_input_weights[h_name] = st.number_input(f"{h_name} 斤量", 48.0, 62.0, 56.0, step=0.5, key=f"sw_{h_name}")

            col_sim1, col_sim2 = st.columns(2)
            with col_sim1: 
                sim_course = st.selectbox("次走競馬場", list(COURSE_DATA.keys()), key="sc_select")
                sim_dist = st.selectbox("距離 (m)", list(range(1000, 3700, 100)), index=6)
                sim_track = st.radio("次走トラック", ["芝", "ダート"], horizontal=True)
            with col_sim2: 
                sim_cushion = st.slider("想定クッション値", 7.0, 12.0, 9.5)
                sim_water = st.slider("想定含水率 (%)", 0.0, 30.0, 10.0)
            
            if st.button("🏁 シミュレーション実行"):
                results_sim = []
                num_total_sim = len(selected_sim_horses)
                styles_sim = {"逃げ": 0, "先行": 0, "差し": 0, "追込": 0}
                avg_l3f_db = df_tab4['l3f'].mean()

                for h_name in selected_sim_horses:
                    h_history = df_tab4[df_tab4['name'] == h_name].sort_values("date")
                    last_3_runs = h_history.tail(3)
                    conv_rtc_list = []
                    
                    # 脚質判定詳細
                    avg_load_3r = last_3_runs['load'].mean()
                    if avg_load_3r <= 3.5: 
                        h_style = "逃げ"
                    elif avg_load_3r <= 7.0: 
                        h_style = "先行"
                    elif avg_load_3r <= 11.0: 
                        h_style = "差し"
                    else: 
                        h_style = "追込"
                    styles_sim[h_style] += 1

                    # 渋滞リスク
                    jam_label = "⚠️詰まり注意" if num_total_sim >= 15 and h_style in ["差し", "追込"] and sim_input_gates[h_name] <= 4 else "-"
                    # スロー適性
                    slow_label = "-"
                    if num_total_sim <= 10:
                        h_min_l3f = h_history['l3f'].min()
                        if h_min_l3f < avg_l3f_db - 0.5:
                            slow_label = "⚡スロー特化"
                        elif h_min_l3f > avg_l3f_db + 0.5:
                            slow_label = "📉瞬発力不足"

                    h_std = h_history['base_rtc'].std() if len(h_history) >= 3 else 0.0
                    h_stab = "⚖️安定" if 0 < h_std < 0.2 else "🎢ムラ" if h_std > 0.4 else "-"
                    
                    h_best_past = h_history.loc[h_history['base_rtc'].idxmin()]
                    h_apt = "🎯馬場◎" if abs(h_best_past['cushion'] - sim_cushion) <= 0.5 and abs(h_best_past['water'] - sim_water) <= 2.0 else "-"

                    # 🌟 【完全復旧】過去3走すべての斤量個別計算ループ
                    for i_r, row_r in last_3_runs.iterrows():
                        p_dist_r = row_r['dist']
                        p_rtc_r = row_r['base_rtc']
                        p_course_r = row_r['course']
                        p_load_r = row_r['load']
                        p_notes_r = str(row_r['notes'])
                        
                        p_w_r = 56.0
                        h_bw_r = 480.0
                        
                        w_match_r = re.search(r'([4-6]\d\.\d)', p_notes_r)
                        if w_match_r:
                            p_w_r = float(w_match_r.group(1))
                            
                        hb_match_r = re.search(r'\((\d{3})kg\)', p_notes_r)
                        if hb_match_r:
                            h_bw_r = float(hb_match_r.group(1))
                        
                        if p_dist_r > 0:
                            l_adj = (p_load_r - 7.0) * 0.02
                            # 斤量感応度
                            sens_f = 0.15 if h_bw_r <= 440 else 0.08 if h_bw_r >= 500 else 0.1
                            w_diff = (sim_input_weights[h_name] - p_w_r) * sens_f
                            
                            # 指数変換
                            b_conv = (p_rtc_r + l_adj + w_diff) / p_dist_r * sim_dist
                            # 坂補正
                            s_adj = (SLOPE_FACTORS.get(sim_course, 0.002) - SLOPE_FACTORS.get(p_course_r, 0.002)) * sim_dist
                            conv_rtc_list.append(b_conv + s_adj)
                    
                    avg_rtc_res = sum(conv_rtc_list) / len(conv_rtc_list) if conv_rtc_list else 0
                    
                    # 距離実績補正
                    h_best_d = h_history.loc[h_history['base_rtc'].idxmin(), 'dist']
                    avg_rtc_res += (abs(sim_dist - h_best_d) / 100) * 0.05
                    
                    # モメンタム
                    h_mom = "-"
                    if len(h_history) >= 2:
                        if h_history.iloc[-1]['base_rtc'] < h_history.iloc[-2]['base_rtc'] - 0.2:
                            h_mom = "📈上昇"
                            avg_rtc_res -= 0.15

                    # 枠×バイアス
                    syn_bias = -0.2 if (sim_input_gates[h_name] <= 4 and analysis_bias_val <= -0.5) or (sim_input_gates[h_name] >= 13 and analysis_bias_val >= 0.5) else 0
                    avg_rtc_res += syn_bias

                    # コース実績
                    h_c_bonus = -0.2 if any((h_history['course'] == sim_course) & (h_history['result_pos'] <= 3)) else 0.0
                    
                    # 最終アジャスト
                    w_adj_f = (sim_water - 10.0) * 0.05
                    c_dict_f = DIRT_COURSE_DATA if sim_track == "ダート" else COURSE_DATA
                    if sim_track == "ダート":
                        w_adj_f = -w_adj_f
                    
                    final_rtc_sim = (avg_rtc_res + (c_dict_f[sim_course] * (sim_dist/1600.0)) + h_c_bonus + w_adj_f - (9.5 - sim_cushion) * 0.1)
                    
                    h_lat = last_3_runs.iloc[-1]
                    results_sim.append({
                        "馬名": h_name, 
                        "脚質": h_style, 
                        "想定タイム": final_rtc_sim, 
                        "渋滞": jam_label, 
                        "スロー": slow_label, 
                        "適性": h_apt, 
                        "安定": h_stab, 
                        "偏差": "⤴️覚醒期待" if final_rtc_sim < h_history['base_rtc'].min() - 0.3 else "-", 
                        "上昇": h_mom, 
                        "レベル": "🔥強ﾒﾝﾂ" if df_tab4[df_tab4['last_race'] == h_lat['last_race']]['base_rtc'].mean() < df_tab4['base_rtc'].mean() - 0.2 else "-", 
                        "load": h_lat['load'], 
                        "状態": "💤休み明け" if (datetime.now() - h_lat['date']).days // 7 >= 12 else "-", 
                        "raw_rtc": final_rtc_sim, 
                        "解析メモ": h_lat['memo']
                    })
                
                # 展開予想
                pred_pace = "ミドルペース"
                if styles_sim["逃げ"] >= 2 or (styles_sim["逃げ"] + styles_sim["先行"]) >= num_total_sim * 0.6:
                    pred_pace = "ハイペース傾向"
                elif styles_sim["逃げ"] == 0 and styles_sim["先行"] <= 1:
                    pred_pace = "スローペース傾向"
                
                df_sim_final = pd.DataFrame(results_sim)
                # 展開シナジー強化
                sim_p_multiplier = 1.5 if num_total_sim >= 15 else 1.0
                
                def apply_synergy_sim(row):
                    adj = 0.0
                    if "ハイ" in pred_pace:
                        if row['脚質'] in ["差し", "追込"]: adj = -0.2 * sim_p_multiplier
                        elif row['脚質'] == "逃げ": adj = 0.2 * sim_p_multiplier
                    elif "スロー" in pred_pace:
                        if row['脚質'] in ["逃げ", "先行"]: adj = -0.2 * sim_p_multiplier
                        elif row['脚質'] in ["差し", "追込"]: adj = 0.2 * sim_p_multiplier
                    return row['raw_rtc'] + adj

                df_sim_final['synergy_rtc'] = df_sim_final.apply(apply_synergy_sim, axis=1)
                df_sim_final = df_sim_final.sort_values("synergy_rtc")
                df_sim_final['RTC順位'] = range(1, len(df_sim_final) + 1)
                
                top_t_sim = df_sim_final.iloc[0]['raw_rtc']
                df_sim_final['差'] = df_sim_final['raw_rtc'] - top_t_sim
                df_sim_final['予想人気'] = df_sim_final['馬名'].map(sim_input_pops)
                df_sim_final['妙味スコア'] = df_sim_final['予想人気'] - df_sim_final['RTC順位']
                
                df_sim_final['役割'] = "-"
                df_sim_final.loc[df_sim_final['RTC順位'] == 1, '役割'] = "◎"
                df_sim_final.loc[df_sim_final['RTC順位'] == 2, '役割'] = "〇"
                df_sim_final.loc[df_sim_final['RTC順位'] == 3, '役割'] = "▲"
                pb_sim = df_sim_final[df_sim_final['RTC順位'] > 1].sort_values("妙味スコア", ascending=False)
                if not pb_sim.empty:
                    df_sim_final.loc[df_sim_final['馬名'] == pb_sim.iloc[0]['馬名'], '役割'] = "★"
                
                df_sim_final['想定タイム'] = df_sim_final['raw_rtc'].apply(format_time)
                df_sim_final['差'] = df_sim_final['差'].apply(lambda x: f"+{x:.1f}" if x > 0 else "±0.0")

                st.markdown("---")
                st.subheader(f"🏁 展開予想：{pred_pace} ({num_total_sim}頭立て)")
                col_rec1, col_rec2 = st.columns(2)
                
                fav_h = df_sim_final[df_sim_final['役割'] == "◎"].iloc[0]['馬名'] if not df_sim_final[df_sim_final['役割'] == "◎"].empty else ""
                opp_h = df_sim_final[df_sim_final['役割'] == "〇"].iloc[0]['馬名'] if not df_sim_final[df_sim_final['役割'] == "〇"].empty else ""
                bomb_h = df_sim_final[df_sim_final['役割'] == "★"].iloc[0]['馬名'] if not df_sim_final[df_sim_final['役割'] == "★"].empty else ""
                
                with col_rec1:
                    st.info(f"**🎯 1点勝負**\n\n◎ {fav_h} － 〇 {opp_h}")
                with col_rec2: 
                    if bomb_h:
                        st.warning(f"**💣 妙味狙い**\n\n◎ {fav_h} － ★ {bomb_h}")
                
                def highlight_sim(row):
                    if row['役割'] == "★":
                        return ['background-color: #ffe4e1; font-weight: bold'] * len(row)
                    if row['役割'] == "◎":
                        return ['background-color: #fff700; font-weight: bold; color: black'] * len(row)
                    return [''] * len(row)
                
                st.table(df_sim_final[["役割", "馬名", "脚質", "渋滞", "スロー", "想定タイム", "差", "妙味スコア", "適性", "安定", "上昇", "レベル", "偏差", "load", "状態", "解析メモ"]].style.apply(highlight_sim, axis=1))

# ==============================================================================
# 11. Tab 5: トレンド
# ==============================================================================
with tab5:
    st.header("📈 馬場トレンド & 統計解析")
    df_tab5 = get_db_data()
    if not df_tab5.empty:
        tc_sel = st.selectbox("トレンド競馬場", list(COURSE_DATA.keys()), key="tc_sel_tab5")
        tdf_tab5 = df_tab5[df_tab5['course'] == tc_sel].sort_values("date")
        if not tdf_tab5.empty:
            st.subheader("💧 コンディション推移"); st.line_chart(tdf_tab5.set_index("date")[["cushion", "water"]])
            st.subheader("🏁 レース傾向"); st.bar_chart(tdf_tab5.groupby('last_race').agg({'load':'mean', 'date':'max'}).sort_values('date', ascending=False).head(15)['load'])

# ==============================================================================
# 12. Tab 6: データ管理 (手動修正同期・再解析・一括削除)
# ==============================================================================
with tab6:
    st.header("🗑 データベース保守 & 管理")
    
    # 🌟 【指示反映】同期不全解消・手動修正反映用ボタン
    if st.button("🔄 スプレッドシートの手動修正を同期（キャッシュ破棄）"):
        st.cache_data.clear()
        st.success("キャッシュを完全に破棄しました。最新のスプレッドシート内容を読み込みます。")
        st.rerun()

    db_df_tab6 = get_db_data()

    def update_eval_tags_full_logic_verbose(row, df_context=None):
        """【完全復元】冗長な条件分岐による再解析用詳細ロジック"""
        m_raw = str(row['memo']) if not pd.isna(row['memo']) else ""
        
        def to_f_safe(v):
            try: return float(v) if not pd.isna(v) else 0.0
            except: return 0.0
            
        f3_v, l3_v, rl3_v, pos_v, l_pos_v, d_v, rtc_v = map(to_f_safe, [
            row['f3f'], row['l3f'], row['race_l3f'], row['result_pos'], row['load'], row['dist'], row['base_rtc']
        ])
        
        # 🌟 斤量をnotesから再抽出（手動修正反映の要）
        n_str = str(row['notes'])
        w_match_v = re.search(r'([4-6]\d\.\d)', n_str)
        indiv_w_v = float(w_match_v.group(1)) if w_match_v else 56.0
        
        # 中盤ラップ
        mid_note_v = "平"
        if d_v > 1200 and f3_v > 0:
            m_lap_v = (rtc_v - f3_v - l3_v) / ((d_v - 1200) / 200)
            if m_lap_v >= 12.8: mid_note_v = "緩"
            elif m_lap_v <= 11.8: mid_note_v = "締"
        elif d_v <= 1200:
            mid_note_v = "短"

        # バイアス判定完全再現
        b_type_v = "フラット"; max_r_v = 16
        if df_context is not None and not pd.isna(row['last_race']):
            r_ctx = df_context[df_context['last_race'] == row['last_race']]
            max_r_v = r_ctx['result_pos'].max() if not r_ctx.empty else 16
            top3_v = r_ctx[pd.to_numeric(r_ctx['result_pos'], errors='coerce') <= 3].copy()
            top3_v['load'] = pd.to_numeric(top3_v['load'], errors='coerce').fillna(7.0)
            
            out_v = top3_v[(top3_v['load'] >= 10.0) | (top3_v['load'] <= 3.0)]
            if len(out_v) == 1:
                b_set_v = pd.concat([
                    top3_v[top3_v['name'] != out_v.iloc[0]['name']], 
                    r_ctx[pd.to_numeric(r_ctx['result_pos'], errors='coerce') == 4]
                ])
            else:
                b_set_v = top3_v
            
            if not b_set_v.empty:
                avg_v = b_set_v['load'].mean()
                if avg_v <= 4.0: b_type_v = "前有利"
                elif avg_v >= 10.0: b_type_v = "後有利"

        # ペース・強度補正
        p_status_v = "ハイペース" if "ハイ" in m_raw else "スローペース" if "スロー" in m_raw else "ミドルペース"
        p_diff_v = 1.5 if p_status_v != "ミドルペース" else 0.0
        rel_p_v = l_pos_v / max_r_v
        f_int_v = max_r_v / 16.0
        
        new_l_score_v = 0.0
        if p_status_v == "ハイペース" and b_type_v != "前有利":
            new_l_score_v = max(0, (0.6 - rel_p_v) * p_diff_v * 3.0) * f_int_v
        elif p_status_v == "スローペース" and b_type_v != "後有利":
            new_l_score_v = max(0, (rel_p_v - 0.4) * p_diff_v * 2.0) * f_int_v
        
        t_list_v = []
        is_c_v = False
        if rl3_v > 0:
            if (rl3_v - l3_v) >= 0.5: t_list_v.append("🚀 アガリ優秀")
            elif (rl3_v - l3_v) <= -1.0: t_list_v.append("📉 失速大")
            
        if pos_v <= 5:
            if b_type_v == "前有利" and l_pos_v >= 10.0:
                t_list_v.append("💎💎 ﾊﾞｲｱｽ極限逆行" if max_r_v >= 16 else "💎 ﾊﾞｲｱｽ逆行")
                is_c_v = True
            elif b_type_v == "後有利" and l_pos_v <= 3.0:
                t_list_v.append("💎💎 ﾊﾞｲｱｽ極限逆行" if max_r_v >= 16 else "💎 ﾊﾞｲｱｽ逆行")
                is_c_v = True
            
            # 展開逆行
            if p_status_v == "ハイペース" and b_type_v != "前有利" and l_pos_v <= 3.0:
                t_list_v.append("📉 激流被害" if max_r_v >= 14 else "🔥 展開逆行")
                is_c_v = True
            elif p_status_v == "スローペース" and b_type_v != "後有利" and l_pos_v >= 10.0:
                if (f3_v - l3_v) > 1.5:
                    t_list_v.append("🔥 展開逆行")
                    is_c_v = True
        
        if max_r_v <= 10 and p_status_v == "スローペース" and pos_v <= 2:
            t_list_v.append("🟢 展開恩恵")

        field_tag_v = "多" if max_r_v >= 16 else "少" if max_r_v <= 10 else "中"
        memo_upd = (f"【{p_status_v}/{b_type_v}/負荷:{new_l_score_v:.1f}({field_tag_v})/{mid_note_v}】" + "/".join(t_list_v)).strip("/")
        flag_upd = ("★逆行狙い " + str(row['next_buy_flag']).replace("★逆行狙い", "")).strip() if is_c_v else str(row['next_buy_flag']).replace("★逆行狙い", "").strip()
        
        return memo_upd, flag_upd

    # 開催週補正セクション
    st.subheader("🗓 過去レースの開催週を一括設定")
    if not db_df_tab6.empty:
        rm_weeks = db_df_tab6[['last_race', 'date']].drop_duplicates(subset=['last_race']).copy()
        rm_weeks['track_week'] = 1
        ew_df = st.data_editor(rm_weeks, hide_index=True)
        if st.button("🔄 補正&再解析を一括適用"):
            w_dict = dict(zip(ew_df['last_race'], ew_df['track_week']))
            for idx_w, row_w in db_df_tab6.iterrows():
                if row_w['last_race'] in w_dict:
                    db_df_tab6.at[idx_w, 'base_rtc'] = row_w['base_rtc'] - (w_dict[row_w['last_race']] - 1) * 0.05
                    m_re, f_re = update_eval_tags_full_logic_verbose(db_df_tab6.iloc[idx_w], db_df_tab6)
                    db_df_tab6.at[idx_w, 'memo'] = m_re
                    db_df_tab6.at[idx_w, 'next_buy_flag'] = f_re
            if safe_update(db_df_tab6):
                st.success("開催週補正と再計算を完了しました。"); st.rerun()

    st.subheader("🛠️ 一括処理メニュー")
    col_adm1, col_adm2 = st.columns(2)
    with col_adm1:
        if st.button("🔄 DB再解析（最新数値を基に上書き）"):
            # 🌟 【指示反映】同期不全解消・手動修正反映の核心
            st.cache_data.clear()
            latest_db = conn.read(ttl=0)
            # カラム正規化
            for c in all_cols:
                if c not in latest_db.columns: latest_db[c] = None
            
            for idx, row in latest_db.iterrows():
                m_upd, f_upd = update_eval_tags_full_logic_verbose(row, latest_db)
                latest_db.at[idx, 'memo'] = m_upd
                latest_db.at[idx, 'next_buy_flag'] = f_upd
            
            if safe_update(latest_db):
                st.success("スプレッドシートの手動修正を基に、全解析メモを更新・同期しました。")
                st.rerun()
    with col_adm2:
        if st.button("🧼 重複削除"):
            b_cnt = len(db_df_tab6)
            db_df_tab6 = db_df_tab6.drop_duplicates(subset=['name', 'date', 'last_race'], keep='first')
            if safe_update(db_df_tab6):
                st.success(f"{b_cnt - len(db_df_tab6)}件の重複を削除しました。"); st.rerun()

    if not db_df_tab6.empty:
        st.subheader("🛠️ データ編集エディタ")
        edf_tab6 = db_df_tab6.copy()
        edf_tab6['base_rtc'] = edf_tab6['base_rtc'].apply(format_time)
        edited_db_final = st.data_editor(edf_tab6.sort_values("date", ascending=False), num_rows="dynamic", use_container_width=True)
        if st.button("💾 エディタの変更内容を反映"):
            save_df_final = edited_db_final.copy()
            save_df_final['base_rtc'] = save_df_final['base_rtc'].apply(parse_time_str)
            if safe_update(save_df_final):
                st.success("エディタの内容をDBに反映しました。"); st.rerun()
        
        st.divider()
        st.subheader("❌ データ削除設定")
        col_del1, col_del2 = st.columns(2)
        with col_del1:
            all_r_del = sorted([str(x) for x in db_df_tab6['last_race'].dropna().unique()])
            target_r_del = st.selectbox("削除対象レース", ["未選択"] + all_r_del)
            if target_r_del != "未選択":
                if st.button(f"🚨 レース【{target_r_del}】を全削除"):
                    if safe_update(db_df_tab6[db_df_tab6['last_race'] != target_r_del]): st.rerun()
        with col_del2:
            all_h_del = sorted([str(x) for x in db_df_tab6['name'].dropna().unique()])
            # 🌟 【完全復元】マルチセレクト一括削除
            target_h_del_list = st.multiselect("削除馬選択（複数可）", all_h_del, key="mult_del_final")
            if target_h_del_list:
                if st.button(f"🚨 選択した{len(target_h_del_list)}頭をDBから削除"):
                    if safe_update(db_df_tab6[~db_df_tab6['name'].isin(target_h_del_list)]): st.rerun()

        st.divider()
        with st.expander("☢️ システム初期化"):
            if st.button("🧨 データベースを完全にリセット"):
                if safe_update(pd.DataFrame(columns=db_df_tab6.columns)):
                    st.success("DBを初期化しました。"); st.rerun()
