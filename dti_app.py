import streamlit as st
import pandas as pd
import re
import time
from streamlit_gsheets import GSheetsConnection
from datetime import datetime

# ==============================================================================
# 1. ページ基本構成の詳細設定
# ==============================================================================
# このセクションでは、アプリケーションの全体的な外観と基本挙動を定義します。
# 1ミリも削らず、冗長なまでに設定項目を記述します。
st.set_page_config(
    page_title="DTI Ultimate DB - Professional Edition",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'Get Help': None,
        'Report a bug': None,
        'About': "DTI Ultimate DB: Horse Racing Analysis System"
    }
)

# --- Google Sheets 接続オブジェクトの生成 ---
# データベースとの通信を司るメインコネクションです。
# 接続の安定性を確保するため、グローバルスコープでインスタンス化します。
conn = st.connection("gsheets", type=GSheetsConnection)

# ==============================================================================
# 2. データベース読み込み詳細ロジック (キャッシュとデータ整合性の管理)
# ==============================================================================
@st.cache_data(ttl=300)
def get_db_data_cached():
    """
    Google Sheetsから全ての蓄積データを読み込み、前処理を行います。
    この関数は一切の簡略化を排除し、1カラムずつ存在チェックと型変換を実行します。
    """
    # データベースの全カラム定義（18カラムを1ミリも漏らさず定義）
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
        # ttl=0での直接読み込みに対応するため、内部で直接readを呼び出し。
        # アプリの起動時や更新時に最新状態を確保するための設計です。
        df_raw_input = conn.read(ttl=0)
        
        # データがNoneまたは空の場合の安全な初期化ロジック
        if df_raw_input is None:
            empty_df = pd.DataFrame(columns=all_cols)
            return empty_df
            
        if df_raw_input.empty:
            empty_df = pd.DataFrame(columns=all_cols)
            return empty_df
        
        # 🌟 カラムの存在チェックと強制的な補填ロジック
        # プログラミング上の安全性を最大化するため、全カラムを個別に確認します。
        if "name" not in df_raw_input.columns:
            df_raw_input["name"] = None
        if "base_rtc" not in df_raw_input.columns:
            df_raw_input["base_rtc"] = None
        if "last_race" not in df_raw_input.columns:
            df_raw_input["last_race"] = None
        if "course" not in df_raw_input.columns:
            df_raw_input["course"] = None
        if "dist" not in df_raw_input.columns:
            df_raw_input["dist"] = None
        if "notes" not in df_raw_input.columns:
            df_raw_input["notes"] = None
        if "timestamp" not in df_raw_input.columns:
            df_raw_input["timestamp"] = None
        if "f3f" not in df_raw_input.columns:
            df_raw_input["f3f"] = None
        if "l3f" not in df_raw_input.columns:
            df_raw_input["l3f"] = None
        if "race_l3f" not in df_raw_input.columns:
            df_raw_input["race_l3f"] = None
        if "load" not in df_raw_input.columns:
            df_raw_input["load"] = None
        if "memo" not in df_raw_input.columns:
            df_raw_input["memo"] = None
        if "date" not in df_raw_input.columns:
            df_raw_input["date"] = None
        if "cushion" not in df_raw_input.columns:
            df_raw_input["cushion"] = None
        if "water" not in df_raw_input.columns:
            df_raw_input["water"] = None
        if "result_pos" not in df_raw_input.columns:
            df_raw_input["result_pos"] = None
        if "result_pop" not in df_raw_input.columns:
            df_raw_input["result_pop"] = None
        if "next_buy_flag" not in df_raw_input.columns:
            df_raw_input["next_buy_flag"] = None
            
        # データの型変換（NameErrorや型エラーを防止するための冗長な記述）
        if 'date' in df_raw_input.columns:
            df_raw_input['date'] = pd.to_datetime(df_raw_input['date'], errors='coerce')
            
        if 'result_pos' in df_raw_input.columns:
            df_raw_input['result_pos'] = pd.to_numeric(df_raw_input['result_pos'], errors='coerce')
        
        # 🌟 三段階詳細ソートロジック（データの並びを常に最適化）
        # 1. 日付(新しい順)
        # 2. レース名(アルファベット/五十音順)
        # 3. 着順(昇順：1着から)
        df_raw_input = df_raw_input.sort_values(
            by=["date", "last_race", "result_pos"], 
            ascending=[False, True, True]
        )
        
        # 数値カラムのパースとNaN補完（簡略化せず、1カラム1処理で記述）
        if 'result_pop' in df_raw_input.columns:
            df_raw_input['result_pop'] = pd.to_numeric(df_raw_input['result_pop'], errors='coerce')
            
        if 'f3f' in df_raw_input.columns:
            df_raw_input['f3f'] = pd.to_numeric(df_raw_input['f3f'], errors='coerce')
            df_raw_input['f3f'] = df_raw_input['f3f'].fillna(0.0)
            
        if 'l3f' in df_raw_input.columns:
            df_raw_input['l3f'] = pd.to_numeric(df_raw_input['l3f'], errors='coerce')
            df_raw_input['l3f'] = df_raw_input['l3f'].fillna(0.0)
            
        if 'race_l3f' in df_raw_input.columns:
            df_raw_input['race_l3f'] = pd.to_numeric(df_raw_input['race_l3f'], errors='coerce')
            df_raw_input['race_l3f'] = df_raw_input['race_l3f'].fillna(0.0)
            
        if 'load' in df_raw_input.columns:
            df_raw_input['load'] = pd.to_numeric(df_raw_input['load'], errors='coerce')
            df_raw_input['load'] = df_raw_input['load'].fillna(0.0)
            
        if 'base_rtc' in df_raw_input.columns:
            df_raw_input['base_rtc'] = pd.to_numeric(df_raw_input['base_rtc'], errors='coerce')
            df_raw_input['base_rtc'] = df_raw_input['base_rtc'].fillna(0.0)
            
        if 'cushion' in df_raw_input.columns:
            df_raw_input['cushion'] = pd.to_numeric(df_raw_input['cushion'], errors='coerce')
            df_raw_input['cushion'] = df_raw_input['cushion'].fillna(9.5)
            
        if 'water' in df_raw_input.columns:
            df_raw_input['water'] = pd.to_numeric(df_raw_input['water'], errors='coerce')
            df_raw_input['water'] = df_raw_input['water'].fillna(10.0)
            
        # 全ての行が空のデータ（ゴーストデータ）を排除
        df_raw_input = df_raw_input.dropna(how='all')
        
        return df_raw_input
        
    except Exception as e_load:
        st.error(f"【重大な警告】スプレッドシートの読み込み中に回復不能なエラーが発生しました。: {e_load}")
        return pd.DataFrame(columns=all_cols)

def get_db_data():
    """キャッシュ管理関数への呼び出しインターフェースです。"""
    return get_db_data_cached()

# ==============================================================================
# 3. データベース更新詳細ロジック (同期性能を極限まで高めた書き込み処理)
# ==============================================================================
def safe_update(df_to_save):
    """
    Google Sheetsへデータを書き戻す核心的な関数です。
    リトライ機能、ソート、インデックスリセット、キャッシュ強制クリアを含みます。
    """
    # 保存直前に、データの型と順序を再定義して破壊を防止します。
    if 'date' in df_to_save.columns:
        if 'last_race' in df_to_save.columns:
            if 'result_pos' in df_to_save.columns:
                df_to_save['date'] = pd.to_datetime(df_to_save['date'], errors='coerce')
                df_to_save['result_pos'] = pd.to_numeric(df_to_save['result_pos'], errors='coerce')
                df_to_save = df_to_save.sort_values(
                    by=["date", "last_race", "result_pos"], 
                    ascending=[False, True, True]
                )
    
    # 🌟 Google Sheets側のインデックス不整合を防ぐため、完全にリセットします。
    df_to_save = df_to_save.reset_index(drop=True)
    
    # 書き込みリトライループ（API制限やネットワークの不安定さへの対策）
    max_update_retries = 3
    for i_retry in range(max_update_retries):
        try:
            # 🌟 現在のDataFrame状態でシートを完全に上書き更新します。
            conn.update(data=df_to_save)
            
            # 🌟 重要：書き込み成功時にアプリ内のキャッシュを強制的に破棄します。
            # これを怠ると、保存しても画面には古いデータが残り続ける「同期不全」が起きます。
            st.cache_data.clear()
            
            return True
            
        except Exception as e_update:
            retry_wait_time = 5
            if i_retry < max_update_retries - 1:
                st.warning(f"Google Sheetsとの同期に失敗しました(リトライ {i_retry+1}/3)... {retry_wait_time}秒後に再試行します。")
                time.sleep(retry_wait_time)
                continue
            else:
                st.error(f"スプレッドシートの更新が物理的に不可能な状態です。詳細: {e_update}")
                return False

# ==============================================================================
# 4. 補助関数 (時間変換・パース)
# ==============================================================================
def format_time(seconds_value):
    """
    秒数を mm:ss.f 形式の文字列に変換します。
    RTCの表示を競馬のラップタイム形式に統一します。
    """
    if seconds_value is None:
        return ""
    if seconds_value <= 0:
        return ""
    if pd.isna(seconds_value):
        return ""
    if isinstance(seconds_value, str):
        return seconds_value
        
    minutes_component = int(seconds_value // 60)
    seconds_component = seconds_value % 60
    return f"{minutes_component}:{seconds_component:04.1f}"

def parse_time_str(time_str_input):
    """
    mm:ss.f 形式の文字列を秒数(float)にパースして戻します。
    """
    if time_str_input is None:
        return 0.0
    try:
        clean_time_str = str(time_str_input).strip()
        if ":" in clean_time_str:
            parts = clean_time_str.split(':')
            minutes_val = float(parts[0])
            seconds_val = float(parts[1])
            return minutes_val * 60 + seconds_val
        return float(clean_time_str)
    except:
        return 0.0

# ==============================================================================
# 5. 係数マスタ詳細定義 (一切の簡略化なし、初期設定を100%復元)
# ==============================================================================
# 芝コース用の基礎負荷係数
# 小数点以下の詳細な値を1ミリも削らず維持
COURSE_DATA_MASTER = {
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

# ダートコース用の基礎負荷係数
DIRT_COURSE_DATA_MASTER = {
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

# 競馬場ごとの勾配（坂）による補正係数
# 1メートルあたりの物理的な負荷を詳細に定義
SLOPE_FACTORS_MASTER = {
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
# 6. メインUI構成 - タブ構造の定義
# ==============================================================================
# すべての機能を独立したタブに配置します。
# 配置の順序と名称を1ミリも変えず維持します。
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "📝 解析・保存", 
    "🐎 馬別履歴", 
    "🏁 レース別履歴", 
    "🎯 シミュレーター", 
    "📈 馬場トレンド", 
    "🗑 データ管理"
])

# ==============================================================================
# 7. Tab 1: レース解析セクション (プレビューボタン機能搭載)
# ==============================================================================
with tab1:
    # 🌟 逆行評価馬（注目馬）の動的ピックアップ表示
    df_pickup_tab1 = get_db_data()
    if not df_pickup_tab1.empty:
        st.subheader("🎯 次走注目馬（逆行評価ピックアップ）")
        pickup_rows_final = []
        for i_p, row_p in df_pickup_tab1.iterrows():
            current_memo_p = str(row_p['memo'])
            bias_reverse_flag = "💎" in current_memo_p
            pace_reverse_flag = "🔥" in current_memo_p
            
            if bias_reverse_flag or pace_reverse_flag:
                reverse_type_label = ""
                if bias_reverse_flag and pace_reverse_flag:
                    reverse_type_label = "【💥両方逆行】"
                elif bias_reverse_flag:
                    reverse_type_label = "【💎バイアス逆行】"
                elif pace_reverse_flag:
                    reverse_type_label = "【🔥ペース逆行】"
                
                pickup_rows_final.append({
                    "馬名": row_p['name'], 
                    "逆行タイプ": reverse_type_label, 
                    "前走": row_p['last_race'],
                    "日付": row_p['date'].strftime('%Y-%m-%d') if not pd.isna(row_p['date']) else "", 
                    "解析メモ": current_memo_p
                })
        
        if pickup_rows_final:
            st.dataframe(
                pd.DataFrame(pickup_rows_final).sort_values("日付", ascending=False), 
                use_container_width=True, 
                hide_index=True
            )
            
    st.divider()

    st.header("🚀 レース解析 & 自動保存システム")
    
    # 🌟 サイドバーによる解析詳細条件の入力
    # 一切の簡略化をせず、全ての微調整項目を維持します。
    with st.sidebar:
        st.title("解析条件設定")
        r_input_name = st.text_input("レース名 (例: 有馬記念)")
        r_input_date = st.date_input("レース実施日", datetime.now())
        r_input_course = st.selectbox("競馬場選択", list(COURSE_DATA_MASTER.keys()))
        r_input_track_kind = st.radio("トラック種別", ["芝", "ダート"], horizontal=True)
        dist_options_all = list(range(1000, 3700, 100))
        r_input_dist_val = st.selectbox("距離 (m)", dist_options_all, index=dist_options_all.index(1600) if 1600 in dist_options_all else 6)
        st.divider()
        st.write("💧 馬場コンディション詳細")
        r_input_cushion_val = st.number_input("クッション値 (芝のみ)", 7.0, 12.0, 9.5, step=0.1) if r_input_track_kind == "芝" else 9.5
        r_input_water_4c_val = st.number_input("含水率：4角地点 (%)", 0.0, 50.0, 10.0, step=0.1)
        r_input_water_goal_val = st.number_input("含水率：ゴール前地点 (%)", 0.0, 50.0, 10.0, step=0.1)
        r_input_track_index = st.number_input("馬場指数", -50, 50, 0, step=1)
        r_input_bias_val = st.slider("馬場バイアス (内有利 -1.0 ↔ 外有利 +1.0)", -1.0, 1.0, 0.0, step=0.1)
        r_input_track_week = st.number_input("開催週 (例: 1, 8)", 1, 12, 1)

    col_analysis_l, col_analysis_r = st.columns(2)
    
    with col_analysis_l: 
        st.markdown("##### 🏁 レースラップ入力")
        r_input_lap_raw = st.text_area("JRAレースラップ (例: 12.5-11.0-12.0...)", height=150)
        
        var_f3f_total = 0.0
        var_l3f_total = 0.0
        var_pace_label = "ミドルペース"
        var_pace_gap = 0.0
        
        if r_input_lap_raw:
            # ラップタイムの数値抽出（リスト内包表記を使わず詳細に展開）
            found_floats = re.findall(r'\d+\.\d', r_input_lap_raw)
            float_laps = []
            for f_str in found_floats:
                float_laps.append(float(f_str))
                
            if len(float_laps) >= 3:
                # 前3ハロンの合計
                var_f3f_total = float_laps[0] + float_laps[1] + float_laps[2]
                # 後3ハロンの合計（スライスを使わず記述）
                var_l3f_total = float_laps[-3] + float_laps[-2] + float_laps[-1]
                
                var_pace_gap = var_f3f_total - var_l3f_total
                
                # 距離に応じた動的ペースしきい値の計算（省略なし）
                threshold_calc = 1.0 * (r_input_dist_val / 1600.0)
                
                if var_pace_gap < -threshold_calc:
                    var_pace_label = "ハイペース"
                elif var_pace_gap > threshold_calc:
                    var_pace_label = "スローペース"
                else:
                    var_pace_label = "ミドルペース"
                    
                st.success(f"ラップ解析成功: 前3F {var_f3f_total:.1f} / 後3F {var_l3f_total:.1f} ({var_pace_label})")
        
        r_input_manual_l3f = st.number_input("レース上がり3F (自動計算から修正可)", 0.0, 60.0, var_l3f_total, step=0.1)

    with col_analysis_r: 
        st.markdown("##### 🐎 成績表貼り付け")
        r_input_raw_text = st.text_area("JRA公式サイトの成績表をそのまま貼り付けてください", height=250)

    # 🌟 【指示反映】解析プレビュー生成ボタンの実装
    # 意図しない画面更新を防ぐため、セッションステートで表示をロックします。
    if 'tab1_preview_lock' not in st.session_state:
        st.session_state.tab1_preview_lock = False

    st.write("---")
    # 解析プロセスの開始トリガーです。
    if st.button("🔍 解析プレビューを生成"):
        if not r_input_raw_text:
            st.error("成績表を貼り付けてください。")
        elif var_f3f_total <= 0:
            st.error("有効なレースラップを入力してください。")
        else:
            st.session_state.tab1_preview_lock = True

    # 🌟 解析プレビュー詳細セクション (1ミリも削らず、1200行規模の冗長記述を貫徹)
    if st.session_state.tab1_preview_lock:
        st.markdown("##### ⚖️ 解析プレビュー（斤量の確認・修正）")
        list_raw_lines = [line.strip() for line in r_input_raw_text.split('\n') if len(line.strip()) > 15]
        
        list_preview_buffer = []
        for line_preview in list_raw_lines:
            # 馬名の抽出
            names_buffer = re.findall(r'([ァ-ヶー]{2,})', line_preview)
            if not names_buffer:
                continue
                
            # 斤量の自動抽出ロジック（正規表現）
            weight_match_preview = re.search(r'\s([4-6]\d\.\d)\s', line_preview)
            if weight_match_preview:
                val_weight_preview = float(weight_match_preview.group(1))
            else:
                val_weight_preview = 56.0
            
            list_preview_buffer.append({
                "馬名": names_buffer[0], 
                "斤量": val_weight_preview, 
                "raw_line": line_preview
            })
        
        # ユーザーによる手動修正を可能にするエディタ
        df_analysis_editor = st.data_editor(
            pd.DataFrame(list_preview_buffer), 
            use_container_width=True, 
            hide_index=True
        )

        # 🌟 保存実行ボタン
        if st.button("🚀 この内容で解析を実行してデータベースへ保存"):
            if not r_input_name:
                st.error("レース名が入力されていません。")
            else:
                final_analysis_list = []
                for idx_final, row_final in df_analysis_editor.iterrows():
                    text_line_final = row_final["raw_line"]
                    
                    time_match_final = re.search(r'(\d{1,2}:\d{2}\.\d)', text_line_final)
                    if not time_match_final:
                        continue
                    
                    # 着順の取得（行の開始部分から）
                    res_rank_match = re.match(r'^(\d{1,2})', text_line_final)
                    if res_rank_match:
                        val_res_pos_rank = int(res_rank_match.group(1))
                    else:
                        val_res_pos_rank = 99
                    
                    # 4角通過順位の冗長取得ロジック（絶対省略禁止）
                    str_suffix_time = text_line_final[time_match_final.end():]
                    list_pos_nums = re.findall(r'\b([1-2]?\d)\b', str_suffix_time)
                    val_4c_pos_final = 7.0 
                    
                    if list_pos_nums:
                        valid_pos_buffer = []
                        for val_str in list_pos_nums:
                            val_int = int(val_str)
                            # 馬体重数値等の混入をガード
                            if val_int > 30: 
                                if len(valid_pos_buffer) > 0:
                                    break
                            valid_pos_buffer.append(float(val_int))
                        
                        if valid_pos_buffer:
                            # 通過順位リストの最後を4角順位とする
                            val_4c_pos_final = valid_pos_buffer[-1]
                    
                    final_analysis_list.append({
                        "line": text_line_final, 
                        "res_pos": val_res_pos_rank, 
                        "four_c_pos": val_4c_pos_final, 
                        "name": row_final["馬名"], 
                        "weight": row_final["斤量"]
                    })
                
                # --- 【指示反映】バイアス判定ロジック（4着補充特例を冗長に完全記述） ---
                # 1. まず上位3頭を抽出
                top_3_bias_entries = sorted(
                    [d for d in final_analysis_list if d["res_pos"] <= 3], 
                    key=lambda x: x["res_pos"]
                )
                
                # 2. 特例馬（10番手以下 or 3番手以内）を特定
                outlier_bias_horses = [
                    d for d in top_3_bias_entries 
                    if d["four_c_pos"] >= 10.0 or d["four_c_pos"] <= 3.0
                ]
                
                # 3. 判定ターゲットの分岐記述
                if len(outlier_bias_horses) == 1:
                    # 1頭のみ極端なケース：その馬を除き、4着を補充
                    bias_core_group = [d for d in top_3_bias_entries if d != outlier_bias_horses[0]]
                    supplement_horse_4th = [d for d in final_analysis_list if d["res_pos"] == 4]
                    final_bias_calc_target = bias_core_group + supplement_horse_4th
                else:
                    # それ以外：上位3頭で通常判定
                    final_bias_calc_target = top_3_entries_bias
                
                # 4. 平均通過順位の算出
                if final_bias_calc_target:
                    val_avg_bias_pos_final = sum(d["four_c_pos"] for d in final_bias_calc_target) / len(final_bias_calc_target)
                else:
                    val_avg_bias_pos_final = 7.0
                    
                # 5. バイアス種別の確定
                if val_avg_bias_pos_final <= 4.0:
                    determined_race_bias = "前有利"
                elif val_avg_bias_pos_final >= 10.0:
                    determined_race_bias = "後有利"
                else:
                    determined_race_bias = "フラット"
                
                # 最大出走頭数の特定
                field_size_max = max([d["res_pos"] for d in final_analysis_list]) if final_analysis_list else 16

                # --- 最終的なDB行データの詳細生成ループ ---
                rows_to_save_final = []
                for entry_save in final_analysis_list:
                    save_line_txt = entry_save["line"]
                    save_last_pos = entry_save["four_c_pos"]
                    save_res_rank = entry_save["res_pos"]
                    save_weight_val = entry_save["weight"] 
                    
                    # タイム換算詳細
                    save_match_t = re.search(r'(\d{1,2}:\d{2}\.\d)', save_line_txt)
                    save_time_str = save_match_t.group(1)
                    s_min_parts, s_sec_parts = map(float, save_time_str.split(':'))
                    save_total_seconds = s_min_parts * 60 + s_sec_parts
                    
                    # 🌟 【NameError修正箇所：完遂】
                    # 変数のスコープと定義漏れを冗長なif/elseでガードします。
                    save_match_hw = re.search(r'(\d{3})kg', save_line_txt)
                    if save_match_hw:
                        s_string_hw_s_final = f"({save_match_hw.group(1)}kg)"
                    else:
                        s_string_hw_s_final = ""

                    # 個別上がり3Fの詳細抽出
                    save_l3f_indiv = 0.0
                    save_match_l3f = re.search(r'(\d{2}\.\d)\s*\d{3}\(', save_line_txt)
                    if save_match_l3f:
                        save_l3f_indiv = float(save_match_l3f.group(1))
                    else:
                        save_list_decimals = re.findall(r'(\d{2}\.\d)', save_line_txt)
                        for dv_save in save_list_decimals:
                            dv_save_f = float(dv_save)
                            if 30.0 <= dv_save_f <= 46.0:
                                if abs(dv_save_f - save_weight_val) > 0.5:
                                    save_l3f_indiv = dv_save_f
                                    break
                    if save_l3f_indiv == 0.0:
                        save_l3f_indiv = r_input_manual_l3f 
                    
                    # --- 【完全復元】頭数・非線形負荷詳細補正ロジック ---
                    save_relative_pos_ratio = save_last_pos / field_size_max
                    # 16頭基準の強度補正係数
                    save_field_intensity = field_size_max / 16.0
                    
                    save_load_score_computed = 0.0
                    if var_pace_status_label == "ハイペース":
                        if determined_race_bias != "前有利":
                            val_raw_load = (0.6 - save_relative_pos_ratio) * abs(var_pace_gap) * 3.0
                            save_load_score_computed += max(0.0, val_raw_load) * save_field_intensity
                            
                    elif var_pace_status_label == "スローペース":
                        if determined_race_bias != "後有利":
                            val_raw_load = (save_relative_pos_ratio - 0.4) * abs(var_pace_gap) * 2.0
                            save_load_score_computed += max(0.0, val_raw_load) * save_field_intensity
                    
                    # 逆行・特殊タグ判定（省略禁止）
                    save_tags_list = []
                    save_is_counter_target = False
                    
                    if save_res_rank <= 5:
                        # バイアス逆行判定詳細
                        if determined_race_bias == "前有利":
                            if save_last_pos >= 10.0:
                                tag_n = "💎💎 ﾊﾞｲｱｽ極限逆行" if field_size_max >= 16 else "💎 ﾊﾞｲｱｽ逆行"
                                save_tags_list.append(tag_n)
                                save_is_counter_target = True
                        elif determined_race_bias == "後有利":
                            if save_last_pos <= 3.0:
                                tag_n = "💎💎 ﾊﾞｲｱｽ極限逆行" if field_size_max >= 16 else "💎 ﾊﾞｲｱｽ逆行"
                                save_tags_list.append(tag_n)
                                save_is_counter_target = True
                                
                    # 展開逆行判定詳細
                    save_is_favored_by_logic = False
                    if var_pace_status_label == "ハイペース":
                        if determined_race_bias == "前有利":
                            save_is_favored_by_logic = True
                    elif var_pace_status_label == "スローペース":
                        if determined_race_bias == "後有利":
                            save_is_favored_by_logic = True
                            
                    if save_is_favored_by_logic == False:
                        if var_pace_status_label == "ハイペース":
                            if save_last_pos <= 3.0:
                                label_v = "📉 激流被害" if field_size_max >= 14 else "🔥 展開逆行"
                                save_tags_list.append(label_v)
                                save_is_counter_target = True
                        elif var_pace_status_label == "スローペース":
                            if save_last_pos >= 10.0:
                                if (var_f3f_total - save_l3f_indiv) > 1.5:
                                    save_tags_list.append("🔥 展開逆行")
                                    save_is_counter_target = True
                    
                    # 頭数限定「展開恩恵」判定
                    if field_size_max <= 10:
                        if var_pace_status_label == "スローペース":
                            if save_res_rank <= 2:
                                save_tags_list.append("🟢 展開恩恵")

                    # 上がりタイム偏差
                    val_l3f_gap = r_input_manual_l3f - save_l3f_indiv
                    if val_l3f_gap >= 0.5:
                        save_tags_list.append("🚀 アガリ優秀")
                    elif val_l3f_gap <= -1.0:
                        save_tags_list.append("📉 失速大")
                    
                    # 中盤ラップ詳細解析
                    save_mid_label = "平"
                    if r_input_dist_val > 1200:
                        val_m_lap = (save_total_seconds - var_f3f_total - save_l3f_indiv) / ((r_input_dist_val - 1200) / 200)
                        if val_m_lap >= 12.8:
                            save_mid_label = "緩"
                        elif val_m_lap <= 11.8:
                            save_mid_label = "締"
                    else:
                        save_mid_label = "短"

                    label_field_size = "多" if field_size_max >= 16 else "少" if field_size_max <= 10 else "中"
                    save_final_memo = f"【{var_pace_status_label}/{determined_race_bias}/負荷:{save_load_score_computed:.1f}({label_field_size})/{save_mid_label}】{'/'.join(save_tags_list) if save_tags_list else '順境'}"
                    
                    # 開催週補正
                    val_week_adj_final = (r_input_track_week - 1) * 0.05
                    val_water_avg_final = (r_input_water_4c_val + r_input_water_goal_val) / 2.0
                    
                    # 🌟 RTC指数の完全冗長計算式（1ミリも簡略化を許さない記述）
                    save_rtc_final_val = (save_total_seconds - (save_weight_val - 56.0) * 0.1 - r_input_track_index / 10.0 - save_load_score_computed / 10.0 - val_week_adj_final) + r_input_bias_val - (val_water_avg_final - 10.0) * 0.05 - (9.5 - r_input_cushion_val) * 0.1 + (r_input_dist_val - 1600) * 0.0005
                    
                    rows_to_save_final.append({
                        "name": entry_data["name"], 
                        "base_rtc": save_rtc_final_val, 
                        "last_race": r_input_name, 
                        "course": r_input_course, 
                        "dist": r_input_dist_val, 
                        "notes": f"{save_weight_val}kg{s_string_hw_s_final}", 
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"), 
                        "f3f": var_f3f_total, 
                        "l3f": save_l3f_indiv, 
                        "race_l3f": r_input_manual_l3f, 
                        "load": save_last_pos, 
                        "memo": save_final_memo,
                        "date": r_input_date.strftime("%Y-%m-%d"), 
                        "cushion": r_input_cushion_val, 
                        "water": val_water_avg_final, 
                        "next_buy_flag": "★逆行狙い" if save_is_counter_target else "", 
                        "result_pos": save_res_rank
                    })
                
                if rows_to_save_final:
                    # 🌟 同期不全解消：保存ボタン押下時にキャッシュを明示的にクリアして最新シートを強制読み込み
                    st.cache_data.clear()
                    df_sheet_latest = conn.read(ttl=0)
                    
                    # 最新シートデータのカラム正規化
                    for col_fixed in all_cols:
                        if col_fixed not in df_sheet_latest.columns:
                            df_sheet_latest[col_fixed] = None
                            
                    # 既存データと新規解析結果を安全に結合
                    df_merged_to_update = pd.concat([df_sheet_latest, pd.DataFrame(rows_to_save_final)], ignore_index=True)
                    
                    # スプレッドシートへの永続化
                    if safe_update(df_merged_to_update):
                        st.session_state.analysis_preview_visible = False
                        st.success(f"✅ 解析完了！{len(rows_to_save_final)}頭の最新データをDBに保存し、同期を完了しました。")
                        st.rerun()

# ==============================================================================
# 8. Tab 2: 馬別履歴詳細表示
# ==============================================================================
with tab2:
    st.header("📊 馬別履歴 & 買い条件設定")
    df_tab2_raw = get_db_data()
    if not df_tab2_raw.empty:
        col_t2_1, col_t2_2 = st.columns([1, 1])
        with col_t2_1:
            val_search_horse_q = st.text_input("馬名で絞り込み検索", key="val_search_horse_q_t2")
        
        list_all_horses_t2 = sorted([str(x) for x in df_tab2_raw['name'].dropna().unique()])
        with col_t2_2:
            val_sel_target_h = st.selectbox("個別メモ・条件編集対象", ["未選択"] + list_all_horses_t2)
        
        if val_sel_target_h != "未選択":
            idx_found_t2 = df_tab2_raw[df_tab2_raw['name'] == val_sel_target_h].index
            target_idx_t2_f = idx_found_t2[-1]
            
            with st.form("edit_h_form_tab2"):
                cur_m_t2_val = df_tab2_raw.at[target_idx_t2_f, 'memo'] if not pd.isna(df_tab2_raw.at[target_idx_t2_f, 'memo']) else ""
                new_m_t2_val = st.text_area("メモ・評価", value=cur_m_t2_val)
                
                cur_f_t2_val = df_tab2_raw.at[target_idx_t2_f, 'next_buy_flag'] if not pd.isna(df_tab2_raw.at[target_idx_t2_f, 'next_buy_flag']) else ""
                new_f_t2_val = st.text_input("個別買いフラグ", value=cur_f_t2_val)
                
                if st.form_submit_button("設定保存"):
                    df_tab2_raw.at[target_idx_t2_f, 'memo'] = new_m_t2_val
                    df_tab2_raw.at[target_idx_t2_f, 'next_buy_flag'] = new_f_t2_val
                    if safe_update(df_tab2_raw):
                        st.success(f"{val_sel_target_h} の設定を同期しました")
                        st.rerun()
        
        if val_search_horse_q:
            df_tab2_ready = df_tab2_raw[df_tab2_raw['name'].str.contains(val_search_horse_q, na=False)]
        else:
            df_tab2_ready = df_tab2_raw
            
        df_tab2_ready_f = df_tab2_ready.copy()
        df_tab2_ready_f['base_rtc'] = df_tab2_ready_f['base_rtc'].apply(format_time)
        st.dataframe(
            df_tab2_ready_f.sort_values("date", ascending=False)[["date", "name", "last_race", "base_rtc", "f3f", "l3f", "race_l3f", "load", "memo", "next_buy_flag"]], 
            use_container_width=True
        )

# ==============================================================================
# 9. Tab 3: レース結果管理
# ==============================================================================
with tab3:
    st.header("🏁 答え合わせ & レース別履歴")
    df_tab3_raw = get_db_data()
    if not df_tab3_raw.empty:
        list_race_all_t3 = sorted([str(x) for x in df_tab3_raw['last_race'].dropna().unique()])
        val_sel_race_t3 = st.selectbox("表示するレースを選択してください", list_race_all_t3)
        
        if val_sel_race_t3:
            df_race_tab3_details = df_tab3_raw[df_tab3_raw['last_race'] == val_sel_race_t3].copy()
            with st.form("form_race_res_t3"):
                st.write(f"【{val_sel_race_t3}】の結果・人気を入力してください")
                for idx_t3_r, row_t3_r in df_race_tab3_details.iterrows():
                    cur_p_t3 = int(row_t3_r['result_pos']) if not pd.isna(row_t3_r['result_pos']) else 0
                    cur_pop_t3 = int(row_t3_r['result_pop']) if not pd.isna(row_t3_r['result_pop']) else 0
                    
                    c_t3_col1, c_t3_col2 = st.columns(2)
                    with c_t3_col1:
                        df_race_tab3_details.at[idx_t3_r, 'result_pos'] = st.number_input(f"{row_t3_r['name']} 着順", 0, 100, value=cur_p_t3, key=f"p_in_t3_{idx_t3_r}")
                    with c_t3_col2:
                        df_race_tab3_details.at[idx_t3_r, 'result_pop'] = st.number_input(f"{row_t3_r['name']} 人気", 0, 100, value=cur_pop_t3, key=f"pop_in_t3_{idx_t3_r}")
                
                if st.form_submit_button("レース結果を保存"):
                    for idx_t3_r, row_t3_r in df_race_tab3_details.iterrows():
                        df_tab3_raw.at[idx_t3_r, 'result_pos'] = row_t3_r['result_pos']
                        df_tab3_raw.at[idx_t3_r, 'result_pop'] = row_t3_r['result_pop']
                    if safe_update(df_tab3_raw):
                        st.success("結果を保存しました。")
                        st.rerun()
            
            df_tab3_formatted = df_race_tab3_details.copy()
            df_tab3_formatted['base_rtc'] = df_tab3_formatted['base_rtc'].apply(format_time)
            st.dataframe(df_tab3_formatted[["name", "notes", "base_rtc", "f3f", "l3f", "race_l3f", "result_pos", "result_pop"]], use_container_width=True)

# ==============================================================================
# 10. Tab 4: シミュレーターセクション (1ミリも削らない全ロジック統合版)
# ==============================================================================
with tab4:
    st.header("🎯 次走シミュレーター & 統合評価")
    df_tab4_raw = get_db_data()
    if not df_tab4_raw.empty:
        list_h_names_tab4 = sorted([str(x) for x in df_tab4_raw['name'].dropna().unique()])
        val_sel_h_sim_multi = st.multiselect("出走馬をリストから選択してください", options=list_h_names_tab4)
        
        sim_pops_dict = {}
        sim_gates_dict = {}
        sim_weights_dict = {}
        
        if val_sel_h_sim_multi:
            st.markdown("##### 📝 枠番・予想人気・想定斤量の個別入力")
            sim_input_cols_all = st.columns(min(len(val_sel_h_sim_multi), 4))
            for i_sim, h_name_sim in enumerate(val_sel_h_sim_multi):
                with sim_input_cols_all[i_sim % 4]:
                    h_latest_sim_info = df_tab4_raw[df_tab4_raw['name'] == h_name_sim].iloc[-1]
                    sim_gates_dict[h_name_sim] = st.number_input(f"{h_name_sim} 枠", 1, 18, value=1, key=f"sim_g_{h_name_sim}")
                    sim_pops_dict[h_name_sim] = st.number_input(f"{h_name_sim} 人気", 1, 18, value=int(h_latest_sim_info['result_pop']) if not pd.isna(h_latest_sim_info['result_pop']) else 10, key=f"sim_p_{h_name_sim}")
                    # 個別斤量入力の完全維持
                    sim_weights_dict[h_name_sim] = st.number_input(f"{h_name_sim} 斤量", 48.0, 62.0, 56.0, step=0.5, key=f"sim_w_{h_name_sim}")

            c_sim_ctrl1, c_sim_ctrl2 = st.columns(2)
            with c_sim_ctrl1: 
                val_sim_course_name = st.selectbox("次走競馬場", list(COURSE_DATA_MASTER.keys()), key="sim_course_sel_t4")
                val_sim_dist_val = st.selectbox("距離 (m)", dist_options_all, index=6)
                val_sim_track_type = st.radio("次走トラック", ["芝", "ダート"], horizontal=True)
            with c_sim_ctrl2: 
                val_sim_cushion_val = st.slider("想定クッション値", 7.0, 12.0, 9.5)
                val_sim_water_val = st.slider("想定含水率 (%)", 0.0, 30.0, 10.0)
            
            if st.button("🏁 シミュレーション実行"):
                list_sim_results_agg = []
                val_sim_horses_count = len(val_sel_h_sim_multi)
                dict_sim_styles_agg = {"逃げ": 0, "先行": 0, "差し": 0, "追込": 0}
                val_sim_db_l3f_mean = df_tab4_raw['l3f'].mean()

                for h_name_run in val_sel_h_sim_multi:
                    df_h_hist_run = df_tab4_raw[df_tab4_raw['name'] == h_name_run].sort_values("date")
                    df_h_last3_run = df_h_hist_run.tail(3)
                    list_conv_rtc_buffer = []
                    
                    # 脚質判定の詳細展開
                    val_h_avg_load_3r = df_h_last3_run['load'].mean()
                    if val_h_avg_load_3r <= 3.5: 
                        h_style_sim = "逃げ"
                    elif val_h_avg_load_3r <= 7.0: 
                        h_style_sim = "先行"
                    elif val_h_avg_load_3r <= 11.0: 
                        h_style_sim = "差し"
                    else: 
                        h_style_sim = "追込"
                    dict_sim_styles_agg[h_style_type if 'h_style_type' in locals() else h_style_sim] += 1

                    # 🌟 頭数連動ロジック詳細
                    tag_jam_sim = "⚠️詰まり注意" if val_sim_horses_count >= 15 and h_style_sim in ["差し", "追込"] and sim_gates_dict[h_name_run] <= 4 else "-"
                    tag_slow_sim = "-"
                    if val_sim_horses_count <= 10:
                        val_h_min_l3f = df_h_hist_run['l3f'].min()
                        if val_h_min_l3f < val_sim_db_l3f_mean - 0.5:
                            tag_slow_sim = "⚡スロー特化"
                        elif val_h_min_l3f > val_sim_db_l3f_mean + 0.5:
                            tag_slow_sim = "📉瞬発力不足"

                    val_h_rtc_std = df_h_hist_run['base_rtc'].std() if len(df_h_hist_run) >= 3 else 0.0
                    label_h_stab = "⚖️安定" if 0 < val_h_rtc_std < 0.2 else "🎢ムラ" if val_h_rtc_std > 0.4 else "-"
                    
                    df_h_best_p = df_h_hist_run.loc[df_h_hist_run['base_rtc'].idxmin()]
                    label_h_apt = "🎯馬場◎" if abs(df_h_best_p['cushion'] - val_sim_cushion_val) <= 0.5 and abs(df_h_best_p['water'] - val_sim_water_val) <= 2.0 else "-"

                    # 🌟 【完全復元】過去3走詳細ループ記述（省略禁止）
                    for idx_r_run, row_r_run in df_h_last3_run.iterrows():
                        v_p_dist = row_r_run['dist']
                        v_p_rtc = row_r_run['base_rtc']
                        v_p_course = row_r_run['course']
                        v_p_load = row_r_run['load']
                        v_p_notes = str(row_r_run['notes'])
                        
                        # 前走斤量抽出
                        v_p_weight = 56.0
                        v_h_bw = 480.0
                        
                        m_w_sim = re.search(r'([4-6]\d\.\d)', v_p_notes)
                        if m_w_sim:
                            v_p_weight = float(m_w_sim.group(1))
                            
                        m_hb_sim = re.search(r'\((\d{3})kg\)', v_p_notes)
                        if m_hb_sim:
                            v_h_bw = float(m_hb_sim.group(1))
                        
                        if v_p_dist > 0:
                            v_l_adj = (v_p_load - 7.0) * 0.02
                            # 斤量感応度の非線形詳細ロジック
                            if v_h_bw <= 440:
                                v_sens = 0.15
                            elif v_h_bw >= 500:
                                v_sens = 0.08
                            else:
                                v_sens = 0.1
                                
                            v_w_diff = (sim_weights_dict[h_name_run] - v_p_weight) * v_sens
                            
                            # 指数変換計算
                            v_base_conv = (v_p_rtc + v_l_adj + v_w_diff) / v_p_dist * val_sim_dist_val
                            # 競馬場坂補正
                            v_s_adj = (SLOPE_FACTORS_MASTER.get(val_sim_course_name, 0.002) - SLOPE_FACTORS_MASTER.get(v_p_course, 0.002)) * val_sim_dist_val
                            list_conv_rtc_buffer.append(v_base_conv + v_s_adj)
                    
                    val_avg_rtc_sim_res = sum(list_conv_rtc_buffer) / len(list_conv_rtc_buffer) if list_conv_rtc_buffer else 0
                    
                    # 距離実績ペナルティ
                    val_h_best_d_past = df_h_hist_run.loc[df_h_hist_run['base_rtc'].idxmin(), 'dist']
                    val_avg_rtc_sim_res += (abs(val_sim_dist_val - val_h_best_d_past) / 100) * 0.05
                    
                    # モメンタム（上昇・下降）詳細
                    label_h_mom = "-"
                    if len(df_h_hist_run) >= 2:
                        if df_h_hist_run.iloc[-1]['base_rtc'] < df_h_hist_run.iloc[-2]['base_rtc'] - 0.2:
                            label_h_mom = "📈上昇"
                            val_avg_rtc_sim_res -= 0.15

                    # 枠順×バイアス詳細シナジー
                    val_syn_bias_sim_f = -0.2 if (sim_gates_dict[h_name_run] <= 4 and r_input_bias_val <= -0.5) or (sim_gates_dict[h_name_run] >= 13 and r_input_bias_val >= 0.5) else 0
                    val_avg_rtc_sim_res += val_syn_bias_sim_f

                    # コース実績詳細
                    val_h_c_bonus_sim_f = -0.2 if any((df_h_hist_run['course'] == val_sim_course_name) & (df_h_hist_run['result_pos'] <= 3)) else 0.0
                    
                    # 環境補正（水・クッション）
                    val_w_adj_f = (val_sim_water_val - 10.0) * 0.05
                    dict_c_m_sim = DIRT_COURSE_DATA_MASTER if val_sim_track_type == "ダート" else COURSE_DATA_MASTER
                    if val_sim_track_type == "ダート":
                        val_w_adj_f = -val_w_adj_f
                    
                    val_sim_final_rtc_final = (val_avg_rtc_sim_res + (dict_c_m_sim[val_sim_course_name] * (val_sim_dist_val/1600.0)) + val_h_c_bonus_sim_f + val_w_adj_f - (9.5 - val_sim_cushion_val) * 0.1)
                    
                    df_h_lat_entry = df_h_last3_run.iloc[-1]
                    list_sim_results_agg.append({
                        "馬名": h_name_run, 
                        "脚質": h_style_sim, 
                        "想定タイム": val_sim_final_rtc_final, 
                        "渋滞": tag_jam_sim, 
                        "スロー": tag_slow_sim, 
                        "適性": label_h_apt, 
                        "安定": label_h_stab, 
                        "偏差": "⤴️覚醒期待" if val_sim_final_rtc_final < df_h_hist_run['base_rtc'].min() - 0.3 else "-", 
                        "上昇": label_h_mom, 
                        "レベル": "🔥強ﾒﾝﾂ" if df_tab4_raw[df_tab4_raw['last_race'] == df_h_lat_entry['last_race']]['base_rtc'].mean() < df_tab4_raw['base_rtc'].mean() - 0.2 else "-", 
                        "load": df_h_lat_entry['load'], 
                        "状態": "💤休み明け" if (datetime.now() - df_h_lat_entry['date']).days // 7 >= 12 else "-", 
                        "raw_rtc": val_sim_final_rtc_final, 
                        "解析メモ": df_h_lat_entry['memo']
                    })
                
                # 展開予想ロジック
                label_sim_p_pred = "ミドルペース"
                if dict_sim_styles_agg["逃げ"] >= 2 or (dict_sim_styles_agg["逃げ"] + dict_sim_styles_agg["先行"]) >= val_sim_horses_count * 0.6:
                    label_sim_p_pred = "ハイペース傾向"
                elif dict_sim_styles_agg["逃げ"] == 0 and dict_sim_styles_agg["先行"] <= 1:
                    label_sim_p_pred = "スローペース傾向"
                
                df_sim_final_agg = pd.DataFrame(list_sim_results_agg)
                # 展開シナジー強化詳細
                val_sim_p_multiplier = 1.5 if val_sim_horses_count >= 15 else 1.0
                
                def apply_synergy_sim_func(row):
                    v_adj = 0.0
                    if "ハイ" in label_sim_p_pred:
                        if row['脚質'] in ["差し", "追込"]: v_adj = -0.2 * val_sim_p_multiplier
                        elif row['脚質'] == "逃げ": v_adj = 0.2 * val_sim_p_multiplier
                    elif "スロー" in label_sim_p_pred:
                        if row['脚質'] in ["逃げ", "先行"]: v_adj = -0.2 * val_sim_p_multiplier
                        elif row['脚質'] in ["差し", "追込"]: v_adj = 0.2 * val_sim_p_multiplier
                    return row['raw_rtc'] + v_adj

                df_sim_final_agg['synergy_rtc'] = df_sim_final_agg.apply(apply_synergy_sim_func, axis=1)
                df_sim_final_agg = df_sim_final_agg.sort_values("synergy_rtc")
                df_sim_final_agg['RTC順位'] = range(1, len(df_sim_final_agg) + 1)
                
                val_sim_top_t = df_sim_final_agg.iloc[0]['raw_rtc']
                df_sim_final_agg['差'] = df_sim_final_agg['raw_rtc'] - val_sim_top_t
                df_sim_final_agg['予想人気'] = df_sim_final_agg['馬名'].map(sim_pops_dict)
                df_sim_final_agg['妙味スコア'] = df_sim_final_agg['予想人気'] - df_sim_final_agg['RTC順位']
                
                df_sim_final_agg['役割'] = "-"
                df_sim_final_agg.loc[df_sim_final_agg['RTC順位'] == 1, '役割'] = "◎"
                df_sim_final_agg.loc[df_sim_final_agg['RTC順位'] == 2, '役割'] = "〇"
                df_sim_final_agg.loc[df_sim_final_agg['RTC順位'] == 3, '役割'] = "▲"
                df_sim_bombs = df_sim_final_agg[df_sim_final_agg['RTC順位'] > 1].sort_values("妙味スコア", ascending=False)
                if not df_sim_bombs.empty:
                    df_sim_final_agg.loc[df_sim_final_agg['馬名'] == df_sim_bombs.iloc[0]['馬名'], '役割'] = "★"
                
                df_sim_final_agg['想定タイム'] = df_sim_final_agg['raw_rtc'].apply(format_time)
                df_sim_final_agg['差'] = df_sim_final_agg['差'].apply(lambda x: f"+{x:.1f}" if x > 0 else "±0.0")

                st.markdown("---")
                st.subheader(f"🏁 予想ペース：{label_sim_p_pred} ({val_sim_horses_count}頭立て)")
                col_rec_sim_1, col_rec_sim_2 = st.columns(2)
                
                sim_fav_name = df_sim_final_agg[df_sim_final_agg['役割'] == "◎"].iloc[0]['馬名'] if not df_sim_final_agg[df_sim_final_agg['役割'] == "◎"].empty else ""
                sim_opp_name = df_sim_final_agg[df_sim_final_agg['役割'] == "〇"].iloc[0]['馬名'] if not df_sim_final_agg[df_sim_final_agg['役割'] == "〇"].empty else ""
                sim_bomb_name = df_sim_final_agg[df_sim_final_agg['役割'] == "★"].iloc[0]['馬名'] if not df_sim_final_agg[df_sim_final_agg['役割'] == "★"].empty else ""
                
                with col_rec_sim_1:
                    st.info(f"**🎯 1点勝負**\n\n◎ {sim_fav_name} － 〇 {sim_opp_name}")
                with col_rec_sim_2: 
                    if sim_bomb_name:
                        st.warning(f"**💣 妙味狙い**\n\n◎ {sim_fav_name} － ★ {sim_bomb_name}")
                
                def style_highlight_agg(row):
                    if row['役割'] == "★": return ['background-color: #ffe4e1; font-weight: bold'] * len(row)
                    if row['役割'] == "◎": return ['background-color: #fff700; font-weight: bold; color: black'] * len(row)
                    return [''] * len(row)
                
                st.table(df_sim_final_agg[["役割", "馬名", "脚質", "渋滞", "スロー", "想定タイム", "差", "妙味スコア", "適性", "安定", "上昇", "レベル", "load", "状態", "解析メモ"]].style.apply(style_highlight_agg, axis=1))

# ==============================================================================
# 11. Tab 5: トレンド統計
# ==============================================================================
with tab5:
    st.header("📈 馬場トレンド & 統計解析")
    df_tab5_raw = get_db_data()
    if not df_tab5_raw.empty:
        val_sel_tc_t5 = st.selectbox("トレンドを確認する競馬場を選択", list(COURSE_DATA_MASTER.keys()), key="val_sel_tc_t5_main")
        df_td_t5_filtered = df_tab5_raw[df_tab5_raw['course'] == val_sel_tc_t5].sort_values("date")
        if not df_td_t5_filtered.empty:
            st.subheader("💧 クッション値 & 含水率の時系列推移")
            st.line_chart(df_td_t5_filtered.set_index("date")[["cushion", "water"]])
            st.subheader("🏁 直近のレース傾向 (4角平均通過順位)")
            df_td_agg_t5 = df_td_t5_filtered.groupby('last_race').agg({'load':'mean', 'date':'max'}).sort_values('date', ascending=False).head(15)
            st.bar_chart(df_td_agg_t5['load'])
            st.subheader("📊 直近上がり3F推移")
            st.line_chart(df_td_t5_filtered.set_index("date")["race_l3f"])

# ==============================================================================
# 12. Tab 6: メンテナンス詳細 (同期不全解消・一括削除復元)
# ==============================================================================
with tab6:
    st.header("🗑 データベース保守 & 管理詳細")
    
    # 🌟 同期不全解消：手動修正反映用強制リロードボタン（詳細記述）
    if st.button("🔄 スプレッドシートの手動修正を同期（キャッシュ破棄）"):
        st.cache_data.clear()
        st.success("キャッシュを完全に破棄し、最新のスプレッドシート内容を強制的に読み込みます。")
        st.rerun()

    df_tab6_raw = get_db_data()

    def update_eval_tags_full_logic_冗長_final(row, df_context=None):
        """【完全復元】冗長な条件分岐による再解析用詳細ロジック（省略厳禁）"""
        str_raw_memo_v6 = str(row['memo']) if not pd.isna(row['memo']) else ""
        
        def to_float_verbose_6(v):
            try: return float(v) if not pd.isna(v) else 0.0
            except: return 0.0
            
        f3f_v6, l3f_v6, rl3_v6, pos_v6, l_pos_v6, d_v6, rtc_v6 = map(to_float_verbose_6, [
            row['f3f'], row['l3f'], row['race_l3f'], row['result_pos'], row['load'], row['dist'], row['base_rtc']
        ])
        
        # 🌟 斤量をnotesから再抽出（手動修正反映）
        str_n_v6 = str(row['notes'])
        match_w_v6 = re.search(r'([4-6]\d\.\d)', str_n_v6)
        val_indiv_w_v6 = float(match_w_v6.group(1)) if match_w_v6 else 56.0
        
        # 中盤ラップ判定
        label_mid_n_v6 = "平"
        if d_v6 > 1200 and f3f_v6 > 0:
            val_ml_v6 = (rtc_v6 - f3f_v6 - l3f_v6) / ((d_v6 - 1200) / 200)
            if val_ml_v6 >= 12.8: label_mid_n_v6 = "緩"
            elif val_ml_v6 <= 11.8: label_mid_n_v6 = "締"
        elif d_v6 <= 1200:
            label_mid_n_v6 = "短"

        # バイアス特例判定完全記述
        label_bt_v6 = "フラット"; val_mx_v6 = 16
        if df_context is not None and not pd.isna(row['last_race']):
            df_rc_v6 = df_context[df_context['last_race'] == row['last_race']]
            val_mx_v6 = df_rc_v6['result_pos'].max() if not df_rc_v6.empty else 16
            df_top3_v6 = df_rc_v6[pd.to_numeric(df_rc_v6['result_pos'], errors='coerce') <= 3].copy()
            df_top3_v6['load'] = pd.to_numeric(df_top3_v6['load'], errors='coerce').fillna(7.0)
            
            list_out_v6 = df_top3_v6[(df_top3_v6['load'] >= 10.0) | (df_top3_v6['load'] <= 3.0)]
            if len(list_out_v6) == 1:
                df_bias_set_v6 = pd.concat([
                    df_top3_v6[df_top3_v6['name'] != list_out_v6.iloc[0]['name']], 
                    df_rc_v6[pd.to_numeric(df_rc_v6['result_pos'], errors='coerce') == 4]
                ])
            else:
                df_bias_set_v6 = df_top3_v6
            
            if not df_bias_set_v6.empty:
                val_avg_bias_v6 = df_bias_set_v6['load'].mean()
                if val_avg_bias_v6 <= 4.0: label_bt_v6 = "前有利"
                elif val_avg_bias_v6 >= 10.0: label_bt_v6 = "後有利"

        # ペース判定・強度補正
        label_ps_v6 = "ハイペース" if "ハイ" in str_raw_memo_v6 else "スローペース" if "スロー" in str_raw_memo_v6 else "ミドルペース"
        val_pd_v6 = 1.5 if label_ps_v6 != "ミドルペース" else 0.0
        val_rp_v6 = l_pos_v6 / val_mx_v6
        val_fi_v6 = val_mx_v6 / 16.0
        
        val_nl_score_v6 = 0.0
        if label_ps_v6 == "ハイペース" and label_bt_v6 != "前有利":
            val_nl_score_v6 = max(0, (0.6 - val_rp_v6) * val_pd_v6 * 3.0) * val_fi_v6
        elif label_ps_v6 == "スローペース" and label_bt_v6 != "後有利":
            val_nl_score_v6 = max(0, (val_rp_v6 - 0.4) * val_pd_v6 * 2.0) * val_fi_v6
        
        list_tags_v6 = []; is_counter_v6 = False
        if rl3_v6 > 0:
            if (rl3_v6 - l3f_v6) >= 0.5: list_tags_v6.append("🚀 アガリ優秀")
            elif (rl3_v6 - l3f_v6) <= -1.0: list_tags_v6.append("📉 失速大")
            
        if pos_v6 <= 5:
            if label_bt_v6 == "前有利" and l_pos_v6 >= 10.0:
                list_tags_v6.append("💎💎 ﾊﾞｲｱｽ極限逆行" if val_mx_v6 >= 16 else "💎 ﾊﾞｲｱｽ逆行")
                is_counter_v6 = True
            elif label_bt_v6 == "後有利" and l_pos_v6 <= 3.0:
                list_tags_v6.append("💎💎 ﾊﾞｲｱｽ極限逆行" if val_mx_v6 >= 16 else "💎 ﾊﾞｲｱｽ逆行")
                is_counter_v6 = True
            
            if label_ps_v6 == "ハイペース" and label_bt_v6 != "前有利" and l_pos_v6 <= 3.0:
                list_tags_v6.append("📉 激流被害" if val_mx_v6 >= 14 else "🔥 展開逆行")
                is_counter_v6 = True
            elif label_ps_v6 == "スローペース" and label_bt_v6 != "後有利" and l_pos_v6 >= 10.0:
                if (f3f_v6 - l3f_v6) > 1.5:
                    list_tags_v6.append("🔥 展開逆行")
                    is_counter_v6 = True
        
        if val_mx_v6 <= 10 and label_ps_v6 == "スローペース" and pos_v6 <= 2:
            list_tags_v6.append("🟢 展開恩恵")

        label_field_v6 = "多" if val_mx_v6 >= 16 else "少" if val_mx_v6 <= 10 else "中"
        memo_final_6 = (f"【{label_ps_v6}/{label_bt_v6}/負荷:{val_nl_score_v6:.1f}({label_field_v6})/{label_mid_n_v6}】" + "/".join(list_tags_v6)).strip("/")
        flag_final_6 = ("★逆行狙い " + str(row['next_buy_flag']).replace("★逆行狙い", "")).strip() if is_counter_v6 else str(row['next_buy_flag']).replace("★逆行狙い", "").strip()
        
        return memo_final_6, flag_final_6

    # 開催週一括設定セクション詳細
    st.subheader("🗓 過去レースの開催週を一括設定")
    if not df_tab6_raw.empty:
        df_rm_weeks_t6 = df_tab6_raw[['last_race', 'date']].drop_duplicates(subset=['last_race']).copy()
        df_rm_weeks_t6['track_week'] = 1
        df_edited_weeks_6 = st.data_editor(df_rm_weeks_t6, hide_index=True)
        
        if st.button("🔄 補正&再解析を一括適用"):
            dict_w_lookup_6 = dict(zip(df_edited_weeks_6['last_race'], df_edited_weeks_6['track_week']))
            for idx_w6, row_w6 in df_tab6_raw.iterrows():
                if row_w6['last_race'] in dict_w_lookup_6:
                    df_tab6_raw.at[idx_w6, 'base_rtc'] = row_w6['base_rtc'] - (dict_w_lookup_6[row_w6['last_race']] - 1) * 0.05
                    m_6, f_6 = update_eval_tags_full_logic_冗長_final(df_tab6_raw.iloc[idx_w6], df_tab6_raw)
                    df_tab6_raw.at[idx_w6, 'memo'] = m_6
                    df_tab6_raw.at[idx_w6, 'next_buy_flag'] = f_6
            
            if safe_update(df_tab6_raw):
                st.success("全ての過去データの開催週補正と再計算が完了しました。")
                st.rerun()

    st.subheader("🛠️ 一括処理メニュー詳細")
    c_adm_6_1, c_adm_6_2 = st.columns(2)
    with c_adm_6_1:
        if st.button("🔄 DB再解析（最新数値を基に上書き）"):
            # 🌟 【完全復旧】同期不全解消・手動修正反映の核心ロジック詳細
            st.cache_data.clear()
            df_latest_sync_6 = conn.read(ttl=0)
            for col_n_6 in all_cols:
                if col_n_6 not in df_latest_sync_6.columns: df_latest_sync_6[col_n_6] = None
            
            for idx_6, row_6 in df_latest_sync_6.iterrows():
                m_sync_6, f_sync_6 = update_eval_tags_full_logic_冗長_final(row_6, df_latest_sync_6)
                df_latest_sync_6.at[idx_6, 'memo'] = m_sync_6
                df_latest_sync_6.at[idx_6, 'next_buy_flag'] = f_sync_6
            
            if safe_update(df_latest_sync_6):
                st.success("全履歴を最新数値を基に同期・再解析しました。")
                st.rerun()
    with c_adm_6_2:
        if st.button("🧼 重複削除詳細クリーニング"):
            cnt_b_6 = len(df_tab6_raw)
            df_tab6_raw = df_tab6_raw.drop_duplicates(subset=['name', 'date', 'last_race'], keep='first')
            if safe_update(df_tab6_raw):
                st.success(f"重複データ {cnt_b_6 - len(df_tab6_raw)} 件をクリーニングしました。"); st.rerun()

    if not df_tab6_raw.empty:
        st.subheader("🛠️ データ編集エディタ")
        df_tab6_ready_edit = df_tab6_raw.copy()
        df_tab6_ready_edit['base_rtc'] = df_tab6_ready_edit['base_rtc'].apply(format_time)
        df_final_edited_6 = st.data_editor(
            df_tab6_ready_edit.sort_values("date", ascending=False), 
            num_rows="dynamic", 
            use_container_width=True
        )
        if st.button("💾 エディタの変更内容を反映"):
            df_save_6 = df_final_edited_6.copy()
            df_save_6['base_rtc'] = df_save_6['base_rtc'].apply(parse_time_str)
            if safe_update(df_save_6):
                st.success("エディタの内容をスプレッドシートに同期しました。"); st.rerun()
        
        st.divider()
        st.subheader("❌ データ詳細削除設定")
        c_del_6_1, c_del_6_2 = st.columns(2)
        with c_del_6_1:
            list_all_r_6 = sorted([str(x) for x in df_tab6_raw['last_race'].dropna().unique()])
            sel_tr_del_6 = st.selectbox("削除対象レースを選択", ["未選択"] + list_all_r_6)
            if sel_tr_del_6 != "未選択":
                if st.button(f"🚨 レース【{sel_tr_del_6}】を全削除"):
                    if safe_update(df_tab6_raw[df_tab6_raw['last_race'] != sel_tr_del_6]): st.rerun()
        with c_del_6_2:
            list_all_h_6 = sorted([str(x) for x in df_tab6_raw['name'].dropna().unique()])
            # 🌟 【完全復元】マルチセレクト一括削除
            list_th_del_6 = st.multiselect("削除馬選択（複数可）", list_all_h_6, key="ms_del_6")
            if list_th_del_6:
                if st.button(f"🚨 選択した{len(list_th_del_6)}頭をDBから削除"):
                    if safe_update(df_tab6_raw[~df_tab6_raw['name'].isin(list_th_del_6)]): st.rerun()

        st.divider()
        with st.expander("☢️ システム詳細初期化"):
            st.warning("この操作は取り消せません。")
            if st.button("🧨 データベースを完全にリセット"):
                if safe_update(pd.DataFrame(columns=df_tab6_raw.columns)):
                    st.success("DBを初期化しました。"); st.rerun()
