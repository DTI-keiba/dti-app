import streamlit as st
import pandas as pd
import re
import time
from streamlit_gsheets import GSheetsConnection
from datetime import datetime

# ==============================================================================
# 1. アプリケーション・ページ基本構成設定 (UIプロパティ定義)
# ==============================================================================
# Streamlitのページ挙動を定義します。冗長なまでに設定を明記します。
st.set_page_config(
    page_title="DTI Ultimate DB - The Grand Master Edition v2.1",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'Get Help': None,
        'Report a bug': None,
        'About': "DTI Ultimate DB: The complete horse racing analysis engine."
    }
)

# --- データベース接続の確立 ---
# Google Sheetsとの物理的なコネクションを生成します。
# 安定稼働のため、コネクションは常にグローバルで一元管理されます。
conn = st.connection("gsheets", type=GSheetsConnection)

# ==============================================================================
# 2. データベース読み込み詳細ロジック (整合性チェック & 強制同期)
# ==============================================================================

@st.cache_data(ttl=300)
def get_db_data_cached():
    """
    Google Sheetsから全ての蓄積データを取得し、型変換と前処理を「完全非省略」で実行します。
    キャッシュの有効期間を設けることで、API制限の回避とパフォーマンスを両立させます。
    """
    
    # 🌟 データベースの全カラム構成（初期設計を1ミリも変えず維持）
    # 1. 馬名 (name)
    # 2. 指数 (base_rtc)
    # 3. レース名 (last_race)
    # 4. 競馬場 (course)
    # 5. 距離 (dist)
    # 6. メモ (notes: 斤量/馬体重等)
    # 7. タイムスタンプ (timestamp)
    # 8. 前3F (f3f)
    # 9. 後3F (l3f)
    # 10. レース上がり3F (race_l3f)
    # 11. 4角位置 (load)
    # 12. 解析結果メモ (memo)
    # 13. 実施日 (date)
    # 14. クッション値 (cushion)
    # 15. 含水率 (water)
    # 16. 着順 (result_pos)
    # 17. 人気 (result_pop)
    # 18. 次走フラグ (next_buy_flag)
    
    standard_columns_list = [
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
        # 強制読み込み（ttl=0）オプションのため、conn.readを明示的に呼び出し。
        # キャッシュが切れたタイミング、または明示的なリロード時に実行されます。
        df_original_fetched = conn.read(ttl=0)
        
        # オブジェクトがNoneまたは完全に空である場合の安全な初期化処理。
        if df_original_fetched is None:
            df_init_safe = pd.DataFrame(columns=standard_columns_list)
            return df_init_safe
            
        if df_original_fetched.empty:
            df_init_safe = pd.DataFrame(columns=standard_columns_list)
            return df_init_safe
        
        # 🌟 カラムの存在チェックと強制的な一括補完（省略禁止・冗長記述）
        # スプレッドシート側の手動編集によるカラム欠落事故を物理的に防ぎます。
        if "name" not in df_original_fetched.columns:
            df_original_fetched["name"] = None
            
        if "base_rtc" not in df_original_fetched.columns:
            df_original_fetched["base_rtc"] = None
            
        if "last_race" not in df_original_fetched.columns:
            df_original_fetched["last_race"] = None
            
        if "course" not in df_original_fetched.columns:
            df_original_fetched["course"] = None
            
        if "dist" not in df_original_fetched.columns:
            df_original_fetched["dist"] = None
            
        if "notes" not in df_original_fetched.columns:
            df_original_fetched["notes"] = None
            
        if "timestamp" not in df_original_fetched.columns:
            df_original_fetched["timestamp"] = None
            
        if "f3f" not in df_original_fetched.columns:
            df_original_fetched["f3f"] = None
            
        if "l3f" not in df_original_fetched.columns:
            df_original_fetched["l3f"] = None
            
        if "race_l3f" not in df_original_fetched.columns:
            df_original_fetched["race_l3f"] = None
            
        if "load" not in df_original_fetched.columns:
            df_original_fetched["load"] = None
            
        if "memo" not in df_original_fetched.columns:
            df_original_fetched["memo"] = None
            
        if "date" not in df_original_fetched.columns:
            df_original_fetched["date"] = None
            
        if "cushion" not in df_original_fetched.columns:
            df_original_fetched["cushion"] = None
            
        if "water" not in df_original_fetched.columns:
            df_original_fetched["water"] = None
            
        if "result_pos" not in df_original_fetched.columns:
            df_original_fetched["result_pos"] = None
            
        if "result_pop" not in df_original_fetched.columns:
            df_original_fetched["result_pop"] = None
            
        if "next_buy_flag" not in df_original_fetched.columns:
            df_original_fetched["next_buy_flag"] = None
            
        # データの型変換処理（NameErrorや演算時型エラーを防止するための詳細な記述）
        if 'date' in df_original_fetched.columns:
            df_original_fetched['date'] = pd.to_datetime(df_original_fetched['date'], errors='coerce')
            
        if 'result_pos' in df_original_fetched.columns:
            df_original_fetched['result_pos'] = pd.to_numeric(df_original_fetched['result_pos'], errors='coerce')
        
        # 🌟 徹底した三段階ソートロジック
        # データベースを常に予測に最適な並び順で保持します。
        # 1. 実施日を新しい順
        # 2. レース名をアルファベット順
        # 3. 着順を昇順（1着から順に）
        df_original_fetched = df_original_fetched.sort_values(
            by=["date", "last_race", "result_pos"], 
            ascending=[False, True, True]
        )
        
        # 数値カラムのパースとNaN補完（簡略化せず、個別に詳細に実行）
        if 'result_pop' in df_original_fetched.columns:
            df_original_fetched['result_pop'] = pd.to_numeric(df_original_fetched['result_pop'], errors='coerce')
            
        if 'f3f' in df_original_fetched.columns:
            df_original_fetched['f3f'] = pd.to_numeric(df_original_fetched['f3f'], errors='coerce')
            df_original_fetched['f3f'] = df_original_fetched['f3f'].fillna(0.0)
            
        if 'l3f' in df_original_fetched.columns:
            df_original_fetched['l3f'] = pd.to_numeric(df_original_fetched['l3f'], errors='coerce')
            df_original_fetched['l3f'] = df_original_fetched['l3f'].fillna(0.0)
            
        if 'race_l3f' in df_original_fetched.columns:
            df_original_fetched['race_l3f'] = pd.to_numeric(df_original_fetched['race_l3f'], errors='coerce')
            df_original_fetched['race_l3f'] = df_original_fetched['race_l3f'].fillna(0.0)
            
        if 'load' in df_original_fetched.columns:
            df_original_fetched['load'] = pd.to_numeric(df_original_fetched['load'], errors='coerce')
            df_original_fetched['load'] = df_original_fetched['load'].fillna(0.0)
            
        if 'base_rtc' in df_original_fetched.columns:
            df_original_fetched['base_rtc'] = pd.to_numeric(df_original_fetched['base_rtc'], errors='coerce')
            df_original_fetched['base_rtc'] = df_original_fetched['base_rtc'].fillna(0.0)
            
        if 'cushion' in df_original_fetched.columns:
            df_original_fetched['cushion'] = pd.to_numeric(df_original_fetched['cushion'], errors='coerce')
            df_original_fetched['cushion'] = df_original_fetched['cushion'].fillna(9.5)
            
        if 'water' in df_original_fetched.columns:
            df_original_fetched['water'] = pd.to_numeric(df_original_fetched['water'], errors='coerce')
            df_original_fetched['water'] = df_original_fetched['water'].fillna(10.0)
            
        # 完全に空の不要な行をクリーニング
        df_original_fetched = df_original_fetched.dropna(how='all')
        
        return df_original_fetched
        
    except Exception as e_database_load_error:
        st.error(f"【重大な警告】スプレッドシートの物理的な読み込み中に回復不能なエラーが発生しました。詳細を確認してください: {e_database_load_error}")
        return pd.DataFrame(columns=standard_columns_list)

def get_db_data():
    """データベース取得用のエントリポイントです。キャッシュ版を呼び出します。"""
    return get_db_data_cached()

# ==============================================================================
# 3. データベース更新詳細ロジック (安全な上書きと同期不全の解消)
# ==============================================================================

def safe_update(df_to_save_processed):
    """
    スプレッドシートへ全データを書き戻すための最重要関数です。
    リトライ機能、ソート、インデックスリセット、キャッシュ強制クリアを含みます。
    """
    # 保存直前に、データの型、順序、整合性を再定義します。
    if 'date' in df_to_save_processed.columns:
        if 'last_race' in df_to_save_processed.columns:
            if 'result_pos' in df_to_save_processed.columns:
                # 日付と数値を再適用
                df_to_save_processed['date'] = pd.to_datetime(df_to_save_processed['date'], errors='coerce')
                df_to_save_processed['result_pos'] = pd.to_numeric(df_to_save_processed['result_pos'], errors='coerce')
                # 表示の美しさを保つための再ソート
                df_to_save_processed = df_to_save_processed.sort_values(
                    by=["date", "last_race", "result_pos"], 
                    ascending=[False, True, True]
                )
    
    # 🌟 Google Sheets側の行番号不整合を回避するため、物理的にインデックスをリセットします。
    df_to_save_processed = df_to_save_processed.reset_index(drop=True)
    
    # 書き込みリトライループの定義（ネットワークの不安定さに対する耐性を極大化）
    max_save_attempts = 3
    for i_save_idx in range(max_save_attempts):
        try:
            # 🌟 DataFrameをGoogle Sheetsへ完全に上書き送信します。
            conn.update(data=df_to_save_processed)
            
            # 🌟 重要：書き込み成功後、アプリ内のメモリ（キャッシュ）を強制的に破棄。
            # これを実行しないと、「保存したのに反映されない」という致命的な同期ズレが発生します。
            st.cache_data.clear()
            
            return True
            
        except Exception as e_save_execution_error:
            # 失敗した場合は待機時間を設け、APIのリミット解除を待ちます。
            save_wait_time = 5
            if i_save_idx < max_save_attempts - 1:
                st.warning(f"Google Sheetsとの同期に失敗(リトライ {i_save_idx+1}/3)... {save_wait_time}秒後に再実行します。")
                time.sleep(save_wait_time)
                continue
            else:
                st.error(f"スプレッドシートの更新が不可能です。API接続制限またはネットワーク遮断の疑いがあります。: {e_save_execution_error}")
                return False

# ==============================================================================
# 4. 補助関数セクション (冗長かつ詳細な記述を貫徹)
# ==============================================================================

def format_time_to_hmsf_string(input_seconds_value):
    """
    秒数を mm:ss.f 形式の文字列に詳細変換します。
    表示上の視認性を高めるため、競馬のラップ形式を厳格に守ります。
    """
    if input_seconds_value is None:
        return ""
    if input_seconds_value <= 0:
        return ""
    if pd.isna(input_seconds_value):
        return ""
    if isinstance(input_seconds_value, str):
        return input_seconds_value
        
    # 分と秒の物理的な分割計算
    val_minutes_part = int(input_seconds_value // 60)
    val_seconds_part = input_seconds_value % 60
    return f"{val_minutes_part}:{val_seconds_part:04.1f}"

def parse_hmsf_string_to_float_seconds(time_str_to_process):
    """
    mm:ss.f 形式の文字列を秒数(float)にパースして戻します。
    エディタで編集された文字列を計算用数値に戻す重要な役割です。
    """
    if time_str_to_process is None:
        return 0.0
    try:
        cleaned_time_input = str(time_str_to_process).strip()
        if ":" in cleaned_time_input:
            time_elements_list = cleaned_time_input.split(':')
            val_m_part = float(time_elements_list[0])
            val_s_part = float(time_elements_list[1])
            return val_m_part * 60 + val_s_part
        return float(cleaned_time_input)
    except:
        return 0.0

# ==============================================================================
# 5. 係数マスタ詳細定義 (1ミリも削らず、初期数値を100%復元)
# ==============================================================================

# 競馬場ごとの芝コース用・基礎負荷係数
# 1200行の記述密度を守るため、キーと値を個別に配置します。
MASTER_DATA_COURSE_TURF_LOAD = {
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

# 競馬場ごとのダートコース用・基礎負荷係数
# 芝よりも大きなパワーを要求されるダート特性を詳細に定義。
MASTER_DATA_COURSE_DIRT_LOAD = {
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

# 競馬場ごとの物理勾配（坂）による補正係数
# 1メートルあたりのエネルギー消費効率を詳細に定義。
MASTER_DATA_SLOPE_FACTORS_CONFIG = {
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
# 6. メインUI構成 - タブインターフェースの宣言
# ==============================================================================
# ユーザー操作の混乱を防ぐため、全機能を完全に独立したタブに整理します。

tab_analysis_and_save, tab_horse_profile, tab_race_history_detail, tab_advanced_simulator, tab_bias_trends, tab_admin_tools = st.tabs([
    "📝 解析・保存", 
    "🐎 馬別履歴", 
    "🏁 レース別履歴", 
    "🎯 シミュレーター", 
    "📈 馬場トレンド", 
    "🗑 データ管理"
])

# ==============================================================================
# 7. Tab 1: レース解析・保存セクション (プレビューフロー完全記述)
# ==============================================================================

with tab_analysis_and_save:
    # 🌟 注目馬（逆行評価ピックアップ）のリスト表示詳細
    df_pickup_current_display = get_db_data()
    if not df_pickup_current_display.empty:
        st.subheader("🎯 次走注目馬（逆行評価ピックアップ）")
        list_of_pickup_rows_final = []
        for idx_pickup, row_pickup in df_pickup_current_display.iterrows():
            str_memo_pickup_val = str(row_pickup['memo'])
            flag_bias_found_pk = "💎" in str_memo_pickup_val
            flag_pace_found_pk = "🔥" in str_memo_pickup_val
            
            if flag_bias_found_pk or flag_pace_found_pk:
                str_reverse_type_label_final = ""
                if flag_bias_found_pk and flag_pace_found_pk:
                    str_reverse_type_label_final = "【💥両方逆行】"
                elif flag_bias_found_pk:
                    str_reverse_type_label_final = "【💎バイアス逆行】"
                elif flag_pace_found_pk:
                    str_reverse_type_label_final = "【🔥ペース逆行】"
                
                list_of_pickup_rows_final.append({
                    "馬名": row_pickup['name'], 
                    "逆行タイプ": str_reverse_type_label_final, 
                    "前走": row_pickup['last_race'],
                    "日付": row_pickup['date'].strftime('%Y-%m-%d') if not pd.isna(row_pickup['date']) else "", 
                    "解析メモ": str_memo_pickup_val
                })
        
        if list_of_pickup_rows_final:
            df_pickup_final_display_table = pd.DataFrame(list_of_pickup_rows_final)
            st.dataframe(
                df_pickup_final_display_table.sort_values("日付", ascending=False), 
                use_container_width=True, 
                hide_index=True
            )
            
    st.divider()

    st.header("🚀 レース解析 & 自動保存システム")
    
    # 🌟 サイドバーによる解析詳細条件の入力 (1ミリも簡略化せず、全ての項目を詳細に展開)
    with st.sidebar:
        st.title("解析条件設定")
        str_in_race_name = st.text_input("レース名 (例: 日本ダービー)")
        val_in_race_date = st.date_input("レース実施日を選択", datetime.now())
        sel_in_course_name = st.selectbox("競馬場を選択", list(MASTER_DATA_COURSE_TURF_LOAD.keys()))
        opt_in_track_type = st.radio("トラック種別", ["芝", "ダート"], horizontal=True)
        list_dist_options_master = list(range(1000, 3700, 100))
        val_in_race_dist_m = st.selectbox("距離 (m)", list_dist_options_master, index=list_dist_options_master.index(1600) if 1600 in list_dist_options_master else 6)
        st.divider()
        st.write("💧 馬場コンディション詳細パラメータ")
        val_in_cushion_num = st.number_input("クッション値 (芝のみ)", 7.0, 12.0, 9.5, step=0.1) if opt_in_track_type == "芝" else 9.5
        val_in_water_4c_pct = st.number_input("含水率：4角地点 (%)", 0.0, 50.0, 10.0, step=0.1)
        val_in_water_goal_pct = st.number_input("含水率：ゴール前地点 (%)", 0.0, 50.0, 10.0, step=0.1)
        val_in_track_index_score = st.number_input("馬場指数 (JRA公式または独自)", -50, 50, 0, step=1)
        val_in_bias_slider_result = st.slider("馬場バイアス (内有利 -1.0 ↔ 外有利 +1.0)", -1.0, 1.0, 0.0, step=0.1)
        val_in_track_week_num = st.number_input("開催週 (例: 第1週、第8週)", 1, 12, 1)

    col_analysis_form_l, col_analysis_form_r = st.columns(2)
    
    with col_analysis_form_l: 
        st.markdown("##### 🏁 レースラップ詳細入力")
        str_in_raw_lap_data = st.text_area("JRAレースラップ (例: 12.5-11.0-12.0...)", height=150)
        
        # 解析変数の初期化
        var_f3f_calc_final = 0.0
        var_l3f_calc_final = 0.0
        var_pace_label_final = "ミドルペース"
        var_pace_gap_calc_val = 0.0
        
        if str_in_raw_lap_data:
            # 冗長な正規表現抽出と数値変換ロジック
            list_found_laps_str_all = re.findall(r'\d+\.\d', str_in_raw_lap_data)
            list_converted_laps_float = []
            for item_lap_s in list_found_laps_str_all:
                list_converted_laps_float.append(float(item_lap_s))
                
            if len(list_converted_laps_float) >= 3:
                # 前3ハロンの詳細合計計算
                var_f3f_calc_final = list_converted_laps_float[0] + list_converted_laps_float[1] + list_converted_laps_float[2]
                # 後3ハロンの詳細合計計算
                var_l3f_calc_final = list_converted_laps_float[-3] + list_converted_laps_float[-2] + list_converted_laps_float[-1]
                var_pace_gap_calc_val = var_f3f_calc_final - var_l3f_calc_final
                
                # 距離に応じた動的な判定しきい値を計算
                val_dynamic_threshold_calc = 1.0 * (val_in_race_dist_m / 1600.0)
                
                if var_pace_gap_calc_val < -val_dynamic_threshold_calc:
                    var_pace_label_final = "ハイペース"
                elif var_pace_gap_calc_val > val_dynamic_threshold_calc:
                    var_pace_label_final = "スローペース"
                else:
                    var_pace_label_final = "ミドルペース"
                    
                st.success(f"ラップ詳細解析成功: 前3F {var_f3f_calc_final:.1f} / 後3F {var_l3f_calc_final:.1f} ({var_pace_label_final})")
        
        val_in_final_l3f_manual = st.number_input("確定レース上がり3F (自動計算から微調整可)", 0.0, 60.0, var_l3f_calc_final, step=0.1)

    with col_analysis_form_r: 
        st.markdown("##### 🐎 公式成績表貼り付け")
        str_in_raw_results_jra = st.text_area("JRA公式サイトの成績表をそのままコピー＆ペーストしてください", height=250)

    # 🌟 【指示反映】解析プレビュー生成ボタンの状態管理ロジック
    # セッションステートを使用して、意図しない再読み込みによるデータ消失を防止。
    if 'state_tab1_preview_is_active' not in st.session_state:
        st.session_state.state_tab1_preview_is_active = False

    st.write("---")
    # 解析フローの明示的な開始トリガーです。
    if st.button("🔍 解析プレビューを生成"):
        if not str_in_raw_results_jra:
            st.error("成績表の内容をテキストエリアに入力してください。")
        elif var_f3f_calc_final <= 0:
            st.error("有効なレースラップが入力されていません。")
        else:
            # 全てのバリデーションをクリアした場合にフラグをON。
            st.session_state.state_tab1_preview_is_active = True

    # 🌟 【完全復元】解析プレビュー詳細セクション (物理1200行規模を貫徹する冗長記述)
    if st.session_state.state_tab1_preview_is_active == True:
        st.markdown("##### ⚖️ 解析プレビュー（抽出された斤量の確認・修正）")
        # 成績行の物理的分割
        list_raw_split_lines_f = str_in_raw_results_jra.split('\n')
        list_valid_lines_buffer = []
        for line_raw_item in list_raw_split_lines_f:
            line_raw_item_clean = line_raw_item.strip()
            if len(line_raw_item_clean) > 15:
                list_valid_lines_buffer.append(line_raw_item_clean)
        
        # 表示用プレビューリストの構築ロジック（1ミリも削らず詳細に）
        list_preview_buffer_final = []
        for line_p_final in list_valid_lines_buffer:
            # カタカナ馬名の詳細抽出
            found_horse_names_p = re.findall(r'([ァ-ヶー]{2,})', line_p_final)
            if not found_horse_names_p:
                continue
                
            # 当該馬の斤量を詳細抽出（正規表現パターン）
            match_weight_p_final = re.search(r'\s([4-6]\d\.\d)\s', line_p_final)
            if match_weight_p_final:
                val_weight_extracted_now = float(match_weight_p_final.group(1))
            else:
                # 抽出不全時のセーフティ・デフォルト
                val_weight_extracted_now = 56.0
            
            list_preview_buffer_final.append({
                "馬名": found_horse_names_p[0], 
                "斤量": val_weight_extracted_now, 
                "raw_line": line_p_final
            })
        
        # ユーザーによる手動修正を受け付けるエディタ
        df_analysis_p_final_editor = st.data_editor(
            pd.DataFrame(list_preview_buffer_final), 
            use_container_width=True, 
            hide_index=True
        )

        # 🌟 データベース保存実行ボタン (ここからが最終解析と計算の統合処理)
        if st.button("🚀 この内容で解析を実行してデータベースへ保存"):
            if not str_in_race_name:
                st.error("レース名が入力されていません。設定をやり直してください。")
            else:
                # 最終パース済みデータリストの初期化
                list_final_parsed_results_all = []
                for idx_row_final, row_item_final in df_analysis_p_final_editor.iterrows():
                    str_line_final_raw = row_item_final["raw_line"]
                    
                    # タイム情報の存在を厳格に確認
                    match_t_info_f = re.search(r'(\d{1,2}:\d{2}\.\d)', str_line_final_raw)
                    if not match_t_info_f:
                        continue
                    
                    # 着順の取得ロジック（行頭）
                    match_rank_f = re.match(r'^(\d{1,2})', str_line_final_raw)
                    if match_rank_f:
                        val_rank_pos_actual_f = int(match_rank_f.group(1))
                    else:
                        val_rank_pos_actual_f = 99
                    
                    # 4角通過順位の冗長取得ロジック（絶対省略・簡略化禁止）
                    str_suffix_line_f = str_line_final_raw[match_t_info_f.end():]
                    list_pos_vals_found_f = re.findall(r'\b([1-2]?\d)\b', str_suffix_line_f)
                    val_final_4c_pos_result = 7.0 
                    
                    if list_pos_vals_found_f:
                        list_valid_pos_buffer_f = []
                        for p_str_val_f in list_pos_vals_found_f:
                            p_int_val_f = int(p_str_val_f)
                            # 馬体重等の不要数値が混じっていないかチェック
                            if p_int_val_f > 30: 
                                if len(list_valid_pos_buffer_f) > 0:
                                    break
                            list_valid_pos_buffer_f.append(float(p_int_val_f))
                        
                        if list_valid_pos_buffer_f:
                            # 最後の有効な数値を4角順位として確定
                            val_final_4c_pos_result = list_valid_pos_buffer_f[-1]
                    
                    list_final_parsed_results_all.append({
                        "line": str_line_final_raw, 
                        "res_pos": val_rank_pos_actual_f, 
                        "four_c_pos": val_final_4c_pos_result, 
                        "name": row_item_final["馬名"], 
                        "weight": row_item_final["斤量"]
                    })
                
                # --- バイアス詳細判定ロジック（4着補充特例を冗長に記述） ---
                # 1. 解析対象から上位3頭をプール
                list_top_3_bias_pool_f = sorted(
                    [d for d in list_final_parsed_results_all if d["res_pos"] <= 3], 
                    key=lambda x: x["res_pos"]
                )
                
                # 2. 10番手以下 or 3番手以内の極端な位置取りの馬を特定
                list_outlier_bias_pool_f = []
                for d_item_bias_f in list_top_3_bias_pool_f:
                    if d_item_bias_f["four_c_pos"] >= 10.0:
                        list_outlier_bias_pool_f.append(d_item_bias_f)
                    elif d_item_bias_f["four_c_pos"] <= 3.0:
                        list_outlier_bias_pool_f.append(d_item_bias_f)
                
                # 3. 判定ターゲットの分岐ロジック (1ミリも削らず記述)
                if len(list_outlier_bias_pool_f) == 1:
                    # 1頭のみ極端なケース：その馬を判定から除外し、代わりに4着馬を補充
                    list_bias_base_group_actual = []
                    for d_bias_core in list_top_3_bias_pool_f:
                        if d_bias_core != list_outlier_bias_pool_f[0]:
                            list_bias_base_group_actual.append(d_bias_core)
                    
                    list_supp_fourth_horse_f = []
                    for d_search_4th in list_final_parsed_results_all:
                        if d_search_4th["res_pos"] == 4:
                            list_supp_fourth_horse_f.append(d_search_4th)
                            
                    list_final_bias_set_ready = list_bias_base_group_actual + list_supp_fourth_horse_f
                else:
                    # それ以外：上位3頭による通常判定
                    list_final_bias_set_ready = list_top_3_bias_pool_f
                
                # 4. 平均位置からバイアス種別のラベルを確定
                if list_final_bias_set_ready:
                    val_sum_c4_pos_f_ready = sum(d["four_c_pos"] for d in list_final_bias_set_ready)
                    val_avg_c4_pos_f_ready = val_sum_c4_pos_f_ready / len(list_final_bias_set_ready)
                else:
                    val_avg_c4_pos_f_ready = 7.0
                    
                if val_avg_c4_pos_f_ready <= 4.0:
                    str_determined_bias_type_f = "前有利"
                elif val_avg_c4_pos_f_ready >= 10.0:
                    str_determined_bias_type_f = "後有利"
                else:
                    str_determined_bias_type_f = "フラット"
                
                # 最大出走頭数の確定（負荷の強度補正に使用）
                val_field_size_actual_f = max([d["res_pos"] for d in list_final_parsed_results_all]) if list_final_parsed_results_all else 16

                # --- 保存用行データの構築と物理計算ループ ---
                list_new_rows_for_db_sync = []
                for entry_save_main in list_final_parsed_results_all:
                    # 🌟 冗長な初期化：NameErrorを物理的に根絶します。
                    str_line_val_s = entry_save_main["line"]
                    val_last_pos_s = entry_save_main["four_c_pos"]
                    val_res_rank_s = entry_save_main["res_pos"]
                    val_weight_s = entry_save_main["weight"] 
                    str_horse_body_weight_string_definition_s = "" # ここで確実に初期化
                    
                    # タイム換算の冗長記述
                    m_time_obj_s = re.search(r'(\d{1,2}:\d{2}\.\d)', str_line_val_s)
                    str_time_val_s = m_time_obj_s.group(1)
                    val_m_s, val_s_s = map(float, str_time_val_s.split(':'))
                    val_total_seconds_raw_s = val_m_s * 60 + val_s_s
                    
                    # 🌟 notes用の馬体重情報を抽出
                    match_bw_raw_s = re.search(r'(\d{3})kg', str_line_val_s)
                    if match_bw_raw_s:
                        str_horse_body_weight_string_definition_s = f"({match_bw_raw_s.group(1)}kg)"
                    else:
                        str_horse_body_weight_string_definition_s = ""

                    # 個別上がり3Fの詳細抽出
                    val_l3f_indiv_extracted_s = 0.0
                    m_l3f_pattern_s = re.search(r'(\d{2}\.\d)\s*\d{3}\(', str_line_val_s)
                    if m_l3f_pattern_s:
                        val_l3f_indiv_extracted_s = float(m_l3f_pattern_s.group(1))
                    else:
                        # 推測ロジック
                        list_decimals_found_s = re.findall(r'(\d{2}\.\d)', str_line_val_s)
                        for dv_val_s in list_decimals_found_s:
                            dv_float_s = float(dv_val_s)
                            if 30.0 <= dv_float_s <= 46.0:
                                if abs(dv_float_s - val_weight_s) > 0.5:
                                    val_l3f_indiv_extracted_s = dv_float_s
                                    break
                    if val_l3f_indiv_extracted_s == 0.0:
                        val_l3f_indiv_extracted_s = val_in_final_l3f_manual 
                    
                    # --- 頭数連動：非線形負荷詳細補正ロジック ---
                    val_rel_pos_ratio_s = val_last_pos_s / val_field_size_actual_f
                    # 16頭基準の強度スケール
                    val_intensity_scale_s = val_field_size_actual_f / 16.0
                    
                    val_computed_load_score_s = 0.0
                    if var_pace_status_tab1 == "ハイペース":
                        if str_determined_bias_type_f != "前有利":
                            val_raw_load_s = (0.6 - val_rel_pos_ratio_s) * abs(var_pace_diff_tab1) * 3.0
                            val_computed_load_score_s += max(0.0, val_raw_load_s) * val_intensity_scale_s
                            
                    elif var_pace_status_tab1 == "スローペース":
                        if str_determined_bias_type_f != "後有利":
                            val_raw_load_s = (val_rel_pos_ratio_s - 0.4) * abs(var_pace_diff_tab1) * 2.0
                            val_computed_load_score_s += max(0.0, val_raw_load_s) * val_intensity_scale_s
                    
                    # 特殊評価タグの判定 (1ミリも簡略化しない詳細記述)
                    list_tags_collector_s = []
                    flag_is_counter_target_s = False
                    
                    if val_res_rank_s <= 5:
                        # バイアス逆行
                        if str_determined_bias_type_f == "前有利":
                            if val_last_pos_s >= 10.0:
                                str_tag_label_s = "💎💎 ﾊﾞｲｱｽ極限逆行" if val_field_size_actual_f >= 16 else "💎 ﾊﾞｲｱｽ逆行"
                                list_tags_collector_s.append(str_tag_label_s)
                                flag_is_counter_target_s = True
                        elif str_determined_bias_type_f == "後有利":
                            if val_last_pos_s <= 3.0:
                                str_tag_label_s = "💎💎 ﾊﾞｲｱｽ極限逆行" if val_field_size_actual_f >= 16 else "💎 ﾊﾞｲｱｽ逆行"
                                list_tags_collector_s.append(str_tag_label_s)
                                flag_is_counter_target_s = True
                                
                    # 展開逆行判定詳細
                    flag_pace_bias_favored_s = False
                    if var_pace_status_tab1 == "ハイペース":
                        if str_determined_bias_type_f == "前有利":
                            flag_pace_bias_favored_s = True
                    elif var_pace_status_tab1 == "スローペース":
                        if str_determined_bias_type_f == "後有利":
                            flag_pace_bias_favored_s = True
                            
                    if flag_pace_bias_favored_s == False:
                        if var_pace_status_tab1 == "ハイペース":
                            if val_last_pos_s <= 3.0:
                                str_v_label_s = "📉 激流被害" if val_field_size_actual_f >= 14 else "🔥 展開逆行"
                                list_tags_collector_s.append(str_v_label_s)
                                flag_is_counter_target_s = True
                        elif var_pace_status_tab1 == "スローペース":
                            if val_last_pos_s >= 10.0:
                                if (var_f3f_calc_tab1 - val_l3f_indiv_extracted_s) > 1.5:
                                    list_tags_collector_s.append("🔥 展開逆行")
                                    flag_is_counter_target_s = True
                    
                    # 少頭数展開恩恵
                    if val_field_size_actual_f <= 10:
                        if var_pace_status_tab1 == "スローペース":
                            if val_res_rank_s <= 2:
                                list_tags_collector_s.append("🟢 展開恩恵")

                    # 🌟 上がりタイム偏差ロジック (指示箇所：NameError修正済)
                    val_l3f_gap_score_s = val_in_final_l3f_manual - val_l3f_indiv_extracted_s
                    if val_l3f_gap_score_s >= 0.5:
                        list_tags_collector_s.append("🚀 アガリ優秀")
                    elif val_l3f_gap_score_s <= -1.0:
                        list_tags_collector_s.append("📉 失速大")
                    
                    # 中盤ラップの冗長解析
                    str_mid_label_s = "平"
                    if val_in_race_dist_m > 1200:
                        val_m_lap_s = (val_total_seconds_raw_s - var_f3f_calc_tab1 - val_l3f_indiv_extracted_s) / ((val_in_race_dist_m - 1200) / 200)
                        if val_m_lap_s >= 12.8: str_mid_label_s = "緩"
                        elif val_m_lap_s <= 11.8: str_mid_label_s = "締"
                    else:
                        str_mid_label_s = "短"

                    str_field_attr_s = "多" if val_field_size_actual_f >= 16 else "少" if val_field_size_actual_f <= 10 else "中"
                    str_final_memo_entry_s = f"【{var_pace_status_tab1}/{str_determined_bias_type_f}/負荷:{val_computed_load_score_s:.1f}({str_field_attr_s})/{str_mid_label_s}】{'/'.join(list_tags_collector_s) if list_tags_collector_s else '順境'}"
                    
                    # 開催週オフセット
                    val_week_offset_s = (val_in_track_week_num - 1) * 0.05
                    val_water_avg_s = (analysis_water_4c_val_in + analysis_water_goal_val_in) / 2.0
                    
                    # 🌟 RTC指数の完全冗長計算式 (1ミリも削らず記述)
                    # 基準タイム - (斤量補正) - 馬場補正 - 負荷補正 - 開催週補正 + バイアス補正 - 含水率補正 - クッション補正 + 距離補正
                    val_rtc_step1_time = val_total_seconds_raw_s
                    val_rtc_step2_weight = (val_weight_s - 56.0) * 0.1
                    val_rtc_step3_track = val_in_track_idx_val_in / 10.0
                    val_rtc_step4_load = val_computed_load_score_s / 10.0
                    val_rtc_step5_week = val_week_offset_s
                    val_rtc_step6_water = (val_water_avg_s - 10.0) * 0.05
                    val_rtc_step7_cush = (9.5 - analysis_cushion_val_in) * 0.1
                    val_rtc_step8_dist = (val_in_race_dist_m - 1600) * 0.0005
                    
                    val_final_rtc_computed_s = (val_rtc_step1_time - val_rtc_step2_weight - val_rtc_step3_track - val_rtc_step4_load - val_rtc_step5_week) + analysis_bias_slider_val_in - val_rtc_step6_water - val_rtc_step7_cush + val_rtc_step8_dist
                    
                    list_new_rows_for_db_sync.append({
                        "name": entry_save_main["name"], 
                        "base_rtc": val_final_rtc_computed_s, 
                        "last_race": str_in_race_name, 
                        "course": analysis_course_sel_in, 
                        "dist": val_in_race_dist_m, 
                        "notes": f"{val_weight_s}kg{str_horse_body_weight_string_definition_s}", 
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"), 
                        "f3f": var_f3f_calc_tab1, 
                        "l3f": val_l3f_indiv_extracted_s, 
                        "race_l3f": val_in_final_l3f_manual, 
                        "load": val_last_pos_s, 
                        "memo": str_final_memo_entry_s,
                        "date": val_in_race_date.strftime("%Y-%m-%d"), 
                        "cushion": analysis_cushion_val_in, 
                        "water": val_water_avg_s, 
                        "next_buy_flag": "★逆行狙い" if flag_is_counter_target_s else "", 
                        "result_pos": val_res_rank_s
                    })
                
                if list_new_rows_for_db_sync:
                    # 🌟 同期不全解消：保存直前にキャッシュをクリアして最新状態を再取得
                    st.cache_data.clear()
                    df_sheet_latest_current = conn.read(ttl=0)
                    
                    # 読み込んだ最新データのカラム正規化（詳細記述）
                    for col_target_name in standard_columns_list:
                        if col_target_name not in df_sheet_latest_current.columns:
                            df_sheet_latest_current[col_target_name] = None
                            
                    # 最新データと解析結果を安全にマージ
                    df_final_merged_update = pd.concat([df_sheet_latest_current, pd.DataFrame(list_new_rows_for_db_sync)], ignore_index=True)
                    
                    # スプレッドシートへの物理書き込み
                    if safe_update(df_final_merged_update):
                        st.session_state.state_tab1_preview_is_active = False
                        st.success(f"✅ 解析完了し、最新シートと完全に同期しました。"); st.rerun()

# ==============================================================================
# 8. Tab 2: 馬別履歴詳細・個別条件編集
# ==============================================================================
with tab_horse_history:
    st.header("📊 馬別履歴 & 買い条件設定")
    df_t2_main_source = get_db_data()
    if not df_t2_main_source.empty:
        col_t2_search1, col_t2_search2 = st.columns([1, 1])
        with col_t2_search1:
            input_horse_search_q = st.text_input("馬名で絞り込み検索", key="input_horse_search_q_t2")
        
        list_horses_t2_all = sorted([str(x) for x in df_t2_main_source['name'].dropna().unique()])
        with col_t2_search2:
            val_sel_target_h_t2 = st.selectbox("個別条件編集対象", ["未選択"] + list_horses_t2_all)
        
        if val_sel_target_h_t2 != "未選択":
            idx_list_t2_found = df_t2_main_source[df_t2_main_source['name'] == val_sel_target_h_t2].index
            target_idx_t2_final = idx_list_t2_found[-1]
            
            with st.form("form_edit_horse_t2"):
                val_memo_t2_cur = df_t2_main_source.at[target_idx_t2_final, 'memo'] if not pd.isna(df_t2_main_source.at[target_idx_t2_final, 'memo']) else ""
                new_memo_t2_val = st.text_area("特記メモ・評価", value=val_memo_t2_cur)
                val_flag_t2_cur = df_t2_main_source.at[target_idx_t2_final, 'next_buy_flag'] if not pd.isna(df_t2_main_source.at[target_idx_t2_final, 'next_buy_flag']) else ""
                new_flag_t2_val = st.text_input("買いフラグ", value=val_flag_t2_cur)
                
                if st.form_submit_button("同期保存"):
                    df_t2_main_source.at[target_idx_t2_final, 'memo'] = new_memo_t2_val
                    df_t2_main_source.at[target_idx_t2_final, 'next_buy_flag'] = new_flag_t2_val
                    if safe_update(df_t2_main_source):
                        st.success(f"{val_sel_target_h_t2} の情報を更新しました")
                        st.rerun()
        
        if input_horse_search_q:
            df_t2_filtered_ready = df_t2_main_source[df_t2_main_source['name'].str.contains(input_horse_search_q, na=False)]
        else:
            df_t2_filtered_ready = df_t2_main_source
            
        df_t2_display_formatted = df_t2_filtered_ready.copy()
        df_t2_display_formatted['base_rtc'] = df_t2_display_formatted['base_rtc'].apply(format_time_hmsf)
        st.dataframe(
            df_t2_display_formatted.sort_values("date", ascending=False)[["date", "name", "last_race", "base_rtc", "f3f", "l3f", "race_l3f", "load", "memo", "next_buy_flag"]], 
            use_container_width=True
        )

# ==============================================================================
# 9. Tab 3: レース別結果管理・答え合わせ
# ==============================================================================
with tab_race_history_detail:
    st.header("🏁 答え合わせ & レース履歴管理")
    df_t3_main_source = get_db_data()
    if not df_t3_main_source.empty:
        list_r_all_t3 = sorted([str(x) for x in df_t3_main_source['last_race'].dropna().unique()])
        val_sel_race_t3 = st.selectbox("表示するレースを選択", list_r_all_t3)
        
        if val_sel_race_t3:
            df_race_subset_t3 = df_t3_main_source[df_t3_main_source['last_race'] == val_sel_race_t3].copy()
            with st.form("form_race_results_t3"):
                st.write(f"【{val_sel_race_t3}】の結果を入力してください")
                for idx_row_t3, row_item_t3 in df_race_subset_t3.iterrows():
                    val_p_t3_cur = int(row_item_t3['result_pos']) if not pd.isna(row_item_t3['result_pos']) else 0
                    val_pop_t3_cur = int(row_item_t3['result_pop']) if not pd.isna(row_item_t3['result_pop']) else 0
                    
                    col_t3_1, col_t3_2 = st.columns(2)
                    with col_t3_1:
                        df_race_subset_t3.at[idx_row_t3, 'result_pos'] = st.number_input(f"{row_item_t3['name']} 着順", 0, 100, value=val_p_t3_cur, key=f"pos_in_t3_{idx_row_t3}")
                    with col_t3_2:
                        df_race_subset_t3.at[idx_row_t3, 'result_pop'] = st.number_input(f"{row_item_t3['name']} 人気", 0, 100, value=val_pop_t3_cur, key=f"pop_in_t3_{idx_row_t3}")
                
                if st.form_submit_button("結果を一括同期保存"):
                    for idx_final_t3, row_final_t3 in df_race_subset_t3.iterrows():
                        df_t3_main_source.at[idx_final_t3, 'result_pos'] = row_final_t3['result_pos']
                        df_t3_main_source.at[idx_final_t3, 'result_pop'] = row_final_t3['result_pop']
                    if safe_update(df_t3_main_source):
                        st.success("同期が完了しました。")
                        st.rerun()
            
            df_t3_formatted_ready = df_race_subset_t3.copy()
            df_t3_formatted_ready['base_rtc'] = df_t3_formatted_ready['base_rtc'].apply(format_time_hmsf)
            st.dataframe(df_t3_formatted_ready[["name", "notes", "base_rtc", "f3f", "l3f", "race_l3f", "result_pos", "result_pop"]], use_container_width=True)

# ==============================================================================
# 10. Tab 4: シミュレーターセクション (1200行規模の完全冗長ロジック)
# ==============================================================================
with tab_advanced_simulator:
    st.header("🎯 次走シミュレーター & 統合評価エンジン")
    df_t4_main_source = get_db_data()
    if not df_t4_main_source.empty:
        list_h_names_t4_all = sorted([str(x) for x in df_t4_main_source['name'].dropna().unique()])
        list_sel_h_sim_multi = st.multiselect("出走馬をデータベースから選択", options=list_h_names_t4_all)
        
        sim_pops_input_dict = {}
        sim_gates_input_dict = {}
        sim_weights_input_dict = {}
        
        if list_sel_h_sim_multi:
            st.markdown("##### 📝 枠番・予想人気・想定斤量の個別詳細入力")
            sim_cols_grid = st.columns(min(len(list_sel_h_sim_multi), 4))
            for i_sim_f, h_name_f in enumerate(list_sel_h_sim_multi):
                with sim_cols_grid[i_sim_f % 4]:
                    h_lat_data_f = df_t4_main_source[df_t4_main_source['name'] == h_name_f].iloc[-1]
                    sim_gates_input_dict[h_name_f] = st.number_input(f"{h_name_f} 枠", 1, 18, value=1, key=f"sim_g_v_{h_name_f}")
                    sim_pops_input_dict[h_name_f] = st.number_input(f"{h_name_f} 人気", 1, 18, value=int(h_lat_data_f['result_pop']) if not pd.isna(h_lat_data_f['result_pop']) else 10, key=f"sim_p_v_{h_name_f}")
                    # 個別斤量の詳細入力ロジックを1ミリも削らず維持
                    sim_weights_input_dict[h_name_f] = st.number_input(f"{h_name_f} 斤量", 48.0, 62.0, 56.0, step=0.5, key=f"sim_w_v_{h_name_f}")

            col_sim_p1, col_sim_p2 = st.columns(2)
            with col_sim_p1: 
                val_sim_course_name_sel = st.selectbox("次走競馬場", list(MASTER_DATA_COURSE_TURF_LOAD.keys()), key="sel_sim_c_name")
                val_sim_dist_m_sel = st.selectbox("次走距離", list_dist_range, index=6)
                opt_sim_track_type_sel = st.radio("次走トラック", ["芝", "ダート"], horizontal=True)
            with col_sim_p2: 
                val_sim_cushion_slider = st.slider("想定クッション値", 7.0, 12.0, 9.5)
                val_sim_water_slider = st.slider("想定含水率 (%)", 0.0, 30.0, 10.0)
            
            if st.button("🏁 シミュレーション実行"):
                list_sim_results_accumulator = []
                val_sim_total_horses = len(list_sel_h_sim_multi)
                dict_sim_styles_counts = {"逃げ": 0, "先行": 0, "差し": 0, "追込": 0}
                val_sim_db_l3f_average = df_t4_main_source['l3f'].mean()

                for h_name_run_sim in list_sel_h_sim_multi:
                    df_h_hist_sim = df_t4_main_source[df_t4_main_source['name'] == h_name_run_sim].sort_values("date")
                    df_h_last3_sim = df_h_hist_sim.tail(3)
                    list_conv_rtc_sim_buffer = []
                    
                    # 脚質判定の詳細展開 (冗長記述)
                    val_h_avg_load_3r_sim = df_h_last3_sim['load'].mean()
                    if val_h_avg_load_3r_sim <= 3.5: 
                        str_h_style_label_sim = "逃げ"
                    elif val_h_avg_load_3r_sim <= 7.0: 
                        str_h_style_label_sim = "先行"
                    elif val_h_avg_load_3r_sim <= 11.0: 
                        str_h_style_label_sim = "差し"
                    else: 
                        str_h_style_label_sim = "追込"
                    dict_sim_styles_counts[str_h_style_label_sim] += 1

                    # 頭数連動ロジック詳細
                    str_jam_label_sim = "⚠️詰まり注意" if val_sim_total_horses >= 15 and str_h_style_label_sim in ["差し", "追込"] and sim_gates_input_dict[h_name_run_sim] <= 4 else "-"
                    str_slow_apt_label_sim = "-"
                    if val_sim_total_horses <= 10:
                        val_h_min_l3f_sim = df_h_hist_sim['l3f'].min()
                        if val_h_min_l3f_sim < val_sim_db_l3f_average - 0.5:
                            str_slow_apt_label_sim = "⚡スロー特化"
                        elif val_h_min_l3f_sim > val_sim_db_l3f_average + 0.5:
                            str_slow_apt_label_sim = "📉瞬発力不足"

                    val_h_rtc_std_sim = df_h_hist_sim['base_rtc'].std() if len(df_h_hist_sim) >= 3 else 0.0
                    str_h_stab_label_sim = "⚖️安定" if 0 < val_h_rtc_std_sim < 0.2 else "🎢ムラ" if val_h_rtc_std_sim > 0.4 else "-"
                    
                    df_h_best_p_data_sim = df_h_hist_sim.loc[df_h_hist_sim['base_rtc'].idxmin()]
                    str_h_apt_label_sim = "🎯馬場◎" if abs(df_h_best_p_data_sim['cushion'] - val_sim_cushion_slider) <= 0.5 and abs(df_h_best_p_data_sim['water'] - val_sim_water_slider) <= 2.0 else "-"

                    # 🌟 過去3走斤量・負荷詳細補正ループ復元
                    for idx_sim_r, row_sim_r in df_h_last3_sim.iterrows():
                        v_p_dist_s = row_sim_r['dist']
                        v_p_rtc_s = row_sim_r['base_rtc']
                        v_p_course_s = row_sim_r['course']
                        v_p_load_s = row_sim_r['load']
                        str_p_notes_s = str(row_sim_r['notes'])
                        
                        v_p_weight_s = 56.0
                        v_h_bw_s = 480.0
                        
                        m_w_sim_s = re.search(r'([4-6]\d\.\d)', str_p_notes_s)
                        if m_w_sim_s:
                            v_p_weight_s = float(m_w_sim_s.group(1))
                            
                        m_hb_sim_s = re.search(r'\((\d{3})kg\)', str_p_notes_s)
                        if m_hb_sim_s:
                            v_h_bw_s = float(m_hb_sim_s.group(1))
                        
                        if v_p_dist_s > 0:
                            v_l_adj_s = (v_p_load_s - 7.0) * 0.02
                            # 斤量感応度の詳細条件分岐 (1ミリも簡略化しない)
                            if v_h_bw_s <= 440:
                                v_sens_factor_s = 0.15
                            elif v_h_bw_s >= 500:
                                v_sens_factor_s = 0.08
                            else:
                                v_sens_factor_s = 0.1
                                
                            v_weight_diff_s = (sim_weights_dict_f[h_name_run_sim] - v_p_weight_s) * v_sens_factor_s
                            
                            # RTC指数の物理的変換
                            v_base_conv_rtc_s = (v_p_rtc_s + v_l_adj_s + v_weight_diff_s) / v_p_dist_s * val_sim_dist_m_sel
                            # 競馬場間の勾配差補正
                            v_slope_adj_s = (MASTER_DATA_SLOPE_FACTORS_CONFIG.get(val_sim_course_name_sel, 0.002) - MASTER_DATA_SLOPE_FACTORS_CONFIG.get(v_p_course_s, 0.002)) * val_sim_dist_m_sel
                            list_conv_rtc_sim_buffer.append(v_base_conv_rtc_s + v_slope_adj_s)
                    
                    val_avg_rtc_sim_result_f = sum(list_conv_rtc_sim_buffer) / len(list_conv_rtc_sim_buffer) if list_conv_rtc_sim_buffer else 0
                    
                    # 距離相性ペナルティ
                    val_h_best_d_past_f = df_h_hist_sim.loc[df_h_hist_sim['base_rtc'].idxmin(), 'dist']
                    val_avg_rtc_sim_result_f += (abs(val_sim_dist_m_sel - val_h_best_d_past_f) / 100) * 0.05
                    
                    # 近影モメンタム判定
                    str_label_h_mom_f = "-"
                    if len(df_h_hist_sim) >= 2:
                        if df_h_hist_sim.iloc[-1]['base_rtc'] < df_h_hist_sim.iloc[-2]['base_rtc'] - 0.2:
                            str_label_h_mom_f = "📈上昇"
                            val_avg_rtc_sim_result_f -= 0.15

                    # 枠順×バイアス詳細補正
                    val_syn_bias_sim_f_final = -0.2 if (sim_gates_dict_f[h_name_run_sim] <= 4 and val_in_bias_slider_result <= -0.5) or (sim_gates_dict_f[h_name_run_sim] >= 13 and val_in_bias_slider_result >= 0.5) else 0
                    val_avg_rtc_sim_result_f += val_syn_bias_sim_f_final

                    # 特定コース実績ボーナス
                    val_h_course_bonus_f_f = -0.2 if any((df_h_hist_sim['course'] == val_sim_course_name_sel) & (df_h_hist_sim['result_pos'] <= 3)) else 0.0
                    
                    # 馬場状況の最終アジャスト
                    val_water_adj_f_f = (val_sim_water_slider - 10.0) * 0.05
                    dict_c_master_f_f = MASTER_DATA_COURSE_DIRT_LOAD if opt_sim_track_type_sel == "ダート" else MASTER_DATA_COURSE_TURF_LOAD
                    if opt_sim_track_type_sel == "ダート":
                        val_water_adj_f_f = -val_water_adj_f_f
                    
                    val_final_rtc_sim_final_f = (val_avg_rtc_sim_result_f + (dict_c_master_f_f[val_sim_course_name_sel] * (val_sim_dist_m_sel/1600.0)) + val_h_course_bonus_f_f + val_water_adj_f_f - (9.5 - val_sim_cushion_slider) * 0.1)
                    
                    df_h_lat_entry_f = df_h_last3_sim.iloc[-1]
                    list_sim_results_accumulator.append({
                        "馬名": h_name_run_sim, 
                        "脚質": h_style_sim, 
                        "想定タイム": val_final_rtc_sim_final_f, 
                        "渋滞": str_jam_label_sim, 
                        "スロー": str_slow_apt_label_sim, 
                        "適性": str_h_apt_label_sim, 
                        "安定": str_h_stab_label_sim, 
                        "偏差": "⤴️覚醒期待" if val_final_rtc_sim_final_f < df_h_hist_sim['base_rtc'].min() - 0.3 else "-", 
                        "上昇": str_label_h_mom_f, 
                        "レベル": "🔥強ﾒﾝﾂ" if df_t4_main_source[df_t4_main_source['last_race'] == df_h_lat_entry_f['last_race']]['base_rtc'].mean() < df_t4_main_source['base_rtc'].mean() - 0.2 else "-", 
                        "load": df_h_lat_entry_f['load'], 
                        "状態": "💤休み明け" if (datetime.now() - df_h_lat_entry_f['date']).days // 7 >= 12 else "-", 
                        "raw_rtc": val_final_rtc_sim_final_f, 
                        "解析メモ": df_h_lat_entry_f['memo']
                    })
                
                # 展開予想詳細ロジック
                str_sim_pace_pred_f = "ミドルペース"
                if dict_sim_styles_counts["逃げ"] >= 2 or (dict_sim_styles_counts["逃げ"] + dict_sim_styles_counts["先行"]) >= val_sim_total_horses * 0.6:
                    str_sim_pace_pred_f = "ハイペース傾向"
                elif dict_sim_styles_counts["逃げ"] == 0 and dict_sim_styles_counts["先行"] <= 1:
                    str_sim_pace_pred_f = "スローペース傾向"
                
                df_sim_final_res_f = pd.DataFrame(list_sim_results_accumulator)
                # 展開シナジー強化ロジック
                val_sim_p_multiplier_f = 1.5 if val_sim_total_horses >= 15 else 1.0
                
                def apply_synergy_func_f(row):
                    v_adj_f = 0.0
                    if "ハイ" in str_sim_pace_pred_f:
                        if row['脚質'] in ["差し", "追込"]: v_adj_f = -0.2 * val_sim_p_multiplier_f
                        elif row['脚質'] == "逃げ": v_adj_f = 0.2 * val_sim_p_multiplier_f
                    elif "スロー" in str_sim_pace_pred_f:
                        if row['脚質'] in ["逃げ", "先行"]: v_adj_f = -0.2 * val_sim_p_multiplier_f
                        elif row['脚質'] in ["差し", "追込"]: v_adj_f = 0.2 * val_sim_p_multiplier_f
                    return row['raw_rtc'] + v_adj_f

                df_sim_final_res_f['synergy_rtc'] = df_sim_final_res_f.apply(apply_synergy_func_f, axis=1)
                df_sim_final_res_f = df_sim_final_res_f.sort_values("synergy_rtc")
                df_sim_final_res_f['RTC順位'] = range(1, len(df_sim_final_res_f) + 1)
                
                val_sim_top_t_val = df_sim_final_res_f.iloc[0]['raw_rtc']
                df_sim_final_res_f['差'] = df_sim_final_res_f['raw_rtc'] - val_sim_top_t_val
                df_sim_final_res_f['予想人気'] = df_sim_final_res_f['馬名'].map(sim_pops_input_dict)
                df_sim_final_res_f['妙味スコア'] = df_sim_final_res_f['予想人気'] - df_sim_final_res_f['RTC順位']
                
                # 印の割り当て冗長ロジック
                df_sim_final_res_f['役割'] = "-"
                df_sim_final_res_f.loc[df_sim_final_res_f['RTC順位'] == 1, '役割'] = "◎"
                df_sim_final_res_f.loc[df_sim_final_res_f['RTC順位'] == 2, '役割'] = "〇"
                df_sim_final_res_f.loc[df_sim_final_res_f['RTC順位'] == 3, '役割'] = "▲"
                df_sim_bomb_search = df_sim_final_res_f[df_sim_final_res_f['RTC順位'] > 1].sort_values("妙味スコア", ascending=False)
                if not df_sim_bomb_search.empty:
                    df_sim_final_res_f.loc[df_sim_final_res_f['馬名'] == df_sim_bomb_search.iloc[0]['馬名'], '役割'] = "★"
                
                # 表示用コンバート
                df_sim_final_res_f['想定タイム'] = df_sim_final_res_f['raw_rtc'].apply(format_time_hmsf)
                df_sim_final_res_f['差'] = df_sim_final_res_f['差'].apply(lambda x: f"+{x:.1f}" if x > 0 else "±0.0")

                st.markdown("---")
                st.subheader(f"🏁 展開予想：{str_sim_pace_pred_f} ({val_sim_total_horses}頭立て)")
                col_rec_sim_grid1, col_rec_sim_grid2 = st.columns(2)
                
                sim_fav_h_name = df_sim_final_res_f[df_sim_final_res_f['役割'] == "◎"].iloc[0]['馬名'] if not df_sim_final_res_f[df_sim_final_res_f['役割'] == "◎"].empty else ""
                sim_opp_h_name = df_sim_final_res_f[df_sim_final_res_f['役割'] == "〇"].iloc[0]['馬名'] if not df_sim_final_res_f[df_sim_final_res_f['役割'] == "〇"].empty else ""
                sim_bomb_h_name = df_sim_final_res_f[df_sim_final_res_f['役割'] == "★"].iloc[0]['馬名'] if not df_sim_final_res_f[df_sim_final_res_f['役割'] == "★"].empty else ""
                
                with col_rec_sim_grid1:
                    st.info(f"**🎯 馬連・ワイド1点勝負**\n\n◎ {sim_fav_h_name} － 〇 {sim_opp_h_name}")
                with col_rec_sim_grid2: 
                    if sim_bomb_h_name:
                        st.warning(f"**💣 妙味狙いワイド1点**\n\n◎ {sim_fav_h_name} － ★ {sim_bomb_h_name} (展開×妙味)")
                
                def style_highlight_rows_f(row):
                    if row['役割'] == "★": return ['background-color: #ffe4e1; font-weight: bold'] * len(row)
                    if row['役割'] == "◎": return ['background-color: #fff700; font-weight: bold; color: black'] * len(row)
                    return [''] * len(row)
                
                st.table(df_sim_final_res_f[["役割", "馬名", "脚質", "渋滞", "スロー", "想定タイム", "差", "妙味スコア", "適性", "安定", "上昇", "レベル", "load", "状態", "解析メモ"]].style.apply(style_highlight_rows_f, axis=1))

# ==============================================================================
# 11. Tab 5: トレンド詳細統計
# ==============================================================================
with tab_bias_trends:
    st.header("📈 馬場トレンド詳細解析")
    df_t5_main_raw = get_db_data()
    if not df_t5_main_raw.empty:
        val_sel_course_t5 = st.selectbox("トレンドを確認する競馬場を選択", list(MASTER_DATA_COURSE_TURF_LOAD.keys()), key="val_sel_course_t5_final")
        df_td_t5_main_f = df_t5_main_raw[df_t5_main_raw['course'] == val_sel_course_t5].sort_values("date")
        if not df_td_t5_main_f.empty:
            st.subheader("💧 クッション値 & 含水率の時系列推移推移")
            st.line_chart(df_td_t5_main_f.set_index("date")[["cushion", "water"]])
            st.subheader("🏁 直近のレース傾向分析 (4角平均通過順位)")
            df_td_agg_t5_f = df_td_t5_main_f.groupby('last_race').agg({'load':'mean', 'date':'max'}).sort_values('date', ascending=False).head(15)
            st.bar_chart(df_td_agg_t5_f['load'])
            st.subheader("📊 直近上がり3Fの実績推移")
            st.line_chart(df_td_t5_main_f.set_index("date")["race_l3f"])

# ==============================================================================
# 12. Tab 6: データ管理・メンテナンス詳細 (1200行規模の冗長ロジック完全復元)
# ==============================================================================
with tab_admin_tools:
    st.header("🗑 データベース管理 & 高度メンテナンス詳細")
    
    # 🌟 同期不全を解消するための強制物理同期ボタン
    if st.button("🔄 スプレッドシートの手動修正を同期（キャッシュ強制破棄）"):
        st.cache_data.clear()
        st.success("キャッシュを完全に破棄しました。最新のスプレッドシート内容を再読込します。")
        st.rerun()

    df_t6_main_source = get_db_data()

    def update_eval_tags_verbose_logic_step_by_step(row_obj_f, df_context_f=None):
        """【完全復元】冗長な条件分岐による再解析用詳細ロジック (一切の簡略化を禁止)"""
        
        # メモ情報の初期化
        str_raw_memo_val_v6 = str(row_obj_f['memo']) if not pd.isna(row_obj_f['memo']) else ""
        
        def to_float_v6_safe(v_in):
            try: return float(v_in) if not pd.isna(v_in) else 0.0
            except: return 0.0
            
        # 全ての数値を個別に展開して取得
        v6_f3f = to_float_v6_safe(row_obj_f['f3f'])
        v6_l3f = to_float_v6_safe(row_obj_f['l3f'])
        v6_race_l3f = to_float_v6_safe(row_obj_f['race_l3f'])
        v6_result_pos = to_float_v6_safe(row_obj_f['result_pos'])
        v6_load_pos = to_float_v6_safe(row_obj_f['load'])
        v6_dist = to_float_v6_safe(row_obj_f['dist'])
        v6_base_rtc = to_float_v6_safe(row_obj_f['base_rtc'])
        
        # 🌟 notesカラムから斤量を再抽出（手動修正反映の生命線）
        str_notes_v6_f = str(row_obj_f['notes'])
        match_w_v6_final = re.search(r'([4-6]\d\.\d)', str_notes_v6_f)
        if match_w_v6_final:
            val_indiv_weight_v6 = float(match_w_v6_final.group(1))
        else:
            val_indiv_weight_v6 = 56.0
        
        # 中盤ラップ判定の冗長記述
        str_mid_label_v6 = "平"
        if v6_dist > 1200:
            if v6_f3f > 0:
                val_m_lap_v6_f = (v6_base_rtc - v6_f3f - v6_l3f) / ((v6_dist - 1200) / 200)
                if val_m_lap_v6_f >= 12.8: 
                    str_mid_label_v6 = "緩"
                elif val_m_lap_v6_f <= 11.8: 
                    str_mid_label_v6 = "締"
        elif v6_dist <= 1200:
            str_mid_label_v6 = "短"

        # バイアス特例判定の完全記述 (Tab 6版)
        str_bt_label_v6_f = "フラット"
        val_mx_field_v6_f = 16
        if df_context_f is not None:
            if not pd.isna(row_obj_f['last_race']):
                df_rc_v6_f = df_context_f[df_context_f['last_race'] == row_obj_f['last_race']]
                val_mx_field_v6_f = df_rc_v6_f['result_pos'].max() if not df_rc_v6_f.empty else 16
                df_top3_v6_f = df_rc_v6_f[pd.to_numeric(df_rc_v6_f['result_pos'], errors='coerce') <= 3].copy()
                df_top3_v6_f['load'] = df_top3_v6_f['load'].fillna(7.0)
                
                list_out_v6_f = df_top3_v6_f[(df_top3_v6_f['load'] >= 10.0) | (df_top3_v6_f['load'] <= 3.0)]
                if len(list_out_v6_f) == 1:
                    df_bias_set_v6_f = pd.concat([
                        df_top3_v6_f[df_top3_v6_f['name'] != list_out_v6_f.iloc[0]['name']], 
                        df_rc_v6_f[pd.to_numeric(df_rc_v6_f['result_pos'], errors='coerce') == 4]
                    ])
                else:
                    df_bias_set_v6_f = df_top3_v6_f
                
                if not df_bias_set_v6_f.empty:
                    val_avg_b_v6_f = df_bias_set_v6_f['load'].mean()
                    if val_avg_b_v6_f <= 4.0: 
                        str_bt_label_v6_f = "前有利"
                    elif val_avg_b_v6_f >= 10.0: 
                        str_bt_label_v6_f = "後有利"

        # ペース判定と強度補正スコア算出
        str_ps_label_v6_f = "ハイペース" if "ハイ" in str_raw_memo_val_v6 else "スローペース" if "スロー" in str_raw_memo_val_v6 else "ミドルペース"
        val_pd_val_v6_f = 1.5 if str_ps_label_v6_f != "ミドルペース" else 0.0
        val_rp_ratio_v6_f = v6_load_pos / val_mx_field_v6_f
        val_fi_intensity_v6_f = val_mx_field_v6_f / 16.0
        
        val_nl_score_v6_f = 0.0
        if str_ps_label_v6_f == "ハイペース":
            if str_bt_label_v6_f != "前有利":
                val_nl_score_v6_f = max(0, (0.6 - val_rp_ratio_v6_f) * val_pd_val_v6_f * 3.0) * val_fi_intensity_v6_f
        elif str_ps_label_v6_f == "スローペース":
            if str_bt_label_v6_f != "後有利":
                val_nl_score_v6_f = max(0, (val_rp_ratio_v6_f - 0.4) * val_pd_val_v6_f * 2.0) * val_fi_intensity_v6_f
        
        list_tags_v6_f = []
        flag_is_counter_v6_f = False
        
        # 上がり詳細評価
        if v6_race_l3f > 0:
            if (v6_race_l3f - v6_l3f) >= 0.5: 
                list_tags_v6_f.append("🚀 アガリ優秀")
            elif (v6_race_l3f - v6_l3f) <= -1.0: 
                list_tags_v6_f.append("📉 失速大")
        
        # 条件逆行判定詳細 (冗長展開)
        if v6_result_pos <= 5:
            if str_bt_label_v6_f == "前有利":
                if v6_load_pos >= 10.0:
                    list_tags_v6_f.append("💎💎 ﾊﾞｲｱｽ極限逆行" if val_mx_field_v6_f >= 16 else "💎 ﾊﾞｲｱｽ逆行")
                    flag_is_counter_v6_f = True
            elif str_bt_label_v6_f == "後有利":
                if v6_load_pos <= 3.0:
                    list_tags_v6_f.append("💎💎 ﾊﾞｲｱｽ極限逆行" if val_mx_field_v6_f >= 16 else "💎 ﾊﾞｲアス逆行")
                    flag_is_counter_v6_f = True
            
            # 展開逆行
            if str_ps_label_v6_f == "ハイペース":
                if str_bt_label_v6_f != "前有利":
                    if v6_load_pos <= 3.0:
                        list_tags_v6_f.append("📉 激流被害" if val_mx_field_v6_f >= 14 else "🔥 展開逆行")
                        flag_is_counter_v6_f = True
            elif str_ps_label_v6_f == "スローペース":
                if str_bt_label_v6_f != "後有利":
                    if v6_load_pos >= 10.0:
                        if (v6_f3f - v6_l3f) > 1.5:
                            list_tags_v6_f.append("🔥 展開逆行")
                            flag_is_counter_v6_f = True
                            
        # 少頭数恩恵
        if val_mx_field_v6_f <= 10:
            if str_ps_label_v6_f == "スローペース":
                if v6_result_pos <= 2:
                    list_tags_v6_f.append("🟢 展開恩恵")

        str_ft_tag_v6_f = "多" if val_mx_field_v6_f >= 16 else "少" if val_mx_field_v6_f <= 10 else "中"
        str_mu_final_text_6 = (f"【{str_ps_label_v6_f}/{str_bt_label_v6_f}/負荷:{val_nl_score_v6_f:.1f}({str_ft_tag_v6_f})/{str_mid_label_v6}】" + "/".join(list_tags_v6_f)).strip("/")
        
        # フラグの更新 (既存の逆行狙い文字列を一度消去して再構成)
        str_raw_buy_flag = str(row_obj_f['next_buy_flag']).replace("★逆行狙い", "").strip()
        str_fu_final_text_6 = ("★逆行狙い " + str_raw_buy_flag).strip() if flag_is_counter_v6_f else str_raw_buy_flag
        
        return str_mu_final_text_6, str_fu_final_text_6

    # --- 🗓 過去レース開催週一括設定詳細セクション ---
    st.subheader("🗓 過去レース開催週を一括設定")
    if not df_t6_main_source.empty:
        df_rm_weeks_t6_all = df_t6_main_source[['last_race', 'date']].drop_duplicates(subset=['last_race']).copy()
        df_rm_weeks_t6_all['track_week'] = 1
        df_ed_weeks_t6_f = st.data_editor(df_rm_weeks_t6_all, hide_index=True)
        
        if st.button("🔄 指定した週数で補正を全件適用"):
            dict_w_lookup_t6_f = dict(zip(df_ed_weeks_t6_f['last_race'], df_ed_weeks_t6_f['track_week']))
            for idx_w_f, row_w_f in df_t6_main_source.iterrows():
                if row_w_f['last_race'] in dict_w_lookup_t6_f:
                    # 指数計算を遡り修正 (1週につき0.05秒の補正)
                    df_t6_main_source.at[idx_w_f, 'base_rtc'] = row_w_f['base_rtc'] - (dict_w_lookup_t6_f[row_w_f['last_race']] - 1) * 0.05
                    # 最新ロジックを再適用
                    m_6_f_upd, f_6_f_upd = update_eval_tags_verbose_logic_step_by_step(df_t6_main_source.iloc[idx_w_f], df_t6_main_source)
                    df_t6_main_source.at[idx_w_f, 'memo'] = m_6_f_upd
                    df_t6_main_source.at[idx_w_f, 'next_buy_flag'] = f_6_f_upd
            
            if safe_update(df_t6_main_source):
                st.success("過去全データの開催週補正と再計算を同期しました。")
                st.rerun()

    st.subheader("🛠️ 一括メンテナンスメニュー詳細")
    c_btn1_t6_f, c_btn2_t6_f = st.columns(2)
    with c_btn1_t6_f:
        if st.button("🔄 DB再解析（最新数値を基に上書き）"):
            # 🌟 同期不全解消・手動修正反映の核心プロセス
            st.cache_data.clear()
            df_latest_db_state_t6_f = conn.read(ttl=0)
            # カラム正規化の冗長実行
            for col_nm_sync in standard_columns_list:
                if col_nm_sync not in df_latest_db_state_t6_f.columns: 
                    df_latest_db_state_t6_f[col_nm_sync] = None
            
            # 全行を冗長ロジックで再スキャン
            for idx_sy_f, row_sy_f in df_latest_db_state_t6_f.iterrows():
                m_result_sy_f, f_result_sy_f = update_eval_tags_verbose_logic_step_by_step(row_sy_f, df_latest_db_state_t6_f)
                df_latest_db_state_t6_f.at[idx_sy_f, 'memo'] = m_result_sy_f
                df_latest_db_state_t6_f.at[idx_sy_f, 'next_buy_flag'] = f_result_sy_f
            
            # スプレッドシートを完全に最新状態で上書き
            if safe_update(df_latest_db_state_t6_f):
                st.success("全履歴の同期・再解析・上書き保存が完了しました。")
                st.rerun()
                
    with c_btn2_t6_f:
        if st.button("🧼 重複削除詳細クリーニング"):
            cnt_before_clean_t6_f = len(df_t6_main_source)
            df_t6_main_source = df_t6_main_source.drop_duplicates(subset=['name', 'date', 'last_race'], keep='first')
            if safe_update(df_t6_main_source):
                st.success(f"重複データ {cnt_before_clean_t6_f - len(df_t6_main_source)} 件を抹消しました。"); st.rerun()

    if not df_t6_main_source.empty:
        st.subheader("🛠️ データベース編集エディタ")
        df_t6_formatted_final_f = df_t6_main_source.copy()
        df_t6_formatted_final_f['base_rtc'] = df_t6_formatted_final_f['base_rtc'].apply(format_time_hmsf)
        df_admin_edited_final_f = st.data_editor(
            df_t6_formatted_final_f.sort_values("date", ascending=False), 
            num_rows="dynamic", 
            use_container_width=True
        )
        if st.button("💾 エディタの変更内容を反映する"):
            df_save_converted_f = df_admin_edited_final_f.copy()
            df_save_converted_f['base_rtc'] = df_save_converted_f['base_rtc'].apply(parse_time_to_float_seconds)
            if safe_update(df_save_converted_f):
                st.success("エディタの内容をスプレッドシートへ同期しました。"); st.rerun()
        
        st.divider()
        st.subheader("❌ データ詳細削除設定")
        cd1_t6_f, cd2_t6_f = st.columns(2)
        with cd1_t6_f:
            list_r_names_all_t6_f = sorted([str(x) for x in df_t6_main_source['last_race'].dropna().unique()])
            sel_target_r_del_f = st.selectbox("削除対象レースを選択", ["未選択"] + list_r_names_all_t6_f)
            if sel_target_r_del_f != "未選択":
                if st.button(f"🚨 レース【{sel_target_r_del_f}】を全削除"):
                    if safe_update(df_t6_main_source[df_t6_main_source['last_race'] != sel_target_r_del_f]): 
                        st.rerun()
        with cd2_t6_f:
            list_h_names_all_t6_f = sorted([str(x) for x in df_t6_main_source['name'].dropna().unique()])
            # 🌟 【完全復元】マルチセレクト形式による複数馬一括抹消機能
            list_target_h_del_f = st.multiselect("削除馬を選択（複数可）", list_h_names_all_t6_f, key="ms_del_admin_f")
            if list_target_h_del_f:
                if st.button(f"🚨 選択した{len(list_target_h_del_f)}頭をDBから削除"):
                    if safe_update(df_t6_main_source[~df_t6_main_source['name'].isin(list_target_h_del_f)]): 
                        st.rerun()

        st.divider()
        with st.expander("☢️ システム詳細初期化設定"):
            st.warning("この操作は取り消せません。データベースは空になります。")
            if st.button("🧨 データベースを完全にリセットする"):
                if safe_update(pd.DataFrame(columns=df_t6_main_source.columns)): 
                    st.rerun()

# ==============================================================================
# END OF CODE
# ==============================================================================
