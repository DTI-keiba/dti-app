import streamlit as st
import pandas as pd
import re
import time
from streamlit_gsheets import GSheetsConnection
from datetime import datetime

# ==============================================================================
# 1. ページ基本構成の詳細定義 (UI Property Specifications)
# ==============================================================================
# このセクションでは、アプリケーションの全体的な外観と基本挙動を定義します。
# ユーザーの要求に基づき、1ミリも削らず、冗長なまでに設定項目を記述します。

# ページ設定の宣言（メタデータ、レイアウト、メニュー項目を詳細に指定）
st.set_page_config(
    page_title="DTI Ultimate DB - The Absolute Master Edition v10.0",
    page_icon="https://cdn.jsdelivr.net/gh/twitter/twemoji@latest/assets/72x72/1f3c7.png",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'Get Help': None,
        'Report a bug': None,
        'About': "DTI Ultimate DB: The complete professional horse racing analysis engine. No data points are ever compromised."
    }
)

# --- データベース接続オブジェクトの物理生成 ---
# Google Sheetsとの通信を司る唯一無二のメインコネクションです。
# 安定稼働を最優先し、グローバルスコープでの一貫性を維持するためにここで定義します。
conn = st.connection("gsheets", type=GSheetsConnection)

# 🌟 データベースの全カラム物理構成定義（グローバル定数化）
# 関数内ローカル変数からグローバル定数へ格上げし、NameErrorを物理的に根絶します。
ABSOLUTE_COLUMN_STRUCTURE_DEFINITION_GLOBAL = [
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
    "next_buy_flag",
    "track_week",
    "race_type",  
    "track_kind",
    "raw_time",    
    "track_idx",   
    "bias_slider"  
]

# ==============================================================================
# 2. データベース読み込み詳細ロジック (整合性チェック & 強制物理同期)
# ==============================================================================

@st.cache_data(ttl=300)
def get_db_data_cached():
    """
    Google Sheetsから全ての蓄積データを取得し、型変換と前処理を「完全非省略」で実行します。
    キャッシュの有効期間(ttl=300)を設けることで、API制限の物理的回避と応答性能を両立させます。
    """
    
    try:
        # 強制読み込み（ttl=0）オプションを使用して、常に最新のシート状態を取得します。
        raw_dataframe_from_sheet = conn.read(ttl=0)
        
        # 取得データがNoneまたは物理的に空である場合の、厳格な安全初期化ロジック。
        if raw_dataframe_from_sheet is None:
            safety_initial_df = pd.DataFrame(columns=ABSOLUTE_COLUMN_STRUCTURE_DEFINITION_GLOBAL)
            return safety_initial_df
            
        if raw_dataframe_from_sheet.empty:
            safety_initial_df = pd.DataFrame(columns=ABSOLUTE_COLUMN_STRUCTURE_DEFINITION_GLOBAL)
            return safety_initial_df
        
        # 🌟 全24カラムの存在チェックと強制的な一括補完（省略禁止・冗長記述の徹底）
        # シート上での手動削除や列の並べ替えによるクラッシュを物理的に防ぎます。
        if "name" not in raw_dataframe_from_sheet.columns:
            raw_dataframe_from_sheet["name"] = None
            
        if "base_rtc" not in raw_dataframe_from_sheet.columns:
            raw_dataframe_from_sheet["base_rtc"] = None
            
        if "last_race" not in raw_dataframe_from_sheet.columns:
            raw_dataframe_from_sheet["last_race"] = None
            
        if "course" not in raw_dataframe_from_sheet.columns:
            raw_dataframe_from_sheet["course"] = None
            
        if "dist" not in raw_dataframe_from_sheet.columns:
            raw_dataframe_from_sheet["dist"] = None
            
        if "notes" not in raw_dataframe_from_sheet.columns:
            raw_dataframe_from_sheet["notes"] = None
            
        if "timestamp" not in raw_dataframe_from_sheet.columns:
            raw_dataframe_from_sheet["timestamp"] = None
            
        if "f3f" not in raw_dataframe_from_sheet.columns:
            raw_dataframe_from_sheet["f3f"] = None
            
        if "l3f" not in raw_dataframe_from_sheet.columns:
            raw_dataframe_from_sheet["l3f"] = None
            
        if "race_l3f" not in raw_dataframe_from_sheet.columns:
            raw_dataframe_from_sheet["race_l3f"] = None
            
        if "load" not in raw_dataframe_from_sheet.columns:
            raw_dataframe_from_sheet["load"] = None
            
        if "memo" not in raw_dataframe_from_sheet.columns:
            raw_dataframe_from_sheet["memo"] = None
            
        if "date" not in raw_dataframe_from_sheet.columns:
            raw_dataframe_from_sheet["date"] = None
            
        if "cushion" not in raw_dataframe_from_sheet.columns:
            raw_dataframe_from_sheet["cushion"] = None
            
        if "water" not in raw_dataframe_from_sheet.columns:
            raw_dataframe_from_sheet["water"] = None
            
        if "result_pos" not in raw_dataframe_from_sheet.columns:
            raw_dataframe_from_sheet["result_pos"] = None
            
        if "result_pop" not in raw_dataframe_from_sheet.columns:
            raw_dataframe_from_sheet["result_pop"] = None
            
        if "next_buy_flag" not in raw_dataframe_from_sheet.columns:
            raw_dataframe_from_sheet["next_buy_flag"] = None
            
        if "track_week" not in raw_dataframe_from_sheet.columns:
            raw_dataframe_from_sheet["track_week"] = 1.0
            
        if "race_type" not in raw_dataframe_from_sheet.columns:
            raw_dataframe_from_sheet["race_type"] = "不明"
            
        if "track_kind" not in raw_dataframe_from_sheet.columns:
            raw_dataframe_from_sheet["track_kind"] = "芝" 
            
        if "raw_time" not in raw_dataframe_from_sheet.columns:
            raw_dataframe_from_sheet["raw_time"] = 0.0
            
        if "track_idx" not in raw_dataframe_from_sheet.columns:
            raw_dataframe_from_sheet["track_idx"] = 0.0
            
        if "bias_slider" not in raw_dataframe_from_sheet.columns:
            raw_dataframe_from_sheet["bias_slider"] = 0.0
            
        # データの型変換（一文字の妥協も許さない詳細なエラー対策）
        if 'date' in raw_dataframe_from_sheet.columns:
            # 日付型への安全な変換
            raw_dataframe_from_sheet['date'] = pd.to_datetime(raw_dataframe_from_sheet['date'], errors='coerce')
            
        if 'result_pos' in raw_dataframe_from_sheet.columns:
            # 着順を数値型へ変換
            raw_dataframe_from_sheet['result_pos'] = pd.to_numeric(raw_dataframe_from_sheet['result_pos'], errors='coerce')
            # NaNを0で埋める安全策
            raw_dataframe_from_sheet['result_pos'] = raw_dataframe_from_sheet['result_pos'].fillna(0)
        
        # 🌟 最重要：三段階詳細ソートロジック
        raw_dataframe_from_sheet = raw_dataframe_from_sheet.sort_values(
            by=["date", "last_race", "result_pos"], 
            ascending=[False, True, True]
        )
        
        # 各種数値カラムのパースとNaN補完（一切の簡略化を禁止、個別に明示的に実行）
        if 'result_pop' in raw_dataframe_from_sheet.columns:
            raw_dataframe_from_sheet['result_pop'] = pd.to_numeric(raw_dataframe_from_sheet['result_pop'], errors='coerce')
            raw_dataframe_from_sheet['result_pop'] = raw_dataframe_from_sheet['result_pop'].fillna(0)
            
        if 'f3f' in raw_dataframe_from_sheet.columns:
            raw_dataframe_from_sheet['f3f'] = pd.to_numeric(raw_dataframe_from_sheet['f3f'], errors='coerce')
            raw_dataframe_from_sheet['f3f'] = raw_dataframe_from_sheet['f3f'].fillna(0.0)
            
        if 'l3f' in raw_dataframe_from_sheet.columns:
            raw_dataframe_from_sheet['l3f'] = pd.to_numeric(raw_dataframe_from_sheet['l3f'], errors='coerce')
            raw_dataframe_from_sheet['l3f'] = raw_dataframe_from_sheet['l3f'].fillna(0.0)
            
        if 'race_l3f' in raw_dataframe_from_sheet.columns:
            raw_dataframe_from_sheet['race_l3f'] = pd.to_numeric(raw_dataframe_from_sheet['race_l3f'], errors='coerce')
            raw_dataframe_from_sheet['race_l3f'] = raw_dataframe_from_sheet['race_l3f'].fillna(0.0)
            
        if 'load' in raw_dataframe_from_sheet.columns:
            raw_dataframe_from_sheet['load'] = pd.to_numeric(raw_dataframe_from_sheet['load'], errors='coerce')
            raw_dataframe_from_sheet['load'] = raw_dataframe_from_sheet['load'].fillna(0.0)
            
        if 'base_rtc' in raw_dataframe_from_sheet.columns:
            raw_dataframe_from_sheet['base_rtc'] = pd.to_numeric(raw_dataframe_from_sheet['base_rtc'], errors='coerce')
            raw_dataframe_from_sheet['base_rtc'] = raw_dataframe_from_sheet['base_rtc'].fillna(0.0)
            
        if 'cushion' in raw_dataframe_from_sheet.columns:
            raw_dataframe_from_sheet['cushion'] = pd.to_numeric(raw_dataframe_from_sheet['cushion'], errors='coerce')
            raw_dataframe_from_sheet['cushion'] = raw_dataframe_from_sheet['cushion'].fillna(9.5)
            
        if 'water' in raw_dataframe_from_sheet.columns:
            raw_dataframe_from_sheet['water'] = pd.to_numeric(raw_dataframe_from_sheet['water'], errors='coerce')
            raw_dataframe_from_sheet['water'] = raw_dataframe_from_sheet['water'].fillna(10.0)
            
        if 'track_week' in raw_dataframe_from_sheet.columns:
            raw_dataframe_from_sheet['track_week'] = pd.to_numeric(raw_dataframe_from_sheet['track_week'], errors='coerce')
            raw_dataframe_from_sheet['track_week'] = raw_dataframe_from_sheet['track_week'].fillna(1.0)

        if 'raw_time' in raw_dataframe_from_sheet.columns:
            raw_dataframe_from_sheet['raw_time'] = pd.to_numeric(raw_dataframe_from_sheet['raw_time'], errors='coerce').fillna(0.0)

        if 'track_idx' in raw_dataframe_from_sheet.columns:
            raw_dataframe_from_sheet['track_idx'] = pd.to_numeric(raw_dataframe_from_sheet['track_idx'], errors='coerce').fillna(0.0)

        if 'bias_slider' in raw_dataframe_from_sheet.columns:
            raw_dataframe_from_sheet['bias_slider'] = pd.to_numeric(raw_dataframe_from_sheet['bias_slider'], errors='coerce').fillna(0.0)
            
        # 全てのカラムが空である不正な行を物理的にクリーニング
        raw_dataframe_from_sheet = raw_dataframe_from_sheet.dropna(how='all')
        
        return raw_dataframe_from_sheet
        
    except Exception as e_database_loading:
        st.error(f"【重大な警告】スプレッドシートの物理的な読み込み中に回復不能なエラーが発生しました。詳細を確認してください: {e_database_loading}")
        return pd.DataFrame(columns=ABSOLUTE_COLUMN_STRUCTURE_DEFINITION_GLOBAL)

def get_db_data():
    """データベース取得用のエントリポイント。キャッシュ管理された関数を詳細に呼び出します。"""
    return get_db_data_cached()

# ==============================================================================
# 3. データベース更新詳細ロジック (同期性能を極大化した物理書き込み)
# ==============================================================================

def safe_update(df_sync_target):
    """
    スプレッドシートへ全データを書き戻すための最重要関数です。
    リトライ機能、ソート、インデックスリセット、キャッシュ強制クリアを完全に含みます。
    """
    if 'date' in df_sync_target.columns:
        if 'last_race' in df_sync_target.columns:
            if 'result_pos' in df_sync_target.columns:
                # 日付と数値を再適用し、不整合を排除
                df_sync_target['date'] = pd.to_datetime(df_sync_target['date'], errors='coerce')
                df_sync_target['result_pos'] = pd.to_numeric(df_sync_target['result_pos'], errors='coerce')
                # 最終的なソート順の強制。これがUIの並びを決定します。
                df_sync_target = df_sync_target.sort_values(
                    by=["date", "last_race", "result_pos"], 
                    ascending=[False, True, True]
                )
                # 🌟 Google Sheets側で日付が空欄になるバグを物理的に阻止。
                df_sync_target['date'] = df_sync_target['date'].dt.strftime('%Y-%m-%d')
                df_sync_target['date'] = df_sync_target['date'].fillna("")
    
    # 🌟 Google Sheets側の物理行との乖離を防ぐため、インデックスを再生成します。
    df_sync_target = df_sync_target.reset_index(drop=True)
    
    # 書き込みリトライループの定義（ネットワークやAPIリミットへの耐性を最大化）
    physical_max_attempts = 3
    for i_attempt_counter in range(physical_max_attempts):
        try:
            conn.update(data=df_sync_target)
            st.cache_data.clear()
            return True
        except Exception as e_sheet_save_critical:
            failure_wait_duration = 5
            if i_attempt_counter < physical_max_attempts - 1:
                st.warning(f"Google Sheetsとの同期に失敗しました(リトライ {i_attempt_counter+1}/3)... {failure_wait_duration}秒後に再実行します。")
                time.sleep(failure_wait_duration)
                continue
            else:
                st.error(f"スプレッドシートの物理的な更新が不可能な状態です。API接続制限またはネットワークの不具合を確認してください。: {e_sheet_save_critical}")
                return False

# ==============================================================================
# 4. 補助関数セクション (冗長かつ詳細な記述を貫徹)
# ==============================================================================

def format_time_to_hmsf_string(val_seconds_raw):
    """
    秒数を mm:ss.f 形式の文字列に詳細変換します。
    """
    if val_seconds_raw is None: return ""
    if val_seconds_raw <= 0: return ""
    if pd.isna(val_seconds_raw): return ""
    if isinstance(val_seconds_raw, str): return val_seconds_raw
        
    val_minutes_component = int(val_seconds_raw // 60)
    val_seconds_component = val_seconds_raw % 60
    return f"{val_minutes_component}:{val_seconds_component:04.1f}"

def parse_time_string_to_seconds(str_time_input):
    """
    mm:ss.f 形式の文字列を秒数(float)にパースして戻します。
    """
    if str_time_input is None: return 0.0
    try:
        cleaned_time_string_val = str(str_time_input).strip()
        if ":" in cleaned_time_string_val:
            list_of_time_parts = cleaned_time_string_val.split(':')
            val_extracted_minutes = float(list_of_time_parts[0])
            val_extracted_seconds = float(list_of_time_parts[1])
            return val_extracted_minutes * 60 + val_extracted_seconds
        return float(cleaned_time_string_val)
    except:
        return 0.0

# ==============================================================================
# 5. 係数マスタ詳細定義 (初期設計を1ミリも削らず、100%物理復元)
# ==============================================================================

MASTER_CONFIG_V65_TURF_LOAD_COEFFS = {
    "東京": 0.10, "中山": 0.25, "京都": 0.15, "阪神": 0.18, "中京": 0.20,
    "新潟": 0.05, "小倉": 0.30, "福島": 0.28, "札幌": 0.22, "函館": 0.25
}

MASTER_CONFIG_V65_DIRT_LOAD_COEFFS = {
    "東京": 0.40, "中山": 0.55, "京都": 0.45, "阪神": 0.48, "中京": 0.50,
    "新潟": 0.42, "小倉": 0.58, "福島": 0.60, "札幌": 0.62, "函館": 0.65
}

MASTER_CONFIG_V65_GRADIENT_FACTORS = {
    "中山": 0.005, "中京": 0.004, "京都": 0.002, "阪神": 0.004, "東京": 0.003,
    "新潟": 0.001, "小倉": 0.002, "福島": 0.003, "札幌": 0.001, "函館": 0.002
}

# ==============================================================================
# 6. メインUI構成 - タブインターフェースの絶対的物理宣言
# ==============================================================================

tab_main_analysis, tab_horse_history, tab_race_history, tab_simulator, tab_trends, tab_backtest, tab_management = st.tabs([
    "📝 解析・保存", 
    "🐎 馬別履歴", 
    "🏁 レース別履歴", 
    "🎯 シミュレーター", 
    "📈 馬場トレンド", 
    "📊 バックテスト",
    "🗑 データ管理"
])

# ==============================================================================
# 7. Tab 1: 解析・保存セクション (不具合完全排除・全ロジック非省略)
# ==============================================================================

with tab_main_analysis:
    df_pickup_tab1_raw = get_db_data()
    if not df_pickup_tab1_raw.empty:
        st.subheader("🎯 次走注目馬（逆行評価ピックアップ）")
        list_pickup_entries_final = []
        for idx_pickup_item, row_pickup_item in df_pickup_tab1_raw.iterrows():
            str_memo_val_item = str(row_pickup_item['memo'])
            flag_bias_exists_pk = "💎" in str_memo_val_item
            flag_pace_exists_pk = "🔥" in str_memo_val_item
            
            if flag_bias_exists_pk or flag_pace_exists_pk:
                label_reverse_type_final = ""
                if flag_bias_exists_pk and flag_pace_exists_pk:
                    label_reverse_type_final = "【💥両方逆行】"
                elif flag_bias_exists_pk:
                    label_reverse_type_final = "【💎バイアス逆行】"
                elif flag_pace_exists_pk:
                    label_reverse_type_final = "【🔥ペース逆行】"
                
                val_date_pk = row_pickup_item['date']
                if not pd.isna(val_date_pk):
                    if isinstance(val_date_pk, str): str_date_pk = val_date_pk
                    else: str_date_pk = val_date_pk.strftime('%Y-%m-%d')
                else: str_date_pk = ""

                # 🔼 RTC推移トレンド判定（直近3走の正規化RTCが単調改善かチェック）
                str_trend_pk = ""
                df_pk_horse_trend = df_pickup_tab1_raw[df_pickup_tab1_raw['name'] == row_pickup_item['name']].sort_values("date")
                pk_recent_valid = df_pk_horse_trend[(df_pk_horse_trend['base_rtc'] > 0) & (df_pk_horse_trend['base_rtc'] < 999)].tail(3)
                if len(pk_recent_valid) >= 3:
                    pk_norm_vals = []
                    for _, pk_r in pk_recent_valid.iterrows():
                        if pk_r['dist'] > 0:
                            pk_norm_vals.append(pk_r['base_rtc'] / pk_r['dist'] * 1600)
                    if len(pk_norm_vals) >= 3:
                        if pk_norm_vals[0] > pk_norm_vals[1] > pk_norm_vals[2]:
                            str_trend_pk = "🔼上昇中"
                        elif pk_norm_vals[0] < pk_norm_vals[1] < pk_norm_vals[2]:
                            str_trend_pk = "🔽下降中"

                list_pickup_entries_final.append({
                    "馬名": row_pickup_item['name'], 
                    "逆行タイプ": label_reverse_type_final,
                    "トレンド": str_trend_pk,
                    "前走": row_pickup_item['last_race'],
                    "日付": str_date_pk, 
                    "解析メモ": str_memo_val_item
                })
        
        if list_pickup_entries_final:
            df_pickup_display_final = pd.DataFrame(list_pickup_entries_final)
            st.dataframe(
                df_pickup_display_final.sort_values("日付", ascending=False), 
                use_container_width=True, 
                hide_index=True
            )
            
    st.divider()

    st.header("🚀 レース解析 & 自動保存システム")
    
    with st.sidebar:
        st.title("解析条件設定")
        str_in_race_name_actual_f = st.text_input("解析対象レース名称")
        val_in_race_date_actual_f = st.date_input("レース実施日を物理指定", datetime.now())
        sel_in_course_name_actual_f = st.selectbox("開催競馬場を指定", list(MASTER_CONFIG_V65_TURF_LOAD_COEFFS.keys()))
        opt_in_track_kind_actual_f = st.radio("トラック物理種別", ["芝", "ダート"], horizontal=True)
        list_dist_range_opts_actual_f = list(range(1000, 3700, 100))
        val_in_dist_actual_actual_f = st.selectbox("物理レース距離(m)", list_dist_range_opts_actual_f, index=list_dist_range_opts_actual_f.index(1600) if 1600 in list_dist_range_opts_actual_f else 6)
        st.divider()
        st.write("💧 馬場物理詳細パラメータ入力")
        val_in_cushion_agg = st.number_input("物理クッション値", 7.0, 12.0, 9.5, step=0.1) if opt_in_track_kind_actual_f == "芝" else 9.5
        val_in_water4c_agg = st.number_input("物理含水率：4角(%)", 0.0, 50.0, 10.0, step=0.1)
        val_in_watergoal_agg = st.number_input("物理含水率：ゴール(%)", 0.0, 50.0, 10.0, step=0.1)
        val_in_trackidx_agg = st.number_input("独自馬場補正指数", -50, 50, 0, step=1)
        val_in_bias_slider_agg = st.slider("物理バイアス強度指定", -1.0, 1.0, 0.0, step=0.1)
        val_in_week_num_agg = st.number_input("当該物理開催週 (1〜12週)", 1, 12, 1)

    col_analysis_left_box, col_analysis_right_box = st.columns(2)
    
    with col_analysis_left_box: 
        st.markdown("##### 🏁 レースラップ詳細入力")
        str_input_raw_lap_text_f = st.text_area("JRAレースラップを貼り付け", height=150)
        
        var_f3f_calc_res_f = 0.0
        var_l3f_calc_res_f = 0.0
        var_pace_label_res_f = "ミドルペース"
        var_pace_gap_res_f = 0.0
        str_race_type_eval_f = "不明"
        var_mid_laps_avg_f = 0.0
        
        if str_input_raw_lap_text_f:
            list_found_laps_f = re.findall(r'\d+\.\d', str_input_raw_lap_text_f)
            list_converted_laps_f = [float(x) for x in list_found_laps_f]
                
            if len(list_converted_laps_f) >= 3:
                var_f3f_calc_res_f = list_converted_laps_f[0] + list_converted_laps_f[1] + list_converted_laps_f[2]
                var_l3f_calc_res_f = list_converted_laps_f[-3] + list_converted_laps_f[-2] + list_converted_laps_f[-1]
                var_pace_gap_res_f = var_f3f_calc_res_f - var_l3f_calc_res_f
                
                val_dynamic_threshold_f = 1.0 * (val_in_dist_actual_actual_f / 1600.0)
                
                if var_pace_gap_res_f < -val_dynamic_threshold_f:
                    var_pace_label_res_f = "ハイペース"
                elif var_pace_gap_res_f > val_dynamic_threshold_f:
                    var_pace_label_res_f = "スローペース"
                else:
                    var_pace_label_res_f = "ミドルペース"
                
                var_total_laps_count_f = len(list_converted_laps_f)
                if var_total_laps_count_f > 6:
                    list_mid_laps_f = list_converted_laps_f[3:-3]
                    var_mid_laps_sum_f = sum(list_mid_laps_f)
                    var_mid_laps_avg_f = var_mid_laps_sum_f / len(list_mid_laps_f)
                    if var_mid_laps_avg_f >= 11.9: str_race_type_eval_f = "瞬発力戦"
                    else: str_race_type_eval_f = "持続力戦"
                else:
                    str_race_type_eval_f = "持続力戦"
                    
                st.success(f"ラップ解析成功: 前3F {var_f3f_calc_res_f:.1f} / 後3F {var_l3f_calc_res_f:.1f} ({var_pace_label_res_f}) / 展開: {str_race_type_eval_f}")
        
        val_in_manual_l3f_v6_agg_actual_final = st.number_input("確定レース上がり3F数値", 0.0, 60.0, var_l3f_calc_res_f, step=0.1)

    with col_analysis_right_box: 
        st.markdown("##### 🐎 成績表詳細貼り付け")
        str_input_raw_jra_results_f = text_area_val = st.text_area("JRA公式サイトの成績表をそのまま貼り付けてください", height=250)

    if 'state_tab1_preview_is_active_f' not in st.session_state:
        st.session_state.state_tab1_preview_is_active_f = False

    st.write("---")
    if st.button("🔍 解析プレビューを生成"):
        if not str_input_raw_jra_results_f:
            st.error("成績表の内容がありません。")
        elif var_f3f_calc_res_f <= 0:
            st.error("有効なレースラップを入力し、解析を行ってください。")
        else:
            st.session_state.state_tab1_preview_is_active_f = True

    if st.session_state.state_tab1_preview_is_active_f == True:
        st.markdown("##### ⚖️ 解析プレビュー（物理抽出結果の確認・修正）")
        
        list_validated_lines_preview = []
        for l in str_input_raw_jra_results_f.strip().split('\n'):
            line_str = l.strip()
            if len(line_str) <= 5: continue
            if "騎手" in line_str and "着差" in line_str: continue
            if "タイム" in line_str and "コーナー" in line_str: continue
            if "着順" in line_str and "馬名" in line_str: continue
            list_validated_lines_preview.append(line_str)
        
        list_preview_table_buffer_f = []
        for line_p_item_f in list_validated_lines_preview:
            found_horse_names_p_f = re.findall(r'([ァ-ヶー]{2,})', line_p_item_f)
            if not found_horse_names_p_f: continue
            match_weight_p_f = re.search(r'([4-6]\d\.\d)', line_p_item_f)
            val_weight_extracted_now_f = float(match_weight_p_f.group(1)) if match_weight_p_f else 56.0
            list_preview_table_buffer_f.append({
                "馬名": found_horse_names_p_f[0], 
                "斤量": val_weight_extracted_now_f, 
                "raw_line": line_p_item_f
            })
        
        df_analysis_preview_actual_f = st.data_editor(
            pd.DataFrame(list_preview_table_buffer_f), 
            use_container_width=True, 
            hide_index=True
        )

        if st.button("🚀 この内容で物理確定しスプレッドシートへ強制同期"):
            v65_final_race_name = str_in_race_name_actual_f
            v65_final_race_date = val_in_race_date_actual_f
            v65_final_course_name = sel_in_course_name_actual_f
            v65_final_dist_m = val_in_dist_actual_actual_f
            v65_final_manual_l3f = val_in_manual_l3f_v6_agg_actual_final
            v75_final_race_type = str_race_type_eval_f
            v80_final_track_kind = opt_in_track_kind_actual_f
            
            if not v65_final_race_name:
                st.error("レース名称が未入力です。詳細物理入力を完了してください。")
            else:
                list_final_parsed_results_acc_v6_agg_actual_f = []
                for idx_row_v65_agg_f, row_item_v65_agg_f in df_analysis_preview_actual_f.iterrows():
                    str_line_v65_agg_f_raw = row_item_v65_agg_f["raw_line"]
                    
                    match_rank_f_v65_agg_final_step_f = re.match(r'(?:^|\s)(\d{1,2})(?:\s|着)', str_line_v65_agg_f_raw)
                    val_rank_pos_num_v6_agg_final_actual_f = int(match_rank_f_v65_agg_final_step_f.group(1)) if match_rank_f_v65_agg_final_step_f else 99
                    
                    match_time_v65_agg_final_step_f = re.search(r'(\d{1,2})[:：](\d{2}\.\d)', str_line_v65_agg_f_raw)
                    str_suffix_v65_agg_final_f_f = str_line_v65_agg_f_raw
                    if match_time_v65_agg_final_step_f:
                        str_suffix_v65_agg_final_f_f = str_line_v65_agg_f_raw[match_time_v65_agg_final_step_f.end():]
                        
                    list_pos_vals_found_v65_agg_final_f_f = re.findall(r'\b([1-2]?\d)\b', str_suffix_v65_agg_final_f_f)
                    val_final_4c_pos_v6_res_agg_final_actual_f = 7.0 
                    
                    if list_pos_vals_found_v65_agg_final_f_f:
                        list_valid_pos_buf_v6_agg_f_f_f = []
                        for p_str_v65_agg_f_f_f in list_pos_vals_found_v65_agg_final_f_f:
                            p_int_v65_agg_f_f_f = int(p_str_v65_agg_f_f_f)
                            if p_int_v65_agg_f_f_f > 30: break
                            list_valid_pos_buf_v6_agg_f_f_f.append(float(p_int_v65_agg_f_f_f))
                        if list_valid_pos_buf_v6_agg_f_f_f:
                            val_final_4c_pos_v6_res_agg_final_actual_f = list_valid_pos_buf_v6_agg_f_f_f[-1]
                    
                    list_final_parsed_results_acc_v6_agg_actual_f.append({
                        "line": str_line_v65_agg_f_raw, "res_pos": val_rank_pos_num_v6_agg_final_actual_f, 
                        "four_c_pos": val_final_4c_pos_v6_res_agg_final_actual_f, "name": row_item_v65_agg_f["馬名"], 
                        "weight": row_item_v65_agg_f["斤量"]
                    })
                
                list_top3_bias_pool_f = sorted([d for d in list_final_parsed_results_acc_v6_agg_actual_f if d["res_pos"] <= 3], key=lambda x: x["res_pos"])
                list_bias_outliers_acc_f = [d for d in list_top3_bias_pool_f if d["four_c_pos"] >= 10.0 or d["four_c_pos"] <= 3.0]
                
                if len(list_bias_outliers_acc_f) == 1:
                    list_bias_core_agg_f = [d for d in list_top3_bias_pool_f if d != list_bias_outliers_acc_f[0]]
                    list_supp_4th_agg_f = [d for d in list_final_parsed_results_acc_v6_agg_actual_f if d["res_pos"] == 4]
                    list_final_bias_set_f_f = list_bias_core_agg_f + list_supp_4th_agg_f
                else:
                    list_final_bias_set_f_f = list_top3_bias_pool_f
                
                val_avg_c4_pos_f = sum(d["four_c_pos"] for d in list_final_bias_set_f_f) / len(list_final_bias_set_f_f) if list_final_bias_set_f_f else 7.0
                str_determined_bias_label_f = "前有利" if val_avg_c4_pos_f <= 4.0 else "後有利" if val_avg_c4_pos_f >= 10.0 else "フラット"
                val_field_size_f_f = max([d["res_pos"] for d in list_final_parsed_results_acc_v6_agg_actual_f]) if list_final_parsed_results_acc_v6_agg_actual_f else 16

                list_new_sync_rows_tab1_v6_final = []
                for entry_save_m_f in list_final_parsed_results_acc_v6_agg_actual_f:
                    str_line_v_step_f = entry_save_m_f["line"]
                    val_l_pos_v_step_f = entry_save_m_f["four_c_pos"]
                    val_r_rank_v_step_f = entry_save_m_f["res_pos"]
                    val_w_val_v_step_f = entry_save_m_f["weight"] 
                    str_horse_body_weight_f_def_f = "" 
                    
                    m_time_obj_v_step_f = re.search(r'(\d{1,2})[:：](\d{2}\.\d)', str_line_v_step_f)
                    val_total_seconds_raw_v_f = 0.0
                    
                    if m_time_obj_v_step_f:
                        val_m_comp_v_f = float(m_time_obj_v_step_f.group(1))
                        val_s_comp_v_f = float(m_time_obj_v_step_f.group(2))
                        val_total_seconds_raw_v_f = val_m_comp_v_f * 60 + val_s_comp_v_f
                    else:
                        list_all_decimals_time_f = re.findall(r'(\d{2}\.\d)', str_line_v_step_f)
                        flag_weight_skipped = False
                        for str_dec_f in list_all_decimals_time_f:
                            float_dec_f = float(str_dec_f)
                            if not flag_weight_skipped and abs(float_dec_f - val_w_val_v_step_f) < 0.01:
                                flag_weight_skipped = True
                                continue
                            if 50.0 <= float_dec_f <= 75.0:
                                val_total_seconds_raw_v_f = float_dec_f
                                break
                    
                    if val_total_seconds_raw_v_f <= 0.0:
                        val_total_seconds_raw_v_f = 999.0
                    
                    match_bw_raw_v_f = re.search(r'(\d{3})kg', str_line_v_step_f)
                    if match_bw_raw_v_f:
                        str_horse_body_weight_f_def_f = f"({match_bw_raw_v_f.group(1)}kg)"

                    val_l3f_indiv_v_f = 0.0
                    m_l3f_p_v_f = re.search(r'(\d{2}\.\d)\s*\d{3}\(', str_line_v_step_f)
                    if m_l3f_p_v_f:
                        val_l3f_indiv_v_f = float(m_l3f_p_v_f.group(1))
                    else:
                        list_decimals_v_f = re.findall(r'(\d{2}\.\d)', str_line_v_step_f)
                        for dv_v_f in list_decimals_v_f:
                            dv_float_v_f = float(dv_v_f)
                            if 30.0 <= dv_float_v_f <= 46.0 and abs(dv_float_v_f - val_w_val_v_step_f) > 0.5:
                                val_l3f_indiv_v_f = dv_float_v_f; break
                    
                    if val_l3f_indiv_v_f == 0.0:
                        val_l3f_indiv_v_f = v65_final_manual_l3f

                    val_rel_ratio_f = val_l_pos_v_step_f / val_field_size_f_f
                    val_scale_f = val_field_size_f_f / 16.0
                    val_computed_load_score_f = 0.0
                    if var_pace_label_res_f == "ハイペース" and str_determined_bias_label_f != "前有利":
                        v_raw_load_calc = (0.6 - val_rel_ratio_f) * abs(var_pace_gap_res_f) * 3.0
                        val_computed_load_score_f = max(0.0, v_raw_load_calc) * val_scale_f
                    elif var_pace_label_res_f == "スローペース" and str_determined_bias_label_f != "後有利":
                        v_raw_load_calc = (val_rel_ratio_f - 0.4) * abs(var_pace_gap_res_f) * 2.0
                        val_computed_load_score_f = max(0.0, v_raw_load_calc) * val_scale_f
                    
                    list_tags_f = []
                    flag_is_counter_f = False
                    
                    if val_r_rank_v_step_f <= 5:
                        if (str_determined_bias_label_f == "前有利" and val_l_pos_v_step_f >= 10.0) or (str_determined_bias_label_f == "後有利" and val_l_pos_v_step_f <= 3.0):
                            list_tags_f.append("💎💎 ﾊﾞｲｱｽ極限逆行" if val_field_size_f_f >= 16 else "💎 ﾊﾞｲｱｽ逆行"); flag_is_counter_f = True
                    
                    if not ((var_pace_label_res_f == "ハイペース" and str_determined_bias_label_f == "前有利") or (var_pace_label_res_f == "スローペース" and str_determined_bias_label_f == "後有利")):
                        if var_pace_label_res_f == "ハイペース" and val_l_pos_v_step_f <= 3.0 and val_r_rank_v_step_f <= 5: 
                            list_tags_f.append("📉 激流被害" if val_field_size_f_f >= 14 else "🔥 展開逆行"); flag_is_counter_f = True
                        elif var_pace_label_res_f == "スローペース" and val_l_pos_v_step_f >= 10.0 and (var_f3f_calc_res_f - val_l3f_indiv_v_f) > 1.5 and val_r_rank_v_step_f <= 5: 
                            list_tags_f.append("🔥 展開逆行"); flag_is_counter_f = True

                    val_l3f_gap_f = v65_final_manual_l3f - val_l3f_indiv_v_f
                    if val_l3f_gap_f >= 0.5: list_tags_f.append("🚀 アガリ優秀")
                    elif val_l3f_gap_f <= -1.0: list_tags_f.append("📉 失速大")
                    
                    r_p1 = val_total_seconds_raw_v_f
                    r_p2 = (val_w_val_v_step_f - 56.0) * 0.1
                    r_p3 = val_in_trackidx_agg / 10.0
                    r_p4 = val_computed_load_score_f / 10.0
                    r_p5 = (val_in_week_num_agg - 1) * 0.05
                    r_p7 = (val_in_water4c_agg + val_in_watergoal_agg) / 2.0
                    r_p8 = (r_p7 - 10.0) * 0.05
                    r_p9 = (9.5 - val_in_cushion_agg) * 0.1
                    r_p10 = (v65_final_dist_m - 1600) * 0.0005
                    
                    val_final_rtc_v = r_p1 - r_p2 - r_p3 - r_p4 - r_p5 + val_in_bias_slider_agg - r_p8 - r_p9 + r_p10

                    str_field_tag_f = "多" if val_field_size_f_f >= 16 else "少" if val_field_size_f_f <= 10 else "中"
                    str_final_memo_f = f"【{var_pace_label_res_f}({v75_final_race_type})/{str_determined_bias_label_f}/負荷:{val_computed_load_score_f:.1f}({str_field_tag_f})/平】{'/'.join(list_tags_f) if list_tags_f else '順境'}"

                    list_new_sync_rows_tab1_v6_final.append({
                        "name": entry_save_m_f["name"], "base_rtc": val_final_rtc_v, 
                        "last_race": v65_final_race_name, "course": v65_final_course_name, "dist": v65_final_dist_m, 
                        "notes": f"{val_w_val_v_step_f}kg{str_horse_body_weight_f_def_f}", 
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"), "f3f": var_f3f_calc_res_f, 
                        "l3f": val_l3f_indiv_v_f, "race_l3f": v65_final_manual_l3f, 
                        "load": val_l_pos_v_step_f, "memo": str_final_memo_f,
                        "date": v65_final_race_date.strftime("%Y-%m-%d"), "cushion": val_in_cushion_agg, 
                        "water": r_p7, "next_buy_flag": "★逆行狙い" if flag_is_counter_f else "", 
                        "result_pos": val_r_rank_v_step_f, "track_week": val_in_week_num_agg,
                        "race_type": v75_final_race_type,
                        "track_kind": v80_final_track_kind,
                        "raw_time": val_total_seconds_raw_v_f,
                        "track_idx": val_in_trackidx_agg,
                        "bias_slider": val_in_bias_slider_agg
                    })
                
                if list_new_sync_rows_tab1_v6_final:
                    st.cache_data.clear()
                    df_sheet_latest_v = conn.read(ttl=0)
                    for col_norm_f in ABSOLUTE_COLUMN_STRUCTURE_DEFINITION_GLOBAL:
                        if col_norm_f not in df_sheet_latest_v.columns: df_sheet_latest_v[col_norm_f] = None
                    df_final_sync_v = pd.concat([df_sheet_latest_v, pd.DataFrame(list_new_sync_rows_tab1_v6_final)], ignore_index=True)
                    if safe_update(df_final_sync_v):
                        st.session_state.state_tab1_preview_is_active_f = False
                        st.success(f"✅ 解析・同期保存が物理的に完了しました。"); st.rerun()

# ==============================================================================
# 8. Tab 2: 馬別履歴詳細 & 個別メンテナンス
# ==============================================================================

with tab_horse_history:
    st.header("📊 馬別履歴 & 買い条件詳細物理管理エンジン")
    df_t2_source_v6 = get_db_data()
    if not df_t2_source_v6.empty:
        col_t2_f1, col_t2_f2 = st.columns([1, 1])
        with col_t2_f1:
            input_horse_search_q_v6 = st.text_input("馬名物理絞り込み検索", key="q_h_t2_v6")
        
        list_h_names_t2_pool = sorted([str(xn) for xn in df_t2_source_v6['name'].dropna().unique()])
        with col_t2_f2:
            val_sel_target_h_t2_v6 = st.selectbox("個別馬実績の物理修正対象馬を選択", ["未選択"] + list_h_names_t2_pool)
        
        if val_sel_target_h_t2_v6 != "未選択":
            idx_list_t2_found = df_t2_source_v6[df_t2_source_v6['name'] == val_sel_target_h_t2_v6].index
            target_idx_t2_f_actual = idx_list_t2_found[-1]
            
            with st.form("form_edit_h_t2_v6_agg"):
                val_memo_t2_v6_cur = df_t2_source_v6.at[target_idx_t2_f_actual, 'memo'] if not pd.isna(df_t2_source_v6.at[target_idx_t2_f_actual, 'memo']) else ""
                new_memo_t2_v6_val = st.text_area("解析評価メモの詳細物理修正", value=val_memo_t2_v6_cur)
                val_flag_t2_v6_cur = df_t2_source_v6.at[target_idx_t2_f_actual, 'next_buy_flag'] if not pd.isna(df_t2_source_v6.at[target_idx_t2_f_actual, 'next_buy_flag']) else ""
                new_flag_t2_v6_val = st.text_input("次走個別買いフラグ物理設定", value=val_flag_t2_v6_cur)
                
                val_kind_t2_v6_cur = str(df_t2_source_v6.at[target_idx_t2_f_actual, 'track_kind']) if not pd.isna(df_t2_source_v6.at[target_idx_t2_f_actual, 'track_kind']) else "芝"
                if val_kind_t2_v6_cur not in ["芝", "ダート"]: val_kind_t2_v6_cur = "芝"
                new_kind_t2_v6_val = st.selectbox("トラック種別物理設定 (芝/ダート)", ["芝", "ダート"], index=0 if val_kind_t2_v6_cur == "芝" else 1)
                
                if st.form_submit_button("同期保存実行"):
                    df_t2_source_v6.at[target_idx_t2_f_actual, 'memo'] = new_memo_t2_v6_val
                    df_t2_source_v6.at[target_idx_t2_f_actual, 'next_buy_flag'] = new_flag_t2_v6_val
                    df_t2_source_v6.at[target_idx_t2_f_actual, 'track_kind'] = new_kind_t2_v6_val
                    if safe_update(df_t2_source_v6):
                        st.success(f"【{val_sel_target_h_t2_v6}】同期成功"); st.rerun()

        # ==============================================================================
        # 【機能3】RTC推移分析 / ピーク時期予測 / 距離適性テーブル
        # ==============================================================================
        st.divider()
        st.subheader(f"📈 {val_sel_target_h_t2_v6} 能力推移詳細分析")

        df_trend_target = df_t2_source_v6[df_t2_source_v6['name'] == val_sel_target_h_t2_v6].sort_values("date")
        df_trend_valid = df_trend_target[(df_trend_target['base_rtc'] > 0) & (df_trend_target['base_rtc'] < 999)].copy()

        if not df_trend_valid.empty:
            # 距離正規化RTC（異なる距離のレースを1600m基準で比較可能にする）
            df_trend_valid['norm_rtc'] = df_trend_valid.apply(
                lambda r: r['base_rtc'] / r['dist'] * 1600 if r['dist'] > 0 else r['base_rtc'], axis=1
            )
            df_trend_valid['date_str'] = df_trend_valid['date'].apply(
                lambda x: x.strftime('%Y-%m-%d') if not pd.isna(x) else ""
            )
            chart_df_trend = df_trend_valid[df_trend_valid['date_str'] != ""][['date_str', 'norm_rtc']].set_index('date_str')
            st.caption("正規化RTC推移（1600m換算・低いほど高パフォーマンス）")
            st.line_chart(chart_df_trend, use_container_width=True)

            # トレンドラベル判定（直近3走）
            recent3_norm = df_trend_valid['norm_rtc'].tail(3).tolist()
            if len(recent3_norm) >= 3:
                if recent3_norm[0] > recent3_norm[1] > recent3_norm[2]:
                    trend_result_label = "🔼上昇中（直近3走で継続的にタイム短縮）"
                elif recent3_norm[0] < recent3_norm[1] < recent3_norm[2]:
                    trend_result_label = "🔽下降中（直近3走で継続的にタイム悪化）"
                else:
                    trend_result_label = "➡️横ばい（直近3走で上下動あり）"
            elif len(recent3_norm) == 2:
                if recent3_norm[0] > recent3_norm[1]:
                    trend_result_label = "🔼上昇（2走で比較）"
                elif recent3_norm[0] < recent3_norm[1]:
                    trend_result_label = "🔽下降（2走で比較）"
                else:
                    trend_result_label = "➡️変化なし"
            else:
                trend_result_label = "データ不足"

            st.metric("📊 RTCトレンド", trend_result_label)

            # 距離別適性テーブル
            st.markdown("##### 🏇 距離帯別適性")
            dist_range_defs = [
                ("短距離 (~1400m)", 0, 1400),
                ("マイル (1401~1800m)", 1401, 1800),
                ("中距離 (1801~2200m)", 1801, 2200),
                ("長距離 (2201m~)", 2201, 99999),
            ]
            dist_apt_rows = []
            for d_label, d_min, d_max in dist_range_defs:
                df_d_sub = df_trend_target[(df_trend_target['dist'] >= d_min) & (df_trend_target['dist'] <= d_max)].copy()
                if df_d_sub.empty:
                    continue
                n_d = len(df_d_sub)
                n_top3_d = len(df_d_sub[df_d_sub['result_pos'] <= 3]) if df_d_sub['result_pos'].sum() > 0 else 0
                valid_rtc_d = df_d_sub[(df_d_sub['base_rtc'] > 0) & (df_d_sub['base_rtc'] < 999)]
                if valid_rtc_d.empty:
                    continue
                avg_norm_d = (valid_rtc_d['base_rtc'] / valid_rtc_d['dist'] * 1600).mean()
                dist_apt_rows.append({
                    "距離帯": d_label,
                    "走数": n_d,
                    "複勝率": f"{n_top3_d / n_d * 100:.0f}%" if n_d > 0 else "-",
                    "平均正規化RTC": f"{avg_norm_d:.2f}秒",
                    "判定": "🔥得意" if (n_top3_d / n_d >= 0.4 and n_d >= 2) else "❌苦手" if (n_top3_d / n_d < 0.2 and n_d >= 3) else "普通",
                })
            if dist_apt_rows:
                st.dataframe(pd.DataFrame(dist_apt_rows), use_container_width=True, hide_index=True)
            else:
                st.caption("距離別データなし（データが少ない場合があります）")
        else:
            st.info("有効なRTCデータがないため推移分析を表示できません。")

        df_t2_filtered_v6 = df_t2_source_v6[df_t2_source_v6['name'].str.contains(input_horse_search_q_v6, na=False)] if input_horse_search_q_v6 else df_t2_source_v6
        df_t2_final_view_f_v6 = df_t2_filtered_v6.copy()
        
        df_t2_final_view_f_v6['date'] = df_t2_final_view_f_v6['date'].apply(lambda x: x.strftime('%Y-%m-%d') if not pd.isna(x) else "")
        df_t2_final_view_f_v6['base_rtc'] = df_t2_final_view_f_v6['base_rtc'].apply(format_time_to_hmsf_string)
        st.dataframe(
            df_t2_final_view_f_v6.sort_values("date", ascending=False)[["date", "name", "last_race", "track_kind", "track_week", "race_type", "base_rtc", "f3f", "l3f", "race_l3f", "load", "memo", "next_buy_flag"]], 
            use_container_width=True
        )

# ==============================================================================
# 9. Tab 3: レース実績物理管理
# ==============================================================================

with tab_race_history:
    st.header("🏁 答え合わせ詳細管理")
    df_t3_f = get_db_data()
    if not df_t3_f.empty:
        list_r_all_v = sorted([str(x) for x in df_t3_f['last_race'].dropna().unique()])
        sel_r_v = st.selectbox("対象レースを選択", list_r_all_v)
        if sel_r_v:
            df_sub_v = df_t3_f[df_t3_f['last_race'] == sel_r_v].copy()
            with st.form("form_race_res_t3_f"):
                for i_v, row_v in df_sub_v.iterrows():
                    c_grid_1, c_grid_2, c_grid_3 = st.columns(3)
                    with c_grid_1:
                        val_pos_safe = 0
                        if not pd.isna(row_v['result_pos']):
                            try: val_pos_safe = int(row_v['result_pos'])
                            except: val_pos_safe = 0
                        df_sub_v.at[i_v, 'result_pos'] = st.number_input(f"{row_v['name']} 着順", 0, 100, val_pos_safe, key=f"p_t3_{i_v}")
                    with c_grid_2:
                        val_pop_safe = 0
                        if not pd.isna(row_v['result_pop']):
                            try: val_pop_safe = int(row_v['result_pop'])
                            except: val_pop_safe = 0
                        df_sub_v.at[i_v, 'result_pop'] = st.number_input(f"{row_v['name']} 人気", 0, 100, val_pop_safe, key=f"pop_t3_{i_v}")
                    with c_grid_3:
                        val_kind_safe = str(row_v.get('track_kind', '芝'))
                        if val_kind_safe not in ["芝", "ダート"]: val_kind_safe = "芝"
                        df_sub_v.at[i_v, 'track_kind'] = st.selectbox(f"{row_v['name']} 芝/ダート", ["芝", "ダート"], index=0 if val_kind_safe == "芝" else 1, key=f"k_t3_{i_v}")
                        
                if st.form_submit_button("同期保存"):
                    for i_v, row_v in df_sub_v.iterrows(): 
                        df_t3_f.at[i_v, 'result_pos'] = row_v['result_pos']
                        df_t3_f.at[i_v, 'result_pop'] = row_v['result_pop']
                        df_t3_f.at[i_v, 'track_kind'] = row_v['track_kind']
                    if safe_update(df_t3_f): st.success("同期完了"); st.rerun()
            df_t3_fmt = df_sub_v.copy()
            df_t3_fmt['base_rtc'] = df_t3_fmt['base_rtc'].apply(format_time_to_hmsf_string)
            st.dataframe(df_t3_fmt[["name", "notes", "track_kind", "track_week", "race_type", "base_rtc", "f3f", "l3f", "race_l3f", "result_pos", "result_pop"]], use_container_width=True)

# ==============================================================================
# 10. Tab 4: シミュレーター詳細工程 (v10.0 究極新機能搭載版)
# ==============================================================================

with tab_simulator:
    st.header("🎯 次走シミュレーター詳細物理計算エンジン")
    df_t4_f = get_db_data()
    if not df_t4_f.empty:
        list_h_names_v = sorted([str(x) for x in df_t4_f['name'].dropna().unique()])
        sel_multi_h = st.multiselect("対象馬を物理選択", list_h_names_v)
        sim_w_map = {}
        sim_g_map = {}
        sim_p_map = {}
        
        if sel_multi_h:
            st.markdown("##### 📝 個別物理パラメータ入力")
            grid_sim = st.columns(min(len(sel_multi_h), 4))
            for i_h, h_i in enumerate(sel_multi_h):
                with grid_sim[i_h % 4]:
                    h_lat_data = df_t4_f[df_t4_f['name'] == h_i].iloc[-1]
                    sim_g_map[h_i] = st.number_input(f"{h_i} 枠", 1, 18, value=1, key=f"g_sim_{h_i}")
                    
                    val_raw_pop_sim_f = 10
                    if not pd.isna(h_lat_data['result_pop']):
                        try: val_raw_pop_sim_f = int(h_lat_data['result_pop'])
                        except: val_raw_pop_sim_f = 10
                    val_safe_pop_sim_f = val_raw_pop_sim_f
                    if val_safe_pop_sim_f < 1: val_safe_pop_sim_f = 1
                    if val_safe_pop_sim_f > 18: val_safe_pop_sim_f = 18
                        
                    sim_p_map[h_i] = st.number_input(f"{h_i} 人気", 1, 18, value=val_safe_pop_sim_f, key=f"p_sim_{h_i}")
                    sim_w_map[h_i] = st.number_input(f"{h_i} 斤量", 48.0, 62.0, 56.0, step=0.5, key=f"w_sim_{h_i}")
            
            c_sc_1, c_sc_2 = st.columns(2)
            with c_sc_1:
                val_sim_course = st.selectbox("次走競馬場", list(MASTER_CONFIG_V65_TURF_LOAD_COEFFS.keys()))
                val_sim_dist = st.selectbox("次走距離", list_dist_range_opts_actual_f if 'list_dist_range_opts_actual_f' in locals() else [1600], index=0)
                opt_sim_track = st.radio("次走種別", ["芝", "ダート"], horizontal=True)
                val_sim_race_name = st.text_input("次走レース名（任意・同一レース歴を検索）", value="", placeholder="例: 天皇賞秋、有馬記念")
            with c_sc_2:
                val_sim_cush = st.slider("想定クッション", 7.0, 12.0, 9.5)
                val_sim_water = st.slider("想定含水率", 0.0, 30.0, 10.0)

            if st.button("🏁 物理シミュレーション実行"):
                list_res_v = []
                num_sim_total = len(sel_multi_h)
                
                dict_styles = {"逃げ": 0, "先行": 0, "差し": 0, "追込": 0}
                dict_race_types_v75 = {"瞬発力": 0, "持続力": 0, "自在": 0}
                dict_horse_pref_type_v75 = {}

                for h_n_v in sel_multi_h:
                    df_h_temp = df_t4_f[df_t4_f['name'] == h_n_v].sort_values("date")
                    df_l3_temp = df_h_temp.tail(3)
                    
                    val_avg_load_3r = df_l3_temp['load'].mean()
                    if val_avg_load_3r <= 3.5: style_l = "逃げ"
                    elif val_avg_load_3r <= 7.0: style_l = "先行"
                    elif val_avg_load_3r <= 11.0: style_l = "差し"
                    else: style_l = "追込"
                    dict_styles[style_l] += 1
                    
                    val_count_shunpatsu_v75 = 0
                    val_count_jizoku_v75 = 0
                    for idx_p, row_p in df_h_temp.iterrows():
                        if row_p['result_pos'] <= 5:
                            if row_p['race_type'] == "瞬発力戦": val_count_shunpatsu_v75 += 1
                            elif row_p['race_type'] == "持続力戦": val_count_jizoku_v75 += 1
                    
                    str_pref_race_type_v75 = "自在"
                    if val_count_shunpatsu_v75 > val_count_jizoku_v75: str_pref_race_type_v75 = "瞬発力"
                    elif val_count_jizoku_v75 > val_count_shunpatsu_v75: str_pref_race_type_v75 = "持続力"
                    
                    dict_horse_pref_type_v75[h_n_v] = str_pref_race_type_v75
                    dict_race_types_v75[str_pref_race_type_v75] += 1

                str_sim_pace = "ミドルペース"
                if dict_styles["逃げ"] >= 2 or (dict_styles["逃げ"] + dict_styles["先行"]) >= num_sim_total * 0.6:
                    str_sim_pace = "ハイペース傾向"
                elif dict_styles["逃げ"] == 0 and dict_styles["先行"] <= 1:
                    str_sim_pace = "スローペース傾向"
                    
                str_sim_race_type_forecast_v75 = "瞬発力戦"
                if dict_race_types_v75["持続力"] > dict_race_types_v75["瞬発力"]:
                    str_sim_race_type_forecast_v75 = "持続力戦"

                for h_n_v in sel_multi_h:
                    df_h_v = df_t4_f[df_t4_f['name'] == h_n_v].sort_values("date")
                    df_l3_v = df_h_v.tail(3); list_conv_rtc_v = []
                    
                    val_avg_load_3r = df_l3_v['load'].mean()
                    if val_avg_load_3r <= 3.5: style_l = "逃げ"
                    elif val_avg_load_3r <= 7.0: style_l = "先行"
                    elif val_avg_load_3r <= 11.0: style_l = "差し"
                    else: style_l = "追込"
                    
                    jam_label = "⚠️詰まり注意" if num_sim_total >= 15 and style_l in ["差し", "追込"] and sim_g_map[h_n_v] <= 4 else "-"
                    
                    flag_is_cross_surface = False
                    str_cross_label = ""

                    for idx_r, row_r in df_l3_v.iterrows():
                        p_w_v = 56.0
                        wm_v = re.search(r'([4-6]\d\.\d)', str(row_r['notes']))
                        if wm_v: p_w_v = float(wm_v.group(1))
                        
                        v_h_bw = 480.0
                        match_bw = re.search(r'\((\d{3})kg\)', str(row_r['notes']))
                        if match_bw: v_h_bw = float(match_bw.group(1))
                        
                        sens_v = 0.15 if v_h_bw <= 440 else 0.08 if v_h_bw >= 500 else 0.1
                        w_diff_v = (sim_w_map[h_n_v] - p_w_v) * sens_v
                        
                        v_p_v_l_adj = (row_r['load'] - 7.0) * 0.02
                        v_step1 = (row_r['base_rtc'] + v_p_v_l_adj + w_diff_v)
                        v_step2 = v_step1 / row_r['dist'] if row_r['dist'] > 0 else v_step1 / 1600.0
                        v_step_rtc = v_step2 * val_sim_dist
                        p_v_s_adj = (MASTER_CONFIG_V65_GRADIENT_FACTORS.get(val_sim_course, 0.002) - MASTER_CONFIG_V65_GRADIENT_FACTORS.get(row_r['course'], 0.002)) * val_sim_dist
                        
                        past_track_kind = str(row_r.get('track_kind', '芝'))
                        if pd.isna(past_track_kind) or past_track_kind == 'nan':
                            past_track_kind = '芝'
                            
                        cross_penalty_v = 0.0
                        if past_track_kind == "芝" and opt_sim_track == "ダート":
                            cross_penalty_v = 3.5 * (val_sim_dist / 1600.0)
                            flag_is_cross_surface = True
                            str_cross_label = "🔄初ダ"
                        elif past_track_kind == "ダート" and opt_sim_track == "芝":
                            cross_penalty_v = -3.5 * (val_sim_dist / 1600.0)
                            flag_is_cross_surface = True
                            str_cross_label = "🔄初芝"
                        
                        list_conv_rtc_v.append(v_step_rtc + p_v_s_adj + cross_penalty_v)
                        
                    # 【機能3】トリム平均: 5走以上は最高・最低を1つずつ除外、3〜4走は中央値、1〜2走は単純平均
                    if len(list_conv_rtc_v) >= 5:
                        sorted_rtc = sorted(list_conv_rtc_v)
                        trimmed_rtc = sorted_rtc[1:-1]
                        val_avg_rtc_res = sum(trimmed_rtc) / len(trimmed_rtc)
                    elif len(list_conv_rtc_v) >= 3:
                        sorted_rtc = sorted(list_conv_rtc_v)
                        mid = len(sorted_rtc) // 2
                        val_avg_rtc_res = sorted_rtc[mid]
                    elif list_conv_rtc_v:
                        val_avg_rtc_res = sum(list_conv_rtc_v) / len(list_conv_rtc_v)
                    else:
                        val_avg_rtc_res = 0

                    c_dict_v = MASTER_CONFIG_V65_DIRT_LOAD_COEFFS if opt_sim_track == "ダート" else MASTER_CONFIG_V65_TURF_LOAD_COEFFS
                    final_rtc_v = val_avg_rtc_res + (c_dict_v.get(val_sim_course, 0.20) * (val_sim_dist/1600.0)) - (9.5 - val_sim_cush) * 0.1
                    
                    course_aptitude_bonus_v9 = 0.0
                    aptitude_label_v9 = "初コース"
                    
                    df_same_course_v9 = df_h_v[df_h_v['course'] == val_sim_course]
                    
                    if not df_same_course_v9.empty and len(df_h_v) > 0:
                        list_all_rtc_v9 = []
                        for idx_all, r_all in df_h_v.iterrows():
                            if 0.0 < r_all['base_rtc'] < 300.0: 
                                v_rtc_all = r_all['base_rtc'] / r_all['dist'] if r_all['dist'] > 0 else r_all['base_rtc'] / 1600.0
                                list_all_rtc_v9.append(v_rtc_all * val_sim_dist)
                        avg_all_rtc_v9 = sum(list_all_rtc_v9) / len(list_all_rtc_v9) if list_all_rtc_v9 else 0.0
                        
                        list_same_course_rtc_v9 = []
                        for idx_same, r_same in df_same_course_v9.iterrows():
                            if 0.0 < r_same['base_rtc'] < 300.0:
                                v_rtc_same = r_same['base_rtc'] / r_same['dist'] if r_same['dist'] > 0 else r_same['base_rtc'] / 1600.0
                                list_same_course_rtc_v9.append(v_rtc_same * val_sim_dist)
                        avg_same_course_rtc_v9 = sum(list_same_course_rtc_v9) / len(list_same_course_rtc_v9) if list_same_course_rtc_v9 else 0.0
                        
                        if avg_all_rtc_v9 > 0.0 and avg_same_course_rtc_v9 > 0.0:
                            aptitude_diff_v9 = avg_same_course_rtc_v9 - avg_all_rtc_v9
                            course_aptitude_bonus_v9 = aptitude_diff_v9 * 0.5
                            
                            if aptitude_diff_v9 <= -0.5:
                                aptitude_label_v9 = "🔥コース鬼"
                            elif aptitude_diff_v9 <= -0.1:
                                aptitude_label_v9 = "⭕適性あり"
                            elif aptitude_diff_v9 >= 0.5:
                                aptitude_label_v9 = "❌コース苦手"
                            else:
                                aptitude_label_v9 = "普通"
                                
                    # 🌟 【新機能1 & 2】安定度指数（RTC偏差）と L3F乖離解析（鬼脚判定）
                    list_all_rtc_std_v10 = []
                    list_burst_scores_v10 = []
                    
                    for idx_all, r_all in df_h_v.iterrows():
                        if 0.0 < r_all['base_rtc'] < 300.0:
                            v_rtc_all_std = r_all['base_rtc'] / r_all['dist'] if r_all['dist'] > 0 else r_all['base_rtc'] / 1600.0
                            list_all_rtc_std_v10.append(v_rtc_all_std * val_sim_dist)
                        
                        if r_all['race_l3f'] > 0.0 and r_all['l3f'] > 0.0:
                            val_l3f_diff_v10 = r_all['race_l3f'] - r_all['l3f']
                            val_burst_score_v10 = val_l3f_diff_v10 * (r_all['load'] / 10.0)
                            list_burst_scores_v10.append(val_burst_score_v10)

                    val_std_rtc_v10 = pd.Series(list_all_rtc_std_v10).std() if len(list_all_rtc_std_v10) > 1 else 0.0
                    label_consistency_v10 = "普通"
                    if pd.isna(val_std_rtc_v10) or val_std_rtc_v10 == 0.0:
                        label_consistency_v10 = "判定不能"
                    elif val_std_rtc_v10 <= 0.5:
                        label_consistency_v10 = "🛡️安定(軸向)"
                    elif val_std_rtc_v10 >= 1.5:
                        label_consistency_v10 = "🎲ムラ(穴向)"

                    val_avg_burst_v10 = sum(list_burst_scores_v10) / len(list_burst_scores_v10) if list_burst_scores_v10 else 0.0
                    label_burst_v10 = "-"
                    if val_avg_burst_v10 >= 0.5:
                        label_burst_v10 = "🚀極限鬼脚"
                    elif val_avg_burst_v10 >= 0.2:
                        label_burst_v10 = "💨鋭い脚"

                    # ==============================================================================
                    # 【機能7】ペース適性スコア：過去レースのペース別・展開別成績から算出
                    # ==============================================================================
                    dict_pace_hit_v10 = {"ハイペース": [], "スローペース": [], "ミドルペース": []}
                    dict_racetype_hit_v10 = {"瞬発力戦": [], "持続力戦": []}

                    for idx_pa, row_pa in df_h_v.iterrows():
                        memo_pa = str(row_pa.get('memo', ''))
                        pos_pa = row_pa.get('result_pos', 0)
                        if pd.isna(pos_pa) or pos_pa <= 0:
                            continue
                        hit_pa = 1 if float(pos_pa) <= 3 else 0
                        if 'ハイ' in memo_pa:
                            dict_pace_hit_v10["ハイペース"].append(hit_pa)
                        elif 'スロー' in memo_pa:
                            dict_pace_hit_v10["スローペース"].append(hit_pa)
                        else:
                            dict_pace_hit_v10["ミドルペース"].append(hit_pa)
                        race_type_pa = str(row_pa.get('race_type', ''))
                        if race_type_pa == "瞬発力戦":
                            dict_racetype_hit_v10["瞬発力戦"].append(hit_pa)
                        elif race_type_pa == "持続力戦":
                            dict_racetype_hit_v10["持続力戦"].append(hit_pa)

                    if "ハイ" in str_sim_pace:
                        sim_target_pace_key = "ハイペース"
                    elif "スロー" in str_sim_pace:
                        sim_target_pace_key = "スローペース"
                    else:
                        sim_target_pace_key = "ミドルペース"

                    pace_samples = dict_pace_hit_v10[sim_target_pace_key]
                    if pace_samples:
                        pace_hit_rate_v10 = sum(pace_samples) / len(pace_samples)
                        n_pace = len(pace_samples)
                        if pace_hit_rate_v10 >= 0.5 and n_pace >= 2:
                            label_pace_apt_v10 = f"🔥{sim_target_pace_key}得意({int(pace_hit_rate_v10*100)}%/{n_pace}走)"
                        elif pace_hit_rate_v10 >= 0.3:
                            label_pace_apt_v10 = f"⭕適性あり({int(pace_hit_rate_v10*100)}%/{n_pace}走)"
                        elif n_pace >= 3:
                            label_pace_apt_v10 = f"❌苦手({int(pace_hit_rate_v10*100)}%/{n_pace}走)"
                        else:
                            label_pace_apt_v10 = f"参考({n_pace}走のみ)"
                    else:
                        label_pace_apt_v10 = "実績なし"

                    # 展開タイプ適性も確認してシナジー補正に加える
                    if str_sim_race_type_forecast_v75 == "瞬発力戦":
                        rt_samples = dict_racetype_hit_v10["瞬発力戦"]
                    else:
                        rt_samples = dict_racetype_hit_v10["持続力戦"]
                    if rt_samples:
                        rt_hit_rate_v10 = sum(rt_samples) / len(rt_samples)
                        rt_label_part = f"/{str_sim_race_type_forecast_v75}:{int(rt_hit_rate_v10*100)}%"
                        label_pace_apt_v10 += rt_label_part

                    # ==============================================================================
                    # 【同一レース過去歴】入力したレース名で馬の過去成績を検索
                    # ==============================================================================
                    label_same_race_hist = "-"
                    if val_sim_race_name.strip():
                        # 部分一致で同名レースの過去出走を検索
                        df_same_race_h = df_h_v[df_h_v['last_race'].str.contains(
                            val_sim_race_name.strip(), na=False, case=False
                        )].sort_values("date")

                        if not df_same_race_h.empty:
                            n_same = len(df_same_race_h)
                            # 結果が入力されている行のみで集計
                            df_same_with_res = df_same_race_h[df_same_race_h['result_pos'] > 0]
                            if not df_same_with_res.empty:
                                best_pos = int(df_same_with_res['result_pos'].min())
                                avg_pos = df_same_with_res['result_pos'].mean()
                                n_top3 = len(df_same_with_res[df_same_with_res['result_pos'] <= 3])
                                # 最新出走時のRTC（1600m正規化）
                                last_same = df_same_race_h.iloc[-1]
                                last_same_date = last_same['date']
                                last_date_str = last_same_date.strftime('%Y/%m/%d') if not pd.isna(last_same_date) else "日付不明"
                                if best_pos == 1:
                                    result_icon = "🥇"
                                elif best_pos <= 3:
                                    result_icon = "🥈"
                                elif best_pos <= 5:
                                    result_icon = "✅"
                                else:
                                    result_icon = "📋"
                                label_same_race_hist = f"{result_icon}{n_same}走/最高{best_pos}着/複勝{n_top3}/{len(df_same_with_res)}回({last_date_str})"
                            else:
                                # 出走歴はあるが着順未入力
                                label_same_race_hist = f"📋出走歴{n_same}回（着順未入力）"
                        else:
                            label_same_race_hist = "初出走"

                    # ==============================================================================
                    # RTC上昇・下降トレンド判定（直近3走の正規化RTCが単調改善か悪化かを判定）
                    # compute_synergyでの微小補正に使用する
                    # ==============================================================================
                    rtc_trend_val = "横ばい"
                    valid_rtc_trend = df_h_v[(df_h_v['base_rtc'] > 0) & (df_h_v['base_rtc'] < 999)].tail(3)
                    if len(valid_rtc_trend) >= 3:
                        trend_norm_vals = []
                        for _, r_tr in valid_rtc_trend.iterrows():
                            if r_tr['dist'] > 0:
                                trend_norm_vals.append(r_tr['base_rtc'] / r_tr['dist'] * 1600)
                        if len(trend_norm_vals) >= 3:
                            if trend_norm_vals[0] > trend_norm_vals[1] > trend_norm_vals[2]:
                                rtc_trend_val = "上昇中"
                            elif trend_norm_vals[0] < trend_norm_vals[1] < trend_norm_vals[2]:
                                rtc_trend_val = "下降中"

                    # ==============================================================================
                    # 【機能7】距離適性ボーナス: 次走距離帯での過去複勝率に基づく補正値を算出
                    # ==============================================================================
                    if val_sim_dist <= 1400:
                        dist_apt_range = (0, 1400)
                    elif val_sim_dist <= 1800:
                        dist_apt_range = (1401, 1800)
                    elif val_sim_dist <= 2200:
                        dist_apt_range = (1801, 2200)
                    else:
                        dist_apt_range = (2201, 99999)

                    df_dist_apt = df_h_v[
                        (df_h_v['dist'] >= dist_apt_range[0]) & (df_h_v['dist'] <= dist_apt_range[1]) &
                        (df_h_v['result_pos'] > 0)
                    ]
                    df_all_results = df_h_v[df_h_v['result_pos'] > 0]
                    dist_apt_bonus = 0.0
                    dist_apt_label = "-"
                    if len(df_dist_apt) >= 2 and len(df_all_results) >= 2:
                        dist_fuku_rate = len(df_dist_apt[df_dist_apt['result_pos'] <= 3]) / len(df_dist_apt)
                        all_fuku_rate = len(df_all_results[df_all_results['result_pos'] <= 3]) / len(df_all_results)
                        dist_diff = dist_fuku_rate - all_fuku_rate
                        dist_apt_bonus = -dist_diff * 0.5
                        if dist_diff >= 0.2:
                            dist_apt_label = f"🔥距離得意({int(dist_fuku_rate*100)}%)"
                        elif dist_diff <= -0.2:
                            dist_apt_label = f"❌距離苦手({int(dist_fuku_rate*100)}%)"
                        else:
                            dist_apt_label = f"普通({int(dist_fuku_rate*100)}%)"

                    list_res_v.append({
                        "馬名": h_n_v, "脚質": style_l, "得意展開": dict_horse_pref_type_v75[h_n_v],
                        "路線変更": str_cross_label if flag_is_cross_surface else "-",
                        "コース適性": aptitude_label_v9, 
                        "安定度": label_consistency_v10,
                        "鬼脚": label_burst_v10,
                        "ペース適性": label_pace_apt_v10,
                        "同一レース歴": label_same_race_hist,
                        "RTCトレンド": "🔼上昇中" if rtc_trend_val == "上昇中" else "🔽下降中" if rtc_trend_val == "下降中" else "➡️横ばい",
                        "距離適性": dist_apt_label,
                        "想定タイム": final_rtc_v, "渋滞": jam_label, 
                        "load": f"{val_avg_load_3r:.1f}", "raw_rtc": final_rtc_v, "解析メモ": df_h_v.iloc[-1]['memo'],
                        "is_cross": flag_is_cross_surface,
                        "course_bonus": course_aptitude_bonus_v9,
                        "rtc_trend": rtc_trend_val,
                        "std_rtc": val_std_rtc_v10 if not pd.isna(val_std_rtc_v10) else 0.0,
                        "dist_apt_bonus": dist_apt_bonus,
                    })
                
                df_final_v = pd.DataFrame(list_res_v)
                
                val_sim_p_mult = 1.5 if num_sim_total >= 15 else 1.0
                def compute_synergy(row):
                    adj = 0.0
                    if "ハイ" in str_sim_pace:
                        if row['脚質'] in ["差し", "追込"]: adj -= 0.2 * val_sim_p_mult
                        elif row['脚質'] == "逃げ": adj += 0.2 * val_sim_p_mult
                    elif "スロー" in str_sim_pace:
                        if row['脚質'] in ["逃げ", "先行"]: adj -= 0.2 * val_sim_p_mult
                        elif row['脚質'] in ["差し", "追込"]: adj += 0.2 * val_sim_p_mult
                    
                    if str_sim_race_type_forecast_v75 == "瞬発力戦":
                        if row['得意展開'] == "瞬発力": adj -= 0.15
                        elif row['得意展開'] == "持続力": adj += 0.15
                    elif str_sim_race_type_forecast_v75 == "持続力戦":
                        if row['得意展開'] == "持続力": adj -= 0.15
                        elif row['得意展開'] == "瞬発力": adj += 0.15
                        
                    if row.get('is_cross', False):
                        adj += 0.0
                        
                    adj += row.get('course_bonus', 0.0)

                    # RTCトレンド補正
                    trend = row.get('rtc_trend', '横ばい')
                    if trend == "上昇中":
                        adj -= 0.15
                    elif trend == "下降中":
                        adj += 0.15

                    # 【機能2】安定度補正: 標準偏差が小さい（安定）馬はボーナス、大きい（ムラ）馬はペナルティ
                    std_v = row.get('std_rtc', 0.0)
                    if std_v > 0:
                        if std_v <= 0.5:
                            adj -= 0.1
                        elif std_v >= 1.5:
                            adj += 0.1

                    # 【機能7】距離適性補正
                    adj += row.get('dist_apt_bonus', 0.0)

                    return row['raw_rtc'] + adj

                df_final_v['synergy_rtc'] = df_final_v.apply(compute_synergy, axis=1)

                # 【機能6】相対評価（フィールド内偏差値）: 出走馬間のsynergy_rtcを偏差値化してソート
                if len(df_final_v) >= 3:
                    rtc_mean = df_final_v['synergy_rtc'].mean()
                    rtc_std = df_final_v['synergy_rtc'].std()
                    if rtc_std > 0:
                        # RTCは低い方が良い → 偏差値は高い方が良い（符号反転）
                        df_final_v['相対偏差値'] = (50 - (df_final_v['synergy_rtc'] - rtc_mean) / rtc_std * 10).round(1)
                    else:
                        df_final_v['相対偏差値'] = 50.0
                else:
                    df_final_v['相対偏差値'] = 50.0

                df_final_v = df_final_v.sort_values("synergy_rtc")
                df_final_v['順位'] = range(1, len(df_final_v) + 1)
                
                df_final_v['役割'] = "-"
                df_final_v.loc[df_final_v['順位'] == 1, '役割'] = "◎"
                df_final_v.loc[df_final_v['順位'] == 2, '役割'] = "〇"
                df_final_v.loc[df_final_v['順位'] == 3, '役割'] = "▲"
                
                df_final_v['予想人気'] = df_final_v['馬名'].map(sim_p_map)
                df_final_v['妙味スコア'] = df_final_v['予想人気'] - df_final_v['順位']
                
                # 🌟 【新機能4】推定オッズ乖離・期待値判定
                def evaluate_expected_value_v10(row):
                    gap = row['妙味スコア']
                    if row['順位'] <= 3 and row['予想人気'] >= 6:
                        return "🔥爆・期待値最高"
                    elif gap >= 3:
                        return "📈妙味あり"
                    elif gap <= -3:
                        return "⚠️過剰人気"
                    else:
                        return "妥当"

                df_final_v['期待値'] = df_final_v.apply(evaluate_expected_value_v10, axis=1)
                
                df_bomb = df_final_v[df_final_v['順位'] > 1].sort_values("妙味スコア", ascending=False)
                if not df_bomb.empty:
                     df_final_v.loc[df_final_v['馬名'] == df_bomb.iloc[0]['馬名'], '役割'] = "★"

                st.markdown("---")
                st.subheader(f"🏁 展開予想：{str_sim_pace} × {str_sim_race_type_forecast_v75} ({num_sim_total}頭立て)")

                # 【機能7】逃げ馬複数警告 & ペース予測根拠を明示表示
                if dict_styles["逃げ"] >= 2:
                    st.error(f"🚨 **逃げ馬複数警告**: 逃げ脚質が{dict_styles['逃げ']}頭確認。ハイペース確定に近く、先行馬も苦戦必至。差し・追込馬を優先的に評価してください。")
                elif dict_styles["逃げ"] == 1 and (dict_styles["逃げ"] + dict_styles["先行"]) >= num_sim_total * 0.5:
                    st.warning(f"⚠️ **前傾メンバー構成**: 先行勢が過半数 ({dict_styles['逃げ']}逃げ/{dict_styles['先行']}先行)。ペースが上がりやすく差し馬の台頭に注意。")
                elif dict_styles["逃げ"] == 0:
                    st.info(f"ℹ️ **逃げ馬不在**: 逃げ脚質0頭。先行争いが激化しにくく、スローペース後の瞬発力勝負になりやすい構成です。")

                col_pace_detail1, col_pace_detail2, col_pace_detail3, col_pace_detail4 = st.columns(4)
                with col_pace_detail1:
                    st.metric("逃げ", f"{dict_styles['逃げ']}頭")
                with col_pace_detail2:
                    st.metric("先行", f"{dict_styles['先行']}頭")
                with col_pace_detail3:
                    st.metric("差し", f"{dict_styles['差し']}頭")
                with col_pace_detail4:
                    st.metric("追込", f"{dict_styles['追込']}頭")
                
                # 本命(1～5人気)1頭・相手(6～9人気)1頭・相手(10人気以下)1頭の2点流し（10人気以下で力不足の馬は除外し1点勝負に）
                df_1_5 = df_final_v[(df_final_v['予想人気'] >= 1) & (df_final_v['予想人気'] <= 5)].sort_values("synergy_rtc")
                df_6_9 = df_final_v[(df_final_v['予想人気'] >= 6) & (df_final_v['予想人気'] <= 9)].sort_values("synergy_rtc")
                df_10_plus = df_final_v[df_final_v['予想人気'] >= 10].sort_values("synergy_rtc")

                honmei_name = ""
                aite_6_9_name = ""
                aite_10_name = ""

                if not df_1_5.empty:
                    honmei_name = df_1_5.iloc[0]['馬名']
                if not df_6_9.empty:
                    aite_6_9_name = df_6_9.iloc[0]['馬名']

                # 10人気以下からは「力が足りる」馬のみ採用（相対偏差値42以上＝フィールド中位以上の実力）
                if not df_10_plus.empty:
                    df_10_strong = df_10_plus[df_10_plus['相対偏差値'] >= 42]
                    if not df_10_strong.empty:
                        aite_10_name = df_10_strong.iloc[0]['馬名']

                col_rec1, col_rec2 = st.columns(2)
                with col_rec1:
                    st.subheader("🎯 買い目（馬連・ワイド）")
                    if honmei_name:
                        lines = [f"**本命（1～5人気）**: {honmei_name}"]
                        if aite_6_9_name:
                            lines.append(f"**相手（6～9人気）**: {aite_6_9_name}")
                        if aite_10_name:
                            lines.append(f"**相手（10人気以下）**: {aite_10_name}")

                        if aite_6_9_name and aite_10_name:
                            lines.append("")
                            lines.append("**2点流し**")
                            lines.append(f"① {honmei_name} × {aite_6_9_name}")
                            lines.append(f"② {honmei_name} × {aite_10_name}")
                        elif aite_6_9_name:
                            lines.append("")
                            lines.append("**1点勝負**（10人気以下に力あり馬なし）")
                            lines.append(f"{honmei_name} × {aite_6_9_name}")
                        else:
                            lines.append("")
                            lines.append("（6～9人気の馬がいないため買い目を出せません）")
                        st.info("\n\n".join(lines))
                    else:
                        st.warning("1～5人気の馬がいないため買い目を出せません。")

                with col_rec2:
                    bomb_name = df_final_v[df_final_v['役割'] == "★"].iloc[0]['馬名'] if not df_final_v[df_final_v['役割'] == "★"].empty else ""
                    if honmei_name and bomb_name:
                        st.warning(f"**💣 妙味狙いワイド 1点**\n\n◎ {honmei_name} － ★ {bomb_name}")

                df_final_v['想定タイム'] = df_final_v['raw_rtc'].apply(format_time_to_hmsf_string)
                
                def highlight_role(row):
                    if row['役割'] == '◎': return ['background-color: #ffffcc; font-weight: bold; color: black'] * len(row)
                    if row['役割'] == '★': return ['background-color: #ffe6e6; font-weight: bold'] * len(row)
                    return [''] * len(row)

                # 同一レース歴カラムはレース名入力時のみ表示
                if val_sim_race_name.strip():
                    sim_display_cols = ["役割", "順位", "相対偏差値", "馬名", "予想人気", "期待値", "RTCトレンド", "距離適性", "同一レース歴", "脚質", "得意展開", "ペース適性", "路線変更", "コース適性", "安定度", "鬼脚", "渋滞", "load", "想定タイム", "解析メモ"]
                else:
                    sim_display_cols = ["役割", "順位", "相対偏差値", "馬名", "予想人気", "期待値", "RTCトレンド", "距離適性", "脚質", "得意展開", "ペース適性", "路線変更", "コース適性", "安定度", "鬼脚", "渋滞", "load", "想定タイム", "解析メモ"]
                st.table(df_final_v[sim_display_cols].style.apply(highlight_role, axis=1))

# ==============================================================================
# 11. Tab 5: トレンド統計詳細 & Tab 6: 物理管理詳細
# ==============================================================================

with tab_trends:
    st.header("📈 馬場トレンド詳細物理統計")
    df_t5_f = get_db_data()
    if not df_t5_f.empty:
        sel_c_v = st.selectbox("トレンド競馬場指定", list(MASTER_CONFIG_V65_TURF_LOAD_COEFFS.keys()), key="tc_v5_final")
        tdf_v = df_t5_f[df_t5_f['course'] == sel_c_v].sort_values("date")
        if not tdf_v.empty:
            st.subheader("💧 物理推移グラフ")
            st.line_chart(tdf_v.set_index("date")[["cushion", "water"]])

# ==============================================================================
# 11.5. Tab バックテスト: 回収率分析 / ケリー基準 / 上昇馬ピックアップ
# ==============================================================================

with tab_backtest:
    st.header("📊 バックテスト & 回収率分析エンジン")
    df_bt = get_db_data()

    # 結果が入力された行のみ対象（result_pos > 0 かつ result_pop > 0）
    df_bt_valid = df_bt[(df_bt['result_pos'] > 0) & (df_bt['result_pop'] > 0)].copy()

    if df_bt_valid.empty:
        st.info("まだ結果が入力されたデータがありません。「レース別履歴」タブで着順・人気を入力するとバックテストが可能になります。")
    else:
        # 人気順位から単勝オッズの推定値マップ（日本競馬標準的な近似値）
        BACKTEST_SINGLE_ODDS_MAP = {
            1: 2.5, 2: 4.2, 3: 6.5, 4: 10.0, 5: 15.0,
            6: 23.0, 7: 35.0, 8: 55.0, 9: 80.0, 10: 110.0
        }

        def bt_estimate_win_odds(pop_val):
            try:
                p_int = int(float(pop_val))
                if p_int in BACKTEST_SINGLE_ODDS_MAP:
                    return BACKTEST_SINGLE_ODDS_MAP[p_int]
                elif p_int > 10:
                    return 110.0 + (p_int - 10) * 25.0
                return 100.0
            except Exception:
                return 100.0

        df_bt_valid['est_win_odds'] = df_bt_valid['result_pop'].apply(bt_estimate_win_odds)
        df_bt_valid['hit_top1'] = (df_bt_valid['result_pos'] == 1).astype(int)
        df_bt_valid['hit_top2'] = (df_bt_valid['result_pos'] <= 2).astype(int)
        df_bt_valid['hit_top3'] = (df_bt_valid['result_pos'] <= 3).astype(int)

        # ============================================================
        # セクション1: 買いフラグ別 回収率シミュレーション
        # ============================================================
        st.subheader("🎯 買いフラグ別 回収率シミュレーション")
        st.caption("※単勝オッズは人気順位からの推定値です。実際の払い戻しとは異なります。")

        flag_condition_defs = [
            ("★逆行狙い (次走フラグあり)", lambda r: "★逆行狙い" in str(r['next_buy_flag'])),
            ("💎バイアス逆行を含む", lambda r: "💎" in str(r['memo'])),
            ("🔥展開逆行を含む", lambda r: "🔥" in str(r['memo'])),
            ("💥両方逆行 (超高評価)", lambda r: "💎" in str(r['memo']) and "🔥" in str(r['memo'])),
            ("全記録（ベースライン比較）", lambda r: True),
        ]

        analysis_result_rows = []
        for flag_display_name, flag_cond_fn in flag_condition_defs:
            df_flag_sub = df_bt_valid[df_bt_valid.apply(flag_cond_fn, axis=1)].copy()
            if len(df_flag_sub) == 0:
                continue

            n_total = len(df_flag_sub)
            win_rate_v = df_flag_sub['hit_top1'].mean()
            rentan_rate_v = df_flag_sub['hit_top2'].mean()
            fuku_rate_v = df_flag_sub['hit_top3'].mean()
            avg_pop_v = df_flag_sub['result_pop'].mean()

            # 推定単勝回収率 = 的中時オッズ合計 / 総投票数 * 100
            total_return_v = df_flag_sub[df_flag_sub['hit_top1'] == 1]['est_win_odds'].sum()
            roi_single_v = (total_return_v / n_total) * 100 if n_total > 0 else 0.0

            # ケリー基準: f* = (b*p - (1-p)) / b
            avg_odds_v = df_flag_sub['est_win_odds'].mean()
            b_kelly = avg_odds_v - 1.0
            p_kelly = win_rate_v
            if b_kelly > 0 and p_kelly > 0:
                kelly_fraction = max(0.0, (b_kelly * p_kelly - (1.0 - p_kelly)) / b_kelly)
                kelly_display = f"{kelly_fraction * 100:.1f}%" if kelly_fraction > 0 else "見送り推奨"
            else:
                kelly_display = "見送り推奨"

            analysis_result_rows.append({
                "フラグ種別": flag_display_name,
                "対象数": n_total,
                "単勝率": f"{win_rate_v * 100:.1f}%",
                "連対率": f"{rentan_rate_v * 100:.1f}%",
                "複勝率": f"{fuku_rate_v * 100:.1f}%",
                "平均人気": f"{avg_pop_v:.1f}",
                "推定単勝回収率": f"{roi_single_v:.0f}%",
                "ケリー推奨賭け比率": kelly_display,
            })

        if analysis_result_rows:
            df_analysis_display = pd.DataFrame(analysis_result_rows)
            st.dataframe(df_analysis_display, use_container_width=True, hide_index=True)

        st.divider()

        # ============================================================
        # セクション2: 馬別 回収率ランキング
        # ============================================================
        st.subheader("🐎 馬別 推定回収率ランキング（Top20）")
        st.caption("走数2走以上のデータがある馬のみ対象。単勝回収率ベースでソート。")

        horse_analysis_rows = []
        for h_name_bt in df_bt_valid['name'].dropna().unique():
            df_h_bt = df_bt_valid[df_bt_valid['name'] == h_name_bt].copy()
            n_h_bt = len(df_h_bt)
            if n_h_bt < 2:
                continue

            win_r_h = df_h_bt['hit_top1'].mean()
            fuku_r_h = df_h_bt['hit_top3'].mean()
            total_ret_h = df_h_bt[df_h_bt['hit_top1'] == 1]['est_win_odds'].sum()
            roi_h = (total_ret_h / n_h_bt) * 100 if n_h_bt > 0 else 0.0
            avg_pop_h = df_h_bt['result_pop'].mean()

            horse_analysis_rows.append({
                "馬名": h_name_bt,
                "走数": n_h_bt,
                "複勝率": f"{fuku_r_h * 100:.0f}%",
                "単勝率": f"{win_r_h * 100:.0f}%",
                "平均人気": f"{avg_pop_h:.1f}",
                "推定単勝回収率": f"{roi_h:.0f}%",
                "_roi_sort": roi_h,
            })

        if horse_analysis_rows:
            df_horse_rank_bt = pd.DataFrame(horse_analysis_rows).sort_values('_roi_sort', ascending=False).drop('_roi_sort', axis=1)
            st.dataframe(df_horse_rank_bt.head(20), use_container_width=True, hide_index=True)

        st.divider()

        # ============================================================
        # セクション3: 上昇馬ピックアップ（RTC推移分析）
        # ============================================================
        st.subheader("🔼 上昇馬ピックアップ（直近3走でRTC単調改善）")
        st.caption("直近3走の正規化RTC（1600m換算）が継続的に低下（改善）している馬を自動抽出します。")

        rising_horse_rows = []
        for h_name_rising in df_bt['name'].dropna().unique():
            df_h_rising = df_bt[df_bt['name'] == h_name_rising].sort_values("date")
            valid_rtc_rising = df_h_rising[(df_h_rising['base_rtc'] > 0) & (df_h_rising['base_rtc'] < 999)].tail(3)
            if len(valid_rtc_rising) < 3:
                continue
            norm_rtc_rising = []
            for _, r_rising in valid_rtc_rising.iterrows():
                if r_rising['dist'] > 0:
                    norm_rtc_rising.append(r_rising['base_rtc'] / r_rising['dist'] * 1600)
            if len(norm_rtc_rising) >= 3 and norm_rtc_rising[0] > norm_rtc_rising[1] > norm_rtc_rising[2]:
                last_entry_rising = df_h_rising.iloc[-1]
                improvement = norm_rtc_rising[0] - norm_rtc_rising[2]
                last_date_rising = last_entry_rising['date']
                last_date_str = last_date_rising.strftime('%Y-%m-%d') if not pd.isna(last_date_rising) else ""
                rising_horse_rows.append({
                    "馬名": h_name_rising,
                    "直近3走 正規化RTC": f"{norm_rtc_rising[0]:.2f} → {norm_rtc_rising[1]:.2f} → {norm_rtc_rising[2]:.2f}",
                    "総改善幅": f"{improvement:.2f}秒",
                    "最終レース": str(last_entry_rising.get('last_race', '')),
                    "最終日付": last_date_str,
                })

        if rising_horse_rows:
            df_rising_display = pd.DataFrame(rising_horse_rows).sort_values("総改善幅", ascending=False)
            st.dataframe(df_rising_display, use_container_width=True, hide_index=True)
        else:
            st.info("直近3走で連続的にRTCが改善している馬は現在いません。データを蓄積すると自動的に表示されます。")

        st.divider()

        # ============================================================
        # セクション4: 同一レース過去歴検索
        # ============================================================
        st.subheader("🔍 同一レース過去歴検索")
        st.caption("レース名（部分一致）を入力すると、そのレースに出走歴がある全馬の成績を表示します。")

        bt_race_search_query = st.text_input("検索するレース名", value="", placeholder="例: 天皇賞、有馬、マイルCS", key="bt_race_search_q")

        if bt_race_search_query.strip():
            # 部分一致で対象レースを絞り込む
            df_bt_race_all = get_db_data()
            df_race_matched = df_bt_race_all[
                df_bt_race_all['last_race'].str.contains(bt_race_search_query.strip(), na=False, case=False)
            ].copy()

            if df_race_matched.empty:
                st.warning(f"「{bt_race_search_query}」に一致するレース出走歴が見つかりません。")
            else:
                # マッチしたレース名の一覧を表示
                matched_race_names = sorted(df_race_matched['last_race'].dropna().unique().tolist())
                st.info(f"マッチしたレース: {', '.join(matched_race_names)} （計{len(df_race_matched)}件）")

                # 馬ごとに集計
                same_race_summary_rows = []
                for h_name_sr in df_race_matched['name'].dropna().unique():
                    df_h_sr = df_race_matched[df_race_matched['name'] == h_name_sr].sort_values("date")
                    n_sr = len(df_h_sr)

                    df_h_sr_with_res = df_h_sr[df_h_sr['result_pos'] > 0]
                    best_pos_sr = int(df_h_sr_with_res['result_pos'].min()) if not df_h_sr_with_res.empty else None
                    avg_pos_sr = df_h_sr_with_res['result_pos'].mean() if not df_h_sr_with_res.empty else None
                    n_top3_sr = len(df_h_sr_with_res[df_h_sr_with_res['result_pos'] <= 3]) if not df_h_sr_with_res.empty else 0

                    # 最新出走時のRTC（正規化）
                    last_sr = df_h_sr.iloc[-1]
                    last_date_sr = last_sr['date']
                    last_date_str_sr = last_date_sr.strftime('%Y-%m-%d') if not pd.isna(last_date_sr) else ""
                    last_rtc_sr = last_sr['base_rtc']
                    last_dist_sr = last_sr['dist']
                    norm_rtc_sr = (last_rtc_sr / last_dist_sr * 1600) if (last_dist_sr > 0 and 0 < last_rtc_sr < 999) else None

                    if best_pos_sr == 1:
                        result_icon_sr = "🥇"
                    elif best_pos_sr is not None and best_pos_sr <= 3:
                        result_icon_sr = "🥈"
                    elif best_pos_sr is not None and best_pos_sr <= 5:
                        result_icon_sr = "✅"
                    elif best_pos_sr is not None:
                        result_icon_sr = "📋"
                    else:
                        result_icon_sr = "❓"

                    same_race_summary_rows.append({
                        "": result_icon_sr,
                        "馬名": h_name_sr,
                        "出走回数": n_sr,
                        "最高着順": f"{best_pos_sr}着" if best_pos_sr is not None else "着順未入力",
                        "平均着順": f"{avg_pos_sr:.1f}" if avg_pos_sr is not None else "-",
                        "複勝回数": f"{n_top3_sr}/{len(df_h_sr_with_res)}" if not df_h_sr_with_res.empty else "-",
                        "最終出走日": last_date_str_sr,
                        "最終レース正規化RTC": f"{norm_rtc_sr:.2f}" if norm_rtc_sr is not None else "-",
                        "最終レース名": str(last_sr.get('last_race', '')),
                    })

                if same_race_summary_rows:
                    # 最高着順でソート（着順が良い順 = 数値が小さい順）
                    df_sr_display = pd.DataFrame(same_race_summary_rows)
                    # 最高着順を数値で一時ソート
                    df_sr_display['_sort_key'] = df_sr_display['最高着順'].str.extract(r'(\d+)').astype(float).fillna(99)
                    df_sr_display = df_sr_display.sort_values('_sort_key').drop('_sort_key', axis=1)
                    st.dataframe(df_sr_display, use_container_width=True, hide_index=True)

with tab_management:
    st.header("🗑 物理管理 & 再解析工程詳細")
    if st.button("🔄 スプレッドシート強制物理同期 (全破棄)"):
        st.cache_data.clear(); st.rerun()
    df_t6_f = get_db_data()
    
    def update_eval_tags_verbose_logic_final_step(row_v, df_ctx_v=None):
        m_r_v = str(row_v['memo']) if not pd.isna(row_v['memo']) else ""
        def to_f_v(v_in, default=0.0):
            try: return float(v_in) if not pd.isna(v_in) else default
            except: return default
        
        f3f_v = to_f_v(row_v['f3f'])
        l3f_v = to_f_v(row_v['l3f'])
        race_l3f_v = to_f_v(row_v['race_l3f'])
        pos_v = to_f_v(row_v['result_pos'])
        l_pos_v = to_f_v(row_v['load'])
        old_rtc_v = to_f_v(row_v['base_rtc'])
        
        raw_time_v = to_f_v(row_v.get('raw_time', 0.0))
        track_idx_v = to_f_v(row_v.get('track_idx', 0.0))
        bias_slider_v = to_f_v(row_v.get('bias_slider', 0.0))
        cushion_v = to_f_v(row_v.get('cushion', 9.5), 9.5)
        water_v = to_f_v(row_v.get('water', 10.0), 10.0)
        dist_v = to_f_v(row_v.get('dist', 1600.0), 1600.0)
        week_v = to_f_v(row_v.get('track_week', 1.0), 1.0)
        
        str_n_v = str(row_v['notes'])
        m_w_v = re.search(r'([4-6]\d\.\d)', str_n_v)
        indiv_w_v = float(m_w_v.group(1)) if m_w_v else 56.0
        
        pace_gap_v = f3f_v - race_l3f_v
        
        bt_label_v = "フラット"; mx_field_v = 16
        if df_ctx_v is not None and not pd.isna(row_v['last_race']):
            rc_sub_v = df_ctx_v[df_ctx_v['last_race'] == row_v['last_race']]
            mx_field_v = rc_sub_v['result_pos'].max() if not rc_sub_v.empty else 16
            
            top3_v = rc_sub_v[rc_sub_v['result_pos'] <= 3].copy()
            top3_v['load'] = top3_v['load'].fillna(7.0)
            
            if not top3_v.empty:
                outlier_mask = (top3_v['load'] >= 10.0) | (top3_v['load'] <= 3.0)
                outliers = top3_v[outlier_mask]
                
                if len(outliers) == 1:
                    core_top3 = top3_v[~outlier_mask]
                    fourth_v = rc_sub_v[rc_sub_v['result_pos'] == 4].copy()
                    fourth_v['load'] = fourth_v['load'].fillna(7.0)
                    bias_calc_pool = pd.concat([core_top3, fourth_v])
                else:
                    bias_calc_pool = top3_v
                    
                if not bias_calc_pool.empty:
                    avg_l_v = bias_calc_pool['load'].mean()
                    if avg_l_v <= 4.0: bt_label_v = "前有利"
                    elif avg_l_v >= 10.0: bt_label_v = "後有利"
        
        ps_label_v = "ハイペース" if "ハイ" in m_r_v else "スローペース" if "スロー" in m_r_v else "ミドルペース"
        
        val_rel_ratio_v = l_pos_v / mx_field_v if mx_field_v > 0 else l_pos_v / 16.0
        val_scale_v = mx_field_v / 16.0
        val_computed_load_v = 0.0
        
        if ps_label_v == "ハイペース" and bt_label_v != "前有利":
            val_computed_load_v = max(0.0, (0.6 - val_rel_ratio_v) * abs(pace_gap_v) * 3.0) * val_scale_v 
        elif ps_label_v == "スローペース" and bt_label_v != "後有利":
            val_computed_load_v = max(0.0, (val_rel_ratio_v - 0.4) * abs(pace_gap_v) * 2.0) * val_scale_v 

        list_tags_v = []
        flag_is_counter_v = False
        
        if pos_v <= 5:
            if (bt_label_v == "前有利" and l_pos_v >= 10.0) or (bt_label_v == "後有利" and l_pos_v <= 3.0):
                list_tags_v.append("💎💎 ﾊﾞｲｱｽ極限逆行" if mx_field_v >= 16 else "💎 ﾊﾞｲｱｽ逆行"); flag_is_counter_v = True
        
        if not ((ps_label_v == "ハイペース" and bt_label_v == "前有利") or (ps_label_v == "スローペース" and bt_label_v == "後有利")):
            if ps_label_v == "ハイペース" and l_pos_v <= 3.0 and pos_v <= 5: 
                list_tags_v.append("📉 激流被害" if mx_field_v >= 14 else "🔥 展開逆行"); flag_is_counter_v = True
            elif ps_label_v == "スローペース" and l_pos_v >= 10.0 and (f3f_v - l3f_v) > 1.5 and pos_v <= 5: 
                list_tags_v.append("🔥 展開逆行"); flag_is_counter_v = True

        str_field_tag_v = "多" if mx_field_v >= 16 else "少" if mx_field_v <= 10 else "中"

        race_type_v = str(row_v.get('race_type', '不明'))
        if pd.isna(race_type_v) or race_type_v == 'nan': race_type_v = "不明"

        mu_final_v = f"【{ps_label_v}({race_type_v})/{bt_label_v}/負荷:{val_computed_load_v:.1f}({str_field_tag_v})/平】{'/'.join(list_tags_v) if list_tags_v else '順境'}"
        
        new_rtc_v = old_rtc_v
        if raw_time_v > 0.0 and raw_time_v != 999.0:
            r_p1 = raw_time_v
            r_p2 = (indiv_w_v - 56.0) * 0.1
            r_p3 = track_idx_v / 10.0
            r_p4 = val_computed_load_v / 10.0
            r_p5 = (week_v - 1) * 0.05
            r_p8 = (water_v - 10.0) * 0.05
            r_p9 = (9.5 - cushion_v) * 0.1
            r_p10 = (dist_v - 1600) * 0.0005
            new_rtc_v = r_p1 - r_p2 - r_p3 - r_p4 - r_p5 + bias_slider_v - r_p8 - r_p9 + r_p10
        elif raw_time_v == 999.0:
            new_rtc_v = 999.0

        return mu_final_v, str(row_v['next_buy_flag']), new_rtc_v

    if st.button("🔄 物理データベース全記録の再計算・物理同期"):
        st.cache_data.clear()
        latest_df_v = conn.read(ttl=0)
        for c_nm in ABSOLUTE_COLUMN_STRUCTURE_DEFINITION_GLOBAL:
            if c_nm not in latest_df_v.columns: latest_df_v[c_nm] = None
        for idx_sy, row_sy in latest_df_v.iterrows():
            m_res, f_res, rtc_res = update_eval_tags_verbose_logic_final_step(row_sy, latest_df_v)
            latest_df_v.at[idx_sy, 'memo'] = m_res
            latest_df_v.at[idx_sy, 'next_buy_flag'] = f_res
            latest_df_v.at[idx_sy, 'base_rtc'] = rtc_res
        if safe_update(latest_df_v): st.success("全履歴の真・再解析成功"); st.rerun()

    if not df_t6_f.empty:
        st.subheader("🛠️ 物理エディタ同期修正工程")
        
        df_for_editor = df_t6_f.copy()
        df_for_editor['date'] = df_for_editor['date'].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notnull(x) else "")
        df_for_editor['base_rtc'] = df_for_editor['base_rtc'].apply(format_time_to_hmsf_string)
        
        edf_f_v = st.data_editor(df_for_editor.sort_values("date", ascending=False), num_rows="dynamic", use_container_width=True)
        
        if st.button("💾 エディタ修正内容を同期確定保存"):
            sdf_f_v = edf_f_v.copy()
            sdf_f_v['base_rtc'] = sdf_f_v['base_rtc'].apply(parse_time_string_to_seconds)
            if safe_update(sdf_f_v): st.success("物理同期完了"); st.rerun()
        
        st.divider(); st.subheader("❌ 物理全抹消詳細設定")
        cd1_v, cd2_v = st.columns(2)
        with cd1_v:
            list_r_v = sorted([str(x) for x in df_t6_f['last_race'].dropna().unique()])
            tr_del_v = st.selectbox("抹消レース物理選択", ["未選択"] + list_r_v)
            if tr_del_v != "未選択" and st.button(f"🚨 レース単位抹消実行"):
                if safe_update(df_t6_f[df_t6_f['last_race'] != tr_del_v]): st.rerun()
        with cd2_v:
            list_h_v = sorted([str(x) for x in df_t6_f['name'].dropna().unique()])
            target_h_multi_v = st.multiselect("抹消対象馬物理選択 (複数可)", list_h_v)
            if target_h_multi_v and st.button(f"🚨 選択した{len(target_h_multi_v)}頭を物理抹消"):
                if safe_update(df_t6_f[~df_t6_f['name'].isin(target_h_multi_v)]): st.rerun()
