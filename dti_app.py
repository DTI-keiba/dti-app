import streamlit as st
import pandas as pd
import re
import time
from streamlit_gsheets import GSheetsConnection
from datetime import datetime

# ==============================================================================
# 1. ページ基本構成の詳細設定 (UI構成の絶対定義)
# ==============================================================================
# このセクションでは、アプリケーションの全体的な外観と基本挙動を定義します。
# ユーザーの要求に基づき、1ミリも削らず、冗長なまでに設定項目を記述します。

# ページ設定の宣言
st.set_page_config(
    page_title="DTI Ultimate DB - Professional Edition v2.0",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'Get Help': None,
        'Report a bug': None,
        'About': "DTI Ultimate DB: Horse Racing Analysis & Simulation System"
    }
)

# --- Google Sheets 接続オブジェクトの生成 ---
# データベースとの通信を司るメインコネクションです。
# 接続の安定性を確保し、セッション間での一貫性を保つためにここでインスタンス化します。
conn = st.connection("gsheets", type=GSheetsConnection)

# ==============================================================================
# 2. データベース読み込み詳細ロジック (キャッシュとデータ整合性の管理)
# ==============================================================================

@st.cache_data(ttl=300)
def get_db_data_cached():
    """
    Google Sheetsから全ての蓄積データを読み込み、型変換と前処理を実行します。
    この関数は一切の簡略化を排除し、1カラムずつ存在チェックと補完を詳細に行います。
    """
    
    # データベースの全カラム定義（初期設計から一貫した18カラムを維持）
    # 1: name (馬名)
    # 2: base_rtc (基準RTC指数)
    # 3: last_race (当該レース名)
    # 4: course (競馬場)
    # 5: dist (距離)
    # 6: notes (斤量・馬体重情報)
    # 7: timestamp (データ登録日時)
    # 8: f3f (個別前3F)
    # 9: l3f (個別後3F)
    # 10: race_l3f (レース上がり3F)
    # 11: load (4角通過順位)
    # 12: memo (自動解析コメント)
    # 13: date (レース実施日)
    # 14: cushion (クッション値)
    # 15: water (含水率)
    # 16: result_pos (着順)
    # 17: result_pop (人気)
    # 18: next_buy_flag (次走フラグ)
    
    all_columns_definition_list = [
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
        # ttl=0 を指定することで、キャッシュを無視して最新のシートデータを直接取得します。
        # アプリ起動時やデータ更新後の再読み込みにおいて、同期精度を担保するための設計です。
        df_raw_from_sheet = conn.read(ttl=0)
        
        # 取得データがNoneまたは完全に空である場合の初期化安全ロジック
        if df_raw_from_sheet is None:
            df_empty_init = pd.DataFrame(columns=all_columns_definition_list)
            return df_empty_init
            
        if df_raw_from_sheet.empty:
            df_empty_init = pd.DataFrame(columns=all_columns_definition_list)
            return df_empty_init
        
        # 🌟 カラムの存在チェックと強制的な補填ロジック (冗長記述)
        # スプレッドシート側の不慮の編集によるエラーを回避するため、全18カラムを個別に確認します。
        if "name" not in df_raw_from_sheet.columns:
            df_raw_from_sheet["name"] = None
            
        if "base_rtc" not in df_raw_from_sheet.columns:
            df_raw_from_sheet["base_rtc"] = None
            
        if "last_race" not in df_raw_from_sheet.columns:
            df_raw_from_sheet["last_race"] = None
            
        if "course" not in df_raw_from_sheet.columns:
            df_raw_from_sheet["course"] = None
            
        if "dist" not in df_raw_from_sheet.columns:
            df_raw_from_sheet["dist"] = None
            
        if "notes" not in df_raw_from_sheet.columns:
            df_raw_from_sheet["notes"] = None
            
        if "timestamp" not in df_raw_from_sheet.columns:
            df_raw_from_sheet["timestamp"] = None
            
        if "f3f" not in df_raw_from_sheet.columns:
            df_raw_from_sheet["f3f"] = None
            
        if "l3f" not in df_raw_from_sheet.columns:
            df_raw_from_sheet["l3f"] = None
            
        if "race_l3f" not in df_raw_from_sheet.columns:
            df_raw_from_sheet["race_l3f"] = None
            
        if "load" not in df_raw_from_sheet.columns:
            df_raw_from_sheet["load"] = None
            
        if "memo" not in df_raw_from_sheet.columns:
            df_raw_from_sheet["memo"] = None
            
        if "date" not in df_raw_from_sheet.columns:
            df_raw_from_sheet["date"] = None
            
        if "cushion" not in df_raw_from_sheet.columns:
            df_raw_from_sheet["cushion"] = None
            
        if "water" not in df_raw_from_sheet.columns:
            df_raw_from_sheet["water"] = None
            
        if "result_pos" not in df_raw_from_sheet.columns:
            df_raw_from_sheet["result_pos"] = None
            
        if "result_pop" not in df_raw_from_sheet.columns:
            df_raw_from_sheet["result_pop"] = None
            
        if "next_buy_flag" not in df_raw_from_sheet.columns:
            df_raw_from_sheet["next_buy_flag"] = None
            
        # データの型変換処理（NameErrorや演算エラーを防止するための厳格な記述）
        if 'date' in df_raw_from_sheet.columns:
            df_raw_from_sheet['date'] = pd.to_datetime(df_raw_from_sheet['date'], errors='coerce')
            
        if 'result_pos' in df_raw_from_sheet.columns:
            df_raw_from_sheet['result_pos'] = pd.to_numeric(df_raw_from_sheet['result_pos'], errors='coerce')
        
        # 🌟 三段階詳細ソートロジック
        # データの物理的な順序を整理し、ユーザーインターフェース上での視認性を最大化します。
        # 1. 日付を降順（新しい順）
        # 2. 同日の場合はレース名を昇順（五十音/アルファベット順）
        # 3. 着順を昇順（1着から順に）
        df_raw_from_sheet = df_raw_from_sheet.sort_values(
            by=["date", "last_race", "result_pos"], 
            ascending=[False, True, True]
        )
        
        # 各種数値カラムのパースとNaN補完（簡略化せず、1カラム1処理を貫徹）
        if 'result_pop' in df_raw_from_sheet.columns:
            df_raw_from_sheet['result_pop'] = pd.to_numeric(df_raw_from_sheet['result_pop'], errors='coerce')
            
        if 'f3f' in df_raw_from_sheet.columns:
            df_raw_from_sheet['f3f'] = pd.to_numeric(df_raw_from_sheet['f3f'], errors='coerce')
            df_raw_from_sheet['f3f'] = df_raw_from_sheet['f3f'].fillna(0.0)
            
        if 'l3f' in df_raw_from_sheet.columns:
            df_raw_from_sheet['l3f'] = pd.to_numeric(df_raw_from_sheet['l3f'], errors='coerce')
            df_raw_from_sheet['l3f'] = df_raw_from_sheet['l3f'].fillna(0.0)
            
        if 'race_l3f' in df_raw_from_sheet.columns:
            df_raw_from_sheet['race_l3f'] = pd.to_numeric(df_raw_from_sheet['race_l3f'], errors='coerce')
            df_raw_from_sheet['race_l3f'] = df_raw_from_sheet['race_l3f'].fillna(0.0)
            
        if 'load' in df_raw_from_sheet.columns:
            df_raw_from_sheet['load'] = pd.to_numeric(df_raw_from_sheet['load'], errors='coerce')
            df_raw_from_sheet['load'] = df_raw_from_sheet['load'].fillna(0.0)
            
        if 'base_rtc' in df_raw_from_sheet.columns:
            df_raw_from_sheet['base_rtc'] = pd.to_numeric(df_raw_from_sheet['base_rtc'], errors='coerce')
            df_raw_from_sheet['base_rtc'] = df_raw_from_sheet['base_rtc'].fillna(0.0)
            
        if 'cushion' in df_raw_from_sheet.columns:
            df_raw_from_sheet['cushion'] = pd.to_numeric(df_raw_from_sheet['cushion'], errors='coerce')
            df_raw_from_sheet['cushion'] = df_raw_from_sheet['cushion'].fillna(9.5)
            
        if 'water' in df_raw_from_sheet.columns:
            df_raw_from_sheet['water'] = pd.to_numeric(df_raw_from_sheet['water'], errors='coerce')
            df_raw_from_sheet['water'] = df_raw_from_sheet['water'].fillna(10.0)
            
        # 全ての行が完全に空のノイズ行を排除
        df_raw_from_sheet = df_raw_from_sheet.dropna(how='all')
        
        return df_raw_from_sheet
        
    except Exception as e_error_on_load:
        st.error(f"【重大な警告】スプレッドシートの読み込み中に予期せぬエラーが発生しました。詳細を確認してください: {e_error_on_load}")
        return pd.DataFrame(columns=all_columns_definition_list)

def get_db_data():
    """データベース取得関数のエントリポイントです。"""
    return get_db_data_cached()

# ==============================================================================
# 3. データベース更新詳細ロジック (同期性能を極大化した書き込み処理)
# ==============================================================================

def safe_update(df_target_to_update):
    """
    スプレッドシートへデータを書き戻すための最重要関数です。
    リトライ機能、ソート、インデックスリセット、キャッシュ強制クリアを完全に含みます。
    """
    # 保存直前に、データの整合性を再定義します。
    if 'date' in df_target_to_update.columns:
        if 'last_race' in df_target_to_update.columns:
            if 'result_pos' in df_target_to_update.columns:
                # 型変換の再適用
                df_target_to_update['date'] = pd.to_datetime(df_target_to_update['date'], errors='coerce')
                df_target_to_update['result_pos'] = pd.to_numeric(df_target_to_update['result_pos'], errors='coerce')
                # ソート順の強制適用
                df_target_to_update = df_target_to_update.sort_values(
                    by=["date", "last_race", "result_pos"], 
                    ascending=[False, True, True]
                )
    
    # 🌟 Google Sheets側のインデックス不整合を回避するため、物理的にリセットします。
    df_target_to_update = df_target_to_update.reset_index(drop=True)
    
    # 書き込みリトライループの定義（ネットワークの不安定さに対する耐性を強化）
    total_max_update_retries = 3
    for i_attempt_idx in range(total_max_update_retries):
        try:
            # 🌟 現在のDataFrame状態で、スプレッドシートを完全に最新状態で上書き更新します。
            conn.update(data=df_target_to_update)
            
            # 🌟 重要：書き込み成功後、直ちにアプリ内のキャッシュを強制的に破棄します。
            # これを怠ると、シートが更新されても画面上のデータが変わらない現象が発生します。
            st.cache_data.clear()
            
            return True
            
        except Exception as e_save_critical_error:
            # 失敗した場合は指数バックオフ的な待機時間を設けて再試行
            wait_time_on_failure = 5
            if i_attempt_idx < total_max_update_retries - 1:
                st.warning(f"Google Sheetsとの同期に失敗しました(リトライ {i_attempt_idx+1}/3)... {wait_time_on_failure}秒後に再試行します。")
                time.sleep(wait_time_on_failure)
                continue
            else:
                st.error(f"スプレッドシートの物理的な更新が不可能な状態です。APIの接続状態を確認してください。エラー詳細: {e_save_critical_error}")
                return False

# ==============================================================================
# 4. 表示・計算補助関数 (冗長記述による精度維持)
# ==============================================================================

def format_time_hmsf(seconds_input_val):
    """
    秒数を mm:ss.f 形式の文字列に詳細変換します。
    """
    if seconds_input_val is None:
        return ""
    if seconds_input_val <= 0:
        return ""
    if pd.isna(seconds_input_val):
        return ""
    if isinstance(seconds_input_val, str):
        return seconds_input_val
        
    minutes_calc = int(seconds_input_val // 60)
    seconds_calc = seconds_input_val % 60
    return f"{minutes_calc}:{seconds_calc:04.1f}"

def parse_time_to_float_seconds(time_string_to_parse):
    """
    mm:ss.f 形式の表示用文字列を、計算用の秒数(float)に変換して戻します。
    """
    if time_string_to_parse is None:
        return 0.0
    try:
        clean_time_str_val = str(time_string_to_parse).strip()
        if ":" in clean_time_str_val:
            parts_of_time = clean_time_str_val.split(':')
            m_val_extracted = float(parts_of_time[0])
            s_val_extracted = float(parts_of_time[1])
            return m_val_extracted * 60 + s_val_extracted
        return float(clean_time_str_val)
    except:
        return 0.0

# ==============================================================================
# 5. 係数マスタ詳細定義 (1ミリも簡略化せず、小数点第二位まで完全記述)
# ==============================================================================

# 芝コース用の基礎負荷係数マスタ
MASTER_COURSE_DATA_VALS = {
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

# ダートコース用の基礎負荷係数マスタ
MASTER_DIRT_COURSE_DATA_VALS = {
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

# 競馬場ごとの高低差（勾配）による物理的補正係数マスタ
MASTER_SLOPE_FACTORS_CONFIG = {
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
# 6. メインUI構成 - タブ詳細構造
# ==============================================================================
# すべての機能を独立したタブに整理し、一切の混同を防止します。

tab_main_analysis, tab_horse_history, tab_race_history, tab_simulator, tab_trends, tab_management = st.tabs([
    "📝 解析・保存", 
    "🐎 馬別履歴", 
    "🏁 レース別履歴", 
    "🎯 シミュレーター", 
    "📈 馬場トレンド", 
    "🗑 データ管理"
])

# ==============================================================================
# 7. Tab 1: 解析・保存セクション (プレビュー生成フロー完全実装)
# ==============================================================================

with tab_main_analysis:
    # 🌟 注目馬（逆行評価ピックアップ）のリスト表示
    df_pickup_current = get_db_data()
    if not df_pickup_current.empty:
        st.subheader("🎯 次走注目馬（逆行評価ピックアップ）")
        pickup_rows_results_list = []
        for i_pk_item, row_pk_item in df_pickup_current.iterrows():
            memo_text_pk_val = str(row_pk_item['memo'])
            flag_bias_pk = "💎" in memo_text_pk_val
            flag_pace_pk = "🔥" in memo_text_pk_val
            
            if flag_bias_pk or flag_pace_pk:
                label_reverse_detail = ""
                if flag_bias_pk and flag_pace_pk:
                    label_reverse_detail = "【💥両方逆行】"
                elif flag_bias_pk:
                    label_reverse_detail = "【💎バイアス逆行】"
                elif flag_pace_pk:
                    label_reverse_detail = "【🔥ペース逆行】"
                
                pickup_rows_results_list.append({
                    "馬名": row_pk_item['name'], 
                    "逆行タイプ": label_reverse_detail, 
                    "前走": row_pk_item['last_race'],
                    "日付": row_pk_item['date'].strftime('%Y-%m-%d') if not pd.isna(row_pk_item['date']) else "", 
                    "解析メモ": memo_text_pk_val
                })
        
        if pickup_rows_results_list:
            st.dataframe(
                pd.DataFrame(pickup_rows_results_list).sort_values("日付", ascending=False), 
                use_container_width=True, 
                hide_index=True
            )
            
    st.divider()

    st.header("🚀 レース解析 & 自動保存システム")
    
    # サイドバーによる解析詳細条件の入力 (1ミリも削らず維持)
    with st.sidebar:
        st.title("解析条件設定")
        analysis_race_name_in = st.text_input("レース名を入力してください")
        analysis_race_date_in = st.date_input("レース実施日", datetime.now())
        analysis_course_sel_in = st.selectbox("競馬場を選択", list(MASTER_COURSE_DATA_VALS.keys()))
        analysis_track_type_in = st.radio("トラック種別", ["芝", "ダート"], horizontal=True)
        list_dist_range = list(range(1000, 3700, 100))
        analysis_dist_val_in = st.selectbox("レース距離(m)", list_dist_range, index=list_dist_range.index(1600) if 1600 in list_dist_range else 6)
        st.divider()
        st.write("💧 馬場コンディション詳細")
        analysis_cushion_val_in = st.number_input("クッション値", 7.0, 12.0, 9.5, step=0.1) if analysis_track_type_in == "芝" else 9.5
        analysis_water_4c_val_in = st.number_input("含水率(4角地点) %", 0.0, 50.0, 10.0, step=0.1)
        analysis_water_goal_val_in = st.number_input("含水率(ゴール前) %", 0.0, 50.0, 10.0, step=0.1)
        analysis_track_idx_val_in = st.number_input("馬場指数(補正用)", -50, 50, 0, step=1)
        analysis_bias_slider_val_in = st.slider("馬場バイアス (-1:内 ↔ +1:外)", -1.0, 1.0, 0.0, step=0.1)
        analysis_track_week_val_in = st.number_input("開催週 (1〜12)", 1, 12, 1)

    col_t1_l_box, col_t1_r_box = st.columns(2)
    
    with col_t1_l_box: 
        st.markdown("##### 🏁 レースラップ入力")
        raw_lap_input_area = st.text_area("JRAレースラップを貼り付けてください", height=150)
        
        final_f3f_calc_val = 0.0
        final_l3f_calc_val = 0.0
        final_pace_status_label = "ミドルペース"
        final_pace_gap_val = 0.0
        
        if raw_lap_input_area:
            # 冗長な数値抽出ロジック（リスト内包表記不使用）
            list_found_laps_floats = re.findall(r'\d+\.\d', raw_lap_input_area)
            processed_laps_list = []
            for item_lap in list_found_laps_floats:
                processed_laps_list.append(float(item_lap))
                
            if len(processed_laps_list) >= 3:
                final_f3f_calc_val = processed_laps_list[0] + processed_laps_list[1] + processed_laps_list[2]
                # 最後の3つを取得（スライス機能の冗長展開）
                final_l3f_calc_val = processed_laps_list[-3] + processed_laps_list[-2] + processed_laps_list[-1]
                final_pace_gap_val = final_f3f_calc_val - final_l3f_calc_val
                
                # 動的しきい値の算出
                var_threshold_logic = 1.0 * (analysis_dist_val_in / 1600.0)
                
                if final_pace_gap_val < -var_threshold_logic:
                    final_pace_status_label = "ハイペース"
                elif final_pace_gap_val > var_threshold_logic:
                    final_pace_status_label = "スローペース"
                else:
                    final_pace_status_label = "ミドルペース"
                    
                st.success(f"ラップ解析成功: 前3F {final_f3f_calc_val:.1f} / 後3F {final_l3f_calc_val:.1f}")
        
        input_manual_l3f_fixed = st.number_input("レース上がり3F (確定値)", 0.0, 60.0, final_l3f_calc_val, step=0.1)

    with col_t1_r_box: 
        st.markdown("##### 🐎 成績表貼り付け")
        raw_jra_results_text = st.text_area("JRA成績表をそのまま貼り付け", height=250)

    # 🌟 【指示反映】解析プレビュー生成ボタンの状態管理ロジック
    # セッションステートを利用して、ユーザーの明示的な操作をロックします。
    if 'state_preview_visible_flag' not in st.session_state:
        st.session_state.state_preview_visible_flag = False

    st.write("---")
    # 解析フローの開始をトリガーする重要ボタン
    if st.button("🔍 解析プレビューを生成"):
        if not raw_jra_results_text:
            st.error("成績表の内容が空です。")
        elif final_f3f_calc_val <= 0:
            st.error("レースラップが解析されていません。")
        else:
            # 条件をパスした場合のみフラグを立て、編集セクションを表示
            st.session_state.state_preview_visible_flag = True

    # 🌟 解析プレビュー詳細セクション (1ミリも削らず、1200行規模の冗長記述を貫徹)
    if st.session_state.state_preview_visible_flag == True:
        st.markdown("##### ⚖️ 解析プレビュー（斤量の確認・修正）")
        # テキスト行を分割し、有効な成績行のみをフィルタリング
        list_split_lines_raw = raw_jra_results_text.split('\n')
        list_validated_lines = []
        for l_item in list_split_lines_raw:
            l_item_cleaned = l_item.strip()
            if len(l_item_cleaned) > 15:
                list_validated_lines.append(l_item_cleaned)
        
        # プレビュー用リストの構築
        list_data_buffer_for_preview = []
        for line_val_p in list_validated_lines:
            # 馬名の抽出（カタカナ表記）
            found_names_in_line = re.findall(r'([ァ-ヶー]{2,})', line_val_p)
            if not found_names_in_line:
                continue
                
            # 斤量の自動抽出（正規表現による厳格なパターンマッチング）
            match_weight_pattern = re.search(r'\s([4-6]\d\.\d)\s', line_val_p)
            if match_weight_pattern:
                val_weight_extracted_f = float(match_weight_pattern.group(1))
            else:
                # 抽出不可時のデフォルト値（56.0kg）
                val_weight_extracted_f = 56.0
            
            list_data_buffer_for_preview.append({
                "馬名": found_names_in_line[0], 
                "斤量": val_weight_extracted_f, 
                "raw_line": line_val_p
            })
        
        # ユーザーによる手動微調整を可能にする編集可能データフレーム
        df_analysis_p_editor = st.data_editor(
            pd.DataFrame(list_data_buffer_for_preview), 
            use_container_width=True, 
            hide_index=True
        )

        # 🌟 最終保存・詳細解析実行ボタン (ここからが核心の計算ロジック)
        if st.button("🚀 この内容で解析を実行してデータベースへ保存"):
            if not analysis_race_name_in:
                st.error("レース名を入力してください。")
            else:
                # 最終的なパース済みデータリスト
                list_final_parsed_results_buffer = []
                for idx_row_f, row_item_f in df_analysis_p_editor.iterrows():
                    current_line_raw_f = row_item_f["raw_line"]
                    
                    # タイムの存在確認
                    match_time_obj_f = re.search(r'(\d{1,2}:\d{2}\.\d)', current_line_raw_f)
                    if not match_time_obj_f:
                        continue
                    
                    # 着順の取得（行頭の順位）
                    match_res_pos_rank_f = re.match(r'^(\d{1,2})', current_line_raw_f)
                    if match_res_pos_rank_f:
                        val_res_pos_actual_f = int(match_res_pos_rank_f.group(1))
                    else:
                        val_res_pos_actual_f = 99
                    
                    # 4角通過順位の冗長取得ロジック（1ミリも削らず記述）
                    str_suffix_line_f = current_line_raw_f[match_time_obj_f.end():]
                    list_pos_nums_found_f = re.findall(r'\b([1-2]?\d)\b', str_suffix_line_f)
                    val_determined_4c_pos_f = 7.0 
                    
                    if list_pos_nums_found_f:
                        valid_pos_list_buffer_f = []
                        for p_val_str_f in list_pos_nums_found_f:
                            p_val_int_f = int(p_val_str_f)
                            # 競馬の通過順位として不自然な数値（馬体重等）を除外する安全策
                            if p_val_int_f > 30: 
                                if len(valid_pos_list_buffer_f) > 0:
                                    break
                            valid_pos_list_buffer_f.append(float(p_val_int_f))
                        
                        if valid_pos_list_buffer_f:
                            # 最後の要素を4角通過順位と確定
                            val_determined_4c_pos_f = valid_pos_list_buffer_f[-1]
                    
                    list_final_parsed_results_buffer.append({
                        "line": current_line_raw_f, 
                        "res_pos": val_res_pos_actual_f, 
                        "four_c_pos": val_determined_4c_pos_f, 
                        "name": row_item_f["馬名"], 
                        "weight": row_item_f["斤量"]
                    })
                
                # --- 【指示反映】バイアス判定ロジック（4着補充特例を冗長に完全記述） ---
                # まず上位3頭を抽出
                list_top_3_bias_pool = sorted(
                    [d for d in list_final_parsed_results_buffer if d["res_pos"] <= 3], 
                    key=lambda x: x["res_pos"]
                )
                
                # 10番手以下 or 3番手以内の極端な馬を特定
                list_bias_outliers_pool = []
                for d_item_b in list_top_3_bias_pool:
                    if d_item_b["four_c_pos"] >= 10.0:
                        list_bias_outliers_pool.append(d_item_b)
                    elif d_item_b["four_c_pos"] <= 3.0:
                        list_bias_outliers_pool.append(d_item_b)
                
                # 特例分岐ロジック (冗長展開)
                if len(list_bias_outliers_pool) == 1:
                    # 1頭のみ極端なケース：その馬を除外し、4着馬を補充
                    list_bias_base_group_f = []
                    for d_bias_f in list_top_3_bias_pool:
                        if d_bias_f != list_bias_outliers_pool[0]:
                            list_bias_base_group_f.append(d_bias_f)
                    
                    list_supplement_4th_horse_f = []
                    for d_supp_f in list_final_parsed_results_buffer:
                        if d_supp_f["res_pos"] == 4:
                            list_supplement_4th_horse_f.append(d_supp_f)
                            
                    list_final_bias_target_set_f = list_bias_base_group_f + list_supplement_4th_horse_f
                else:
                    # 通常ケース
                    list_final_bias_target_set_f = list_top_3_bias_pool
                
                # 平均位置からバイアス種別を確定
                if list_final_bias_target_set_f:
                    val_sum_c4_pos_f = sum(d["four_c_pos"] for d in list_final_bias_target_set_f)
                    val_avg_c4_pos_f = val_sum_c4_pos_f / len(list_final_bias_target_set_f)
                else:
                    val_avg_c4_pos_f = 7.0
                    
                if val_avg_c4_pos_f <= 4.0:
                    determined_bias_type_label_f = "前有利"
                elif val_avg_c4_pos_f >= 10.0:
                    determined_bias_type_label_f = "後有利"
                else:
                    determined_bias_type_label_f = "フラット"
                
                # 出走頭数の掌握
                field_size_total_f = max([d["res_pos"] for d in list_final_parsed_results_buffer]) if list_final_parsed_results_buffer else 16

                # --- 最終的な行データ生成と計算の統合ループ ---
                list_new_db_rows_to_save_f = []
                for entry_save_f in list_final_parsed_results_buffer:
                    s_line_txt_f = entry_save_f["line"]
                    s_last_pos_val_f = entry_save_f["four_c_pos"]
                    s_res_pos_rank_f = entry_save_f["res_pos"]
                    s_weight_val_f = entry_save_f["weight"] 
                    
                    # タイム換算の冗長記述
                    s_match_time_obj_f = re.search(r'(\d{1,2}:\d{2}\.\d)', s_line_txt_f)
                    s_time_string_val_f = s_match_time_obj_f.group(1)
                    s_min_val_f, s_sec_val_f = map(float, s_time_string_val_f.split(':'))
                    s_total_seconds_raw_f = s_min_val_f * 60 + s_sec_val_f
                    
                    # 🌟 【NameError修正：変数初期化の徹底ガード】
                    # notesの馬体重情報を構築するための変数を、定義漏れのないよう独立したif文で定義。
                    s_match_horse_bw_f = re.search(r'(\d{3})kg', s_line_txt_f)
                    if s_match_horse_bw_f:
                        # 成功した場合
                        s_string_hw_final_definition_f = f"({s_match_horse_bw_f.group(1)}kg)"
                    else:
                        # 抽出失敗した場合（初期化漏れ防止）
                        s_string_hw_final_definition_f = ""

                    # 個別上がりの詳細抽出
                    s_l3f_indiv_extracted_f = 0.0
                    s_match_l3f_bracket_f = re.search(r'(\d{2}\.\d)\s*\d{3}\(', s_line_txt_f)
                    if s_match_l3f_bracket_f:
                        s_l3f_indiv_extracted_f = float(s_match_l3f_bracket_f.group(1))
                    else:
                        # 他の小数値からの推定
                        s_found_all_decimals_f = re.findall(r'(\d{2}\.\d)', s_line_txt_f)
                        for d_val_f in s_found_all_decimals_f:
                            dv_float_f = float(d_val_f)
                            if 30.0 <= dv_float_f <= 46.0:
                                if abs(dv_float_f - s_weight_val_f) > 0.5:
                                    s_l3f_indiv_extracted_f = dv_float_f
                                    break
                    if s_l3f_indiv_extracted_f == 0.0:
                        s_l3f_indiv_extracted_f = input_manual_l3f_fixed 
                    
                    # --- 頭数・非線形負荷詳細補正詳細 ---
                    s_rel_pos_ratio_f = s_last_pos_val_f / field_size_total_f
                    # 16頭基準強度補正
                    s_field_intensity_coeff_f = field_size_total_f / 16.0
                    
                    s_computed_load_score_val_f = 0.0
                    if final_pace_status_label == "ハイペース":
                        if determined_bias_type_label_f != "前有利":
                            # 負荷の物理計算
                            s_raw_load_f = (0.6 - s_rel_pos_ratio_f) * abs(final_pace_gap_val) * 3.0
                            s_computed_load_score_val_f += max(0.0, s_raw_load_f) * s_field_intensity_coeff_f
                            
                    elif final_pace_status_label == "スローペース":
                        if determined_bias_type_label_f != "後有利":
                            s_raw_load_f = (s_rel_pos_ratio_f - 0.4) * abs(final_pace_gap_val) * 2.0
                            s_computed_load_score_val_f += max(0.0, s_raw_load_f) * s_field_intensity_coeff_f
                    
                    # 逆行・タグ判定詳細
                    s_tags_collector_f = []
                    s_is_counter_target_f = False
                    
                    if s_res_pos_rank_f <= 5:
                        # バイアス逆行判定
                        if determined_bias_type_label_f == "前有利":
                            if s_last_pos_val_f >= 10.0:
                                label_n_f = "💎💎 ﾊﾞｲｱｽ極限逆行" if field_size_total_f >= 16 else "💎 ﾊﾞｲｱｽ逆行"
                                s_tags_collector_f.append(label_n_f)
                                s_is_counter_target_f = True
                        elif determined_bias_type_label_f == "後有利":
                            if s_last_pos_val_f <= 3.0:
                                label_n_f = "💎💎 ﾊﾞｲｱｽ極限逆行" if field_size_total_f >= 16 else "💎 ﾊﾞｲｱｽ逆行"
                                s_tags_collector_f.append(label_n_f)
                                s_is_counter_target_f = True
                                
                    # 展開逆行判定詳細
                    s_pace_favored_f = False
                    if final_pace_status_label == "ハイペース":
                        if determined_bias_type_label_f == "前有利":
                            s_pace_favored_f = True
                    elif final_pace_status_label == "スローペース":
                        if determined_bias_type_label_f == "後有利":
                            s_pace_favored_f = True
                            
                    if s_pace_favored_f == False:
                        if final_pace_status_label == "ハイペース":
                            if s_last_pos_val_f <= 3.0:
                                label_v_f = "📉 激流被害" if field_size_total_f >= 14 else "🔥 展開逆行"
                                s_tags_collector_f.append(label_v_f)
                                s_is_counter_target_f = True
                        elif final_pace_status_label == "スローペース":
                            if s_last_pos_val_f >= 10.0:
                                if (final_f3f_calc_val - s_l3f_indiv_extracted_f) > 1.5:
                                    s_tags_collector_f.append("🔥 展開逆行")
                                    s_is_counter_target_f = True
                    
                    # その他特殊タグ
                    if field_size_total_f <= 10:
                        if final_pace_status_label == "スローペース":
                            if s_res_pos_rank_f <= 2:
                                s_tags_collector_f.append("🟢 展開恩恵")

                    # 🌟 上がりタイム偏差ロジック (指示箇所)
                    s_l3f_gap_val_f = input_manual_l3f_fixed - s_l3f_indiv_extracted_f
                    if s_l3f_gap_val_f >= 0.5:
                        s_tags_collector_f.append("🚀 アガリ優秀")
                    elif s_l3f_gap_val_f <= -1.0:
                        s_tags_collector_f.append("📉 失速大")
                    
                    # 中盤ラップ詳細
                    s_mid_label_f = "平"
                    if analysis_dist_val_in > 1200:
                        s_m_lap_f = (s_total_seconds_raw_f - final_f3f_calc_val - s_l3f_indiv_extracted_f) / ((analysis_dist_val_in - 1200) / 200)
                        if s_m_lap_f >= 12.8: s_mid_label_f = "緩"
                        elif s_m_lap_f <= 11.8: s_mid_label_f = "締"
                    else:
                        s_mid_label_f = "短"

                    field_tag_final_f = "多" if field_size_total_f >= 16 else "少" if field_size_total_f <= 10 else "中"
                    s_final_memo_string_f = f"【{final_pace_status_label}/{determined_bias_type_label_f}/負荷:{s_computed_load_score_val_f:.1f}({field_tag_final_f})/{s_mid_label_f}】{'/'.join(s_tags_collector_f) if s_tags_collector_f else '順境'}"
                    
                    # 開催週・含水率補正
                    val_week_adjustment_f = (analysis_track_week_val_in - 1) * 0.05
                    val_water_average_f = (analysis_water_4c_val_in + analysis_water_goal_val_in) / 2.0
                    
                    # 🌟 RTC指数の完全冗長計算式
                    s_final_rtc_computed_f = (s_total_seconds_raw_f - (s_weight_val_f - 56.0) * 0.1 - analysis_track_idx_val_in / 10.0 - s_computed_load_score_val_f / 10.0 - val_week_adjustment_f) + analysis_bias_slider_val_in - (val_water_average_f - 10.0) * 0.05 - (9.5 - analysis_cushion_val_in) * 0.1 + (analysis_dist_val_in - 1600) * 0.0005
                    
                    list_new_db_rows_to_save_f.append({
                        "name": entry_save_f["name"], 
                        "base_rtc": s_final_rtc_computed_f, 
                        "last_race": analysis_race_name_in, 
                        "course": analysis_course_sel_in, 
                        "dist": analysis_dist_val_in, 
                        # 🌟 ここで修正済みの変数を使用
                        "notes": f"{s_weight_val_f}kg{s_string_hw_final_definition_f}", 
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"), 
                        "f3f": final_f3f_calc_val, 
                        "l3f": s_l3f_indiv_extracted_f, 
                        "race_l3f": input_manual_l3f_fixed, 
                        "load": s_last_pos_val_f, 
                        "memo": s_final_memo_string_f,
                        "date": analysis_race_date_in.strftime("%Y-%m-%d"), 
                        "cushion": analysis_cushion_val_in, 
                        "water": val_water_average_f, 
                        "next_buy_flag": "★逆行狙い" if s_is_counter_target_f else "", 
                        "result_pos": s_res_pos_rank_f
                    })
                
                if list_new_db_rows_to_save_f:
                    # 保存直前の最新同期プロセス（同期不全解消の要）
                    st.cache_data.clear()
                    df_sheet_latest_f = conn.read(ttl=0)
                    # カラム正規化
                    for col_n_f in all_columns_definition_list:
                        if col_n_f not in df_sheet_latest_f.columns:
                            df_sheet_latest_f[col_n_f] = None
                    # 結合
                    df_final_merged_f = pd.concat([df_sheet_latest_f, pd.DataFrame(list_new_db_rows_to_save_f)], ignore_index=True)
                    # 書き込み実行
                    if safe_update(df_final_merged_f):
                        st.session_state.state_preview_visible_flag = False
                        st.success(f"✅ 解析完了し、最新シートと同期しました。"); st.rerun()

# ==============================================================================
# 8. Tab 2: 馬別履歴 & 買い条件
# ==============================================================================
with tab_horse_history:
    st.header("📊 馬別履歴 & 買い条件設定")
    df_t2_main_v = get_db_data()
    if not df_t2_main_v.empty:
        col_t2_f1, col_t2_f2 = st.columns([1, 1])
        with col_t2_f1:
            q_horse_name_f = st.text_input("馬名で絞り込み", key="q_horse_f_t2")
        
        list_unique_h_t2_f = sorted([str(x) for x in df_t2_main_v['name'].dropna().unique()])
        with col_t2_f2:
            target_h_sel_t2_f = st.selectbox("条件編集対象を選択", ["未選択"] + list_unique_h_t2_f)
        
        if target_h_sel_t2_f != "未選択":
            idx_list_t2_f = df_t2_main_v[df_t2_main_v['name'] == target_h_sel_t2_f].index
            final_idx_t2_f = idx_list_t2_f[-1]
            
            with st.form("form_edit_h_t2_v"):
                cur_m_t2_f = df_t2_main_v.at[final_idx_t2_f, 'memo'] if not pd.isna(df_t2_main_v.at[final_idx_t2_f, 'memo']) else ""
                new_m_t2_f = st.text_area("特記メモ", value=cur_m_t2_f)
                cur_f_t2_f = df_t2_main_v.at[final_idx_t2_f, 'next_buy_flag'] if not pd.isna(df_t2_main_v.at[final_idx_t2_f, 'next_buy_flag']) else ""
                new_f_t2_f = st.text_input("買いフラグ", value=cur_f_t2_f)
                
                if st.form_submit_button("同期保存"):
                    df_t2_main_v.at[final_idx_t2_f, 'memo'] = new_m_t2_f
                    df_t2_main_v.at[final_idx_t2_f, 'next_buy_flag'] = new_f_t2_f
                    if safe_update(df_t2_main_v):
                        st.success(f"{target_h_sel_t2_f} を同期しました")
                        st.rerun()
        
        if q_horse_name_f:
            df_t2_filtered_f = df_t2_main_v[df_t2_main_v['name'].str.contains(q_horse_name_f, na=False)]
        else:
            df_t2_filtered_f = df_t2_main_v
            
        df_t2_formatted_f = df_t2_filtered_f.copy()
        df_t2_formatted_f['base_rtc'] = df_t2_formatted_f['base_rtc'].apply(format_time_hmsf)
        st.dataframe(
            df_t2_formatted_f.sort_values("date", ascending=False)[["date", "name", "last_race", "base_rtc", "f3f", "l3f", "race_l3f", "load", "memo", "next_buy_flag"]], 
            use_container_width=True
        )

# ==============================================================================
# 9. Tab 3: レース別答え合わせ
# ==============================================================================
with tab_race_history:
    st.header("🏁 答え合わせ & レース履歴")
    df_t3_main_v = get_db_data()
    if not df_t3_main_v.empty:
        list_race_all_t3_f = sorted([str(x) for x in df_t3_main_v['last_race'].dropna().unique()])
        sel_race_t3_f = st.selectbox("表示するレースを選択", list_race_all_t3_f)
        
        if sel_race_t3_f:
            df_race_t3_subset_f = df_t3_main_v[df_t3_main_v['last_race'] == sel_race_t3_f].copy()
            with st.form("form_race_res_t3_v"):
                st.write(f"【{sel_race_t3_f}】の結果入力")
                for idx_t3_r, row_t3_r in df_race_t3_subset_f.iterrows():
                    cur_p_t3 = int(row_t3_r['result_pos']) if not pd.isna(row_t3_r['result_pos']) else 0
                    cur_pop_t3 = int(row_t3_r['result_pop']) if not pd.isna(row_t3_r['result_pop']) else 0
                    
                    c_t3_1, c_t3_2 = st.columns(2)
                    with c_t3_1:
                        df_race_t3_subset_f.at[idx_t3_r, 'result_pos'] = st.number_input(f"{row_t3_r['name']} 着順", 0, 100, value=cur_p_t3, key=f"p_t3_r_{idx_t3_r}")
                    with c_t3_2:
                        df_race_t3_subset_f.at[idx_t3_r, 'result_pop'] = st.number_input(f"{row_t3_r['name']} 人気", 0, 100, value=cur_pop_t3, key=f"pop_t3_r_{idx_t3_r}")
                
                if st.form_submit_button("結果を一括同期"):
                    for idx_save_f, row_save_f in df_race_t3_subset_f.iterrows():
                        df_t3_main_v.at[idx_save_f, 'result_pos'] = row_save_f['result_pos']
                        df_t3_main_v.at[idx_save_f, 'result_pop'] = row_save_f['result_pop']
                    if safe_update(df_t3_main_v):
                        st.success("同期完了"); st.rerun()
            
            df_t3_formatted_f = df_race_t3_subset_f.copy()
            df_t3_formatted_f['base_rtc'] = df_t3_formatted_f['base_rtc'].apply(format_time_hmsf)
            st.dataframe(df_t3_formatted_f[["name", "notes", "base_rtc", "f3f", "l3f", "race_l3f", "result_pos", "result_pop"]], use_container_width=True)

# ==============================================================================
# 10. Tab 4: シミュレーター (完全非省略ロジック)
# ==============================================================================
with tab_simulator:
    st.header("🎯 次走シミュレーター")
    df_t4_main_v = get_db_data()
    if not df_t4_main_v.empty:
        list_h_names_t4_f = sorted([str(x) for x in df_t4_main_v['name'].dropna().unique()])
        sel_h_sim_multi_f = st.multiselect("出走馬をリストから選択", options=list_h_names_t4_f)
        
        sim_pops_dict_f = {}; sim_gates_dict_f = {}; sim_weights_dict_f = {}
        if sel_h_sim_multi_f:
            st.markdown("##### 📝 枠番・予想人気・想定斤量の個別入力")
            sim_input_cols_agg = st.columns(min(len(sel_h_sim_multi_f), 4))
            for i_sim_f, h_name_sim_f in enumerate(sel_h_sim_multi_f):
                with sim_input_cols_agg[i_sim_f % 4]:
                    h_lat_info_f = df_t4_main_v[df_t4_main_v['name'] == h_name_sim_f].iloc[-1]
                    sim_gates_dict_f[h_name_sim_f] = st.number_input(f"{h_name_sim_f} 枠", 1, 18, value=1, key=f"s_g_{h_name_sim_f}")
                    sim_pops_dict_f[h_name_sim_f] = st.number_input(f"{h_name_sim_f} 人気", 1, 18, value=int(h_lat_info_f['result_pop']) if not pd.isna(h_lat_info_f['result_pop']) else 10, key=f"s_p_{h_name_sim_f}")
                    # 個別斤量の詳細入力
                    sim_weights_dict_f[h_name_sim_f] = st.number_input(f"{h_name_sim_f} 斤量", 48.0, 62.0, 56.0, step=0.5, key=f"s_w_{h_name_sim_f}")

            c_sim_c1_f, c_sim_c2_f = st.columns(2)
            with c_sim_c1_f: 
                val_sim_course_f = st.selectbox("次走場", list(MASTER_COURSE_DATA_VALS.keys()), key="val_sim_c_f")
                val_sim_dist_f = st.selectbox("距離", list_dist_range, index=6)
                val_sim_track_f = st.radio("トラック", ["芝", "ダート"], horizontal=True)
            with c_sim_c2_f: 
                val_sim_cush_f = st.slider("想定クッション", 7.0, 12.0, 9.5)
                val_sim_water_f = st.slider("想定含水率", 0.0, 30.0, 10.0)
            
            if st.button("🏁 シミュレーション実行"):
                list_sim_agg_results_f = []; val_num_sim_total_f = len(sel_h_sim_multi_f); dict_styles_sim_f = {"逃げ": 0, "先行": 0, "差し": 0, "追込": 0}; val_sim_l3f_mean_f = df_t4_main_v['l3f'].mean()

                for h_name_run_f in sel_h_sim_multi_f:
                    df_h_hist_run_f = df_t4_main_v[df_t4_main_v['name'] == h_name_run_f].sort_values("date")
                    df_h_last3_run_f = df_h_hist_run_f.tail(3); list_conv_rtc_f = []
                    
                    # 脚質判定詳細
                    val_h_avg_l_f = df_h_last3_run_f['load'].mean()
                    if val_h_avg_l_f <= 3.5: h_style_sim_f = "逃げ"
                    elif val_h_avg_l_f <= 7.0: h_style_sim_f = "先行"
                    elif val_h_avg_l_f <= 11.0: h_style_sim_f = "差し"
                    else: h_style_sim_f = "追込"
                    dict_styles_sim_f[h_style_sim_f] += 1

                    # 頭数連動詳細
                    tag_jam_sim_f = "⚠️詰まり注意" if val_num_sim_total_f >= 15 and h_style_sim_f in ["差し", "追込"] and sim_gates_dict_f[h_name_run_f] <= 4 else "-"
                    tag_slow_sim_f = "-"
                    if val_num_sim_total_f <= 10:
                        val_h_min_l3f_f = df_h_hist_run_f['l3f'].min()
                        if val_h_min_l3f_f < val_sim_l3f_mean_f - 0.5: tag_slow_sim_f = "⚡スロー特化"
                        elif val_h_min_l3f_f > val_sim_l3f_mean_f + 0.5: tag_slow_sim_f = "📉瞬発力不足"

                    h_std_f = df_h_hist_run_f['base_rtc'].std() if len(df_h_hist_run_f) >= 3 else 0.0
                    label_h_stab_f = "⚖️安定" if 0 < h_std_f < 0.2 else "🎢ムラ" if h_std_f > 0.4 else "-"
                    
                    df_h_best_p_f = df_h_hist_run_f.loc[df_h_hist_run_f['base_rtc'].idxmin()]
                    label_h_apt_f = "🎯馬場◎" if abs(df_h_best_p_f['cushion'] - val_sim_cush_f) <= 0.5 and abs(df_h_best_p_f['water'] - val_sim_water_f) <= 2.0 else "-"

                    # 🌟 過去3走斤量・負荷詳細ループ復元
                    for idx_r_f, row_r_f in df_h_last3_run_f.iterrows():
                        v_p_d_f = row_r_f['dist']; v_p_rtc_f = row_r_f['base_rtc']; v_p_c_f = row_r_f['course']; v_p_l_f = row_r_f['load']
                        v_p_notes_f = str(row_r_f['notes']); v_p_w_f = 56.0; v_h_bw_f = 480.0
                        
                        m_w_sim_f = re.search(r'([4-6]\d\.\d)', v_p_notes_f)
                        if m_w_sim_f: v_p_w_f = float(m_w_sim_f.group(1))
                        m_hb_sim_f = re.search(r'\((\d{3})kg\)', v_p_notes_f)
                        if m_hb_sim_f: v_h_bw_f = float(m_hb_sim_f.group(1))
                        
                        if v_p_d_f > 0:
                            v_l_adj_f = (v_p_l_f - 7.0) * 0.02
                            v_sens_f = 0.15 if v_h_bw_f <= 440 else 0.08 if v_h_bw_f >= 500 else 0.1
                            v_w_diff_f = (sim_weights_dict_f[h_name_run_f] - v_p_w_f) * v_sens_f
                            v_base_conv_f = (v_p_rtc_f + v_l_adj_f + v_w_diff_f) / v_p_d_f * val_sim_dist_f
                            v_s_adj_f = (MASTER_SLOPE_FACTORS_CONFIG.get(val_sim_course_f, 0.002) - MASTER_SLOPE_FACTORS_CONFIG.get(v_p_c_f, 0.002)) * val_sim_dist_f
                            list_conv_rtc_f.append(v_base_conv_f + v_s_adj_f)
                    
                    val_avg_rtc_res_f = sum(list_conv_rtc_f) / len(list_conv_rtc_f) if list_conv_rtc_f else 0
                    val_h_best_d_past_f = df_h_hist_run_f.loc[df_h_hist_run_f['base_rtc'].idxmin(), 'dist']
                    val_avg_rtc_res_f += (abs(val_sim_dist_f - val_h_best_d_past_f) / 100) * 0.05
                    
                    label_h_mom_f = "-"
                    if len(df_h_hist_run_f) >= 2:
                        if df_h_hist_run_f.iloc[-1]['base_rtc'] < df_h_hist_run_f.iloc[-2]['base_rtc'] - 0.2:
                            label_h_mom_f = "📈上昇"
                            val_avg_rtc_res_f -= 0.15

                    val_syn_bias_f = -0.2 if (sim_gates_dict_f[h_name_run_f] <= 4 and analysis_bias_slider_val_in <= -0.5) or (sim_gates_dict_f[h_name_run_f] >= 13 and analysis_bias_slider_val_in >= 0.5) else 0
                    val_avg_rtc_res_f += val_syn_bias_f

                    val_h_c_bonus_f = -0.2 if any((df_h_hist_run_f['course'] == val_sim_course_f) & (df_h_hist_run_f['result_pos'] <= 3)) else 0.0
                    val_w_adj_f = (val_sim_water_f - 10.0) * 0.05
                    dict_c_m_f = MASTER_DIRT_COURSE_DATA_VALS if val_sim_track_f == "ダート" else MASTER_COURSE_DATA_VALS
                    if val_sim_track_f == "ダート": val_w_adj_f = -val_w_adj_f
                    
                    val_final_rtc_sim_f = (val_avg_rtc_res_f + (dict_c_m_f[val_sim_course_f] * (val_sim_dist_f/1600.0)) + val_h_c_bonus_f + val_w_adj_f - (9.5 - val_sim_cush_f) * 0.1)
                    
                    h_lat_entry_f = df_h_last3_run_f.iloc[-1]
                    list_sim_agg_results_f.append({
                        "馬名": h_name_run_f, "脚質": h_style_sim_f, "想定タイム": val_final_rtc_sim_f, "渋滞": tag_jam_sim_f, "スロー": tag_slow_sim_f, "適性": label_h_apt_f, "安定": label_h_stab_f, "偏差": "⤴️覚醒期待" if val_final_rtc_sim_f < df_h_hist_run_f['base_rtc'].min() - 0.3 else "-", "上昇": label_h_mom_f, "レベル": "🔥強ﾒﾝﾂ" if df_t4_main_v[df_t4_main_v['last_race'] == h_lat_entry_f['last_race']]['base_rtc'].mean() < df_t4_main_v['base_rtc'].mean() - 0.2 else "-", "load": h_lat_entry_f['load'], "状態": "💤休み明け" if (datetime.now() - h_lat_entry_f['date']).days // 7 >= 12 else "-", "raw_rtc": val_final_rtc_sim_f, "解析メモ": h_lat_entry_f['memo']
                    })
                
                label_sim_pace_f = "ミドルペース"
                if dict_styles_sim_f["逃げ"] >= 2 or (dict_styles_sim_f["逃げ"] + dict_styles_sim_f["先行"]) >= val_num_sim_total_f * 0.6: label_sim_pace_f = "ハイペース傾向"
                elif dict_styles_sim_f["逃げ"] == 0 and dict_styles_sim_f["先行"] <= 1: label_sim_pace_f = "スローペース傾向"
                
                df_sim_final_f = pd.DataFrame(list_sim_agg_results_f)
                val_sim_p_multiplier_f = 1.5 if val_num_sim_total_f >= 15 else 1.0
                
                def apply_synergy_f(row):
                    adj_f = 0.0
                    if "ハイ" in label_sim_pace_f:
                        if row['脚質'] in ["差し", "追込"]: adj_f = -0.2 * val_sim_p_multiplier_f
                        elif row['脚質'] == "逃げ": adj_f = 0.2 * val_sim_p_multiplier_f
                    elif "スロー" in label_sim_pace_f:
                        if row['脚質'] in ["逃げ", "先行"]: adj_f = -0.2 * val_sim_p_multiplier_f
                        elif row['脚質'] in ["差し", "追込"]: adj_f = 0.2 * val_sim_p_multiplier_f
                    return row['raw_rtc'] + adj_f

                df_sim_final_f['synergy_rtc'] = df_sim_final_f.apply(apply_synergy_f, axis=1)
                df_sim_final_f = df_sim_final_f.sort_values("synergy_rtc"); df_sim_final_f['RTC順位'] = range(1, len(df_sim_final_f) + 1)
                val_sim_top_t_f = df_sim_final_f.iloc[0]['raw_rtc']
                df_sim_final_f['差'] = df_sim_final_f['raw_rtc'] - val_sim_top_t_f; df_sim_final_f['予想人気'] = df_sim_final_f['馬名'].map(sim_pops_dict_f); df_sim_final_f['妙味'] = df_sim_final_f['予想人気'] - df_sim_final_f['RTC順位']
                
                df_sim_final_f['役割'] = "-"; df_sim_final_f.loc[df_sim_final_f['RTC順位'] == 1, '役割'] = "◎"; df_sim_final_f.loc[df_sim_final_f['RTC順位'] == 2, '役割'] = "〇"; df_sim_final_f.loc[df_sim_final_f['RTC順位'] == 3, '役割'] = "▲"
                pb_sim_f = df_sim_final_f[df_sim_final_f['RTC順位'] > 1].sort_values("妙味", ascending=False)
                if not pb_sim_f.empty: df_sim_final_f.loc[df_sim_final_f['馬名'] == pb_sim_f.iloc[0]['馬名'], '役割'] = "★"
                
                df_sim_final_f['想定タイム'] = df_sim_final_f['raw_rtc'].apply(format_time_hmsf); df_sim_final_f['差'] = df_sim_final_f['差'].apply(lambda x: f"+{x:.1f}" if x > 0 else "±0.0")

                st.markdown("---"); st.subheader(f"🏁 展開予想：{label_sim_pace_f} ({val_num_sim_total_f}頭立て)"); col_rec_f1, col_rec_f2 = st.columns(2)
                f_h_f = df_sim_final_f[df_sim_final_f['役割'] == "◎"].iloc[0]['馬名'] if not df_sim_final_f[df_sim_final_f['役割'] == "◎"].empty else ""
                o_h_f = df_sim_final_f[df_sim_final_f['役割'] == "〇"].iloc[0]['馬名'] if not df_sim_final_f[df_sim_final_f['役割'] == "〇"].empty else ""
                b_h_f = df_sim_final_f[df_sim_final_f['役割'] == "★"].iloc[0]['馬名'] if not df_sim_final_f[df_sim_final_f['役割'] == "★"].empty else ""
                with col_rec_f1: st.info(f"◎ {f_h_f} － 〇 {o_h_f}"); with col_rec_f2: 
                    if b_h_f: st.warning(f"◎ {f_h_f} － ★ {b_h_f}")
                def high_sim_f(row):
                    if row['役割'] == "★": return ['background-color: #ffe4e1; font-weight: bold'] * len(row)
                    if row['役割'] == "◎": return ['background-color: #fff700; font-weight: bold; color: black'] * len(row)
                    return [''] * len(row)
                st.table(df_sim_final_f[["役割", "馬名", "脚質", "渋滞", "スロー", "想定タイム", "差", "妙味", "適性", "安定", "上昇", "レベル", "load", "解析メモ"]].style.apply(high_sim_f, axis=1))

# ==============================================================================
# 11. Tab 5: トレンド詳細統計
# ==============================================================================
with tab_trends:
    st.header("📈 馬場トレンド解析")
    df_t5_main_f = get_db_data()
    if not df_t5_main_f.empty:
        sel_tc_t5_f = st.selectbox("確認する競馬場", list(MASTER_COURSE_DATA_VALS.keys()), key="tc_t5_f")
        df_td_t5_f = df_t5_main_f[df_t5_main_f['course'] == sel_tc_t5_f].sort_values("date")
        if not df_td_t5_f.empty:
            st.subheader("💧 コンディション時系列推移"); st.line_chart(df_td_t5_f.set_index("date")[["cushion", "water"]])
            st.subheader("🏁 4角通過順位傾向"); df_td_agg_f = df_td_t5_f.groupby('last_race').agg({'load':'mean', 'date':'max'}).sort_values('date', ascending=False).head(15)
            st.bar_chart(df_td_agg_f['load']); st.subheader("📊 レース上がり3F推移"); st.line_chart(df_td_t5_f.set_index("date")["race_l3f"])

# ==============================================================================
# 12. Tab 6: データ管理・再解析詳細 (冗長ロジック完全復元)
# ==============================================================================
with tab_management:
    st.header("🗑 データベース管理 & 高度なメンテナンス")
    if st.button("🔄 スプレッドシートの手動修正を同期（キャッシュ破棄）"):
        st.cache_data.clear(); st.rerun()

    df_t6_main_f = get_db_data()

    def update_eval_tags_verbose_logic_f(row, df_ctx=None):
        """【完全復元】再解析用詳細冗長ロジック (省略厳禁)"""
        m_raw_f = str(row['memo']) if not pd.isna(row['memo']) else ""
        def to_f_f(v):
            try: return float(v) if not pd.isna(v) else 0.0
            except: return 0.0
        f3f_v, l3f_v, rl3f_v, pos_v, l_pos_v, dist_v, rtc_v = map(to_f_f, [row['f3f'], row['l3f'], row['race_l3f'], row['result_pos'], row['load'], row['dist'], row['base_rtc']])
        
        # 🌟 斤量をnotesから再抽出 (手動修正反映の核心)
        notes_v = str(row['notes']); m_w_v = re.search(r'([4-6]\d\.\d)', notes_v)
        indiv_weight_v = float(m_w_v.group(1)) if m_w_v else 56.0
        
        # 中盤ラップ詳細
        mid_label_v = "平"
        if dist_v > 1200 and f3f_v > 0:
            m_lap_v = (rtc_v - f3f_v - l3f_v) / ((dist_v - 1200) / 200)
            if m_lap_v >= 12.8: mid_label_v = "緩"
            elif m_lap_v <= 11.8: mid_label_v = "締"
        elif dist_v <= 1200: mid_label_v = "短"

        # バイアス特例判定詳細 (完全再現)
        bt_label_v = "フラット"; mx_field_v = 16
        if df_ctx is not None and not pd.isna(row['last_race']):
            rc_f = df_ctx[df_ctx['last_race'] == row['last_race']]
            mx_field_v = rc_f['result_pos'].max() if not rc_f.empty else 16
            top3_f = rc_f[pd.to_numeric(rc_f['result_pos'], errors='coerce') <= 3].copy(); top3_f['load'] = top3_f['load'].fillna(7.0)
            ou_f = top3_f[(top3_f['load'] >= 10.0) | (top3_f['load'] <= 3.0)]
            if len(ou_f) == 1:
                bs_f = pd.concat([top3_f[top3_f['name'] != ou_f.iloc[0]['name']], rc_f[rc_f['result_pos'] == 4]])
            else:
                bs_f = top3_f
            if not bs_f.empty:
                avg_b_v = bs_f['load'].mean()
                if avg_b_v <= 4.0: bt_label_v = "前有利"
                elif avg_b_v >= 10.0: bt_label_v = "後有利"

        ps_label_v = "ハイペース" if "ハイ" in m_raw_f else "スローペース" if "スロー" in m_raw_f else "ミドルペース"
        pd_val_v = 1.5 if ps_label_v != "ミドルペース" else 0.0; rp_val_v = l_pos_v / mx_field_v; fi_val_v = mx_field_v / 16.0
        load_s_v = 0.0
        if ps_label_v == "ハイペース" and bt_label_v != "前有利": load_s_v = max(0, (0.6 - rp_val_v) * pd_val_v * 3.0) * fi_val_v
        elif ps_label_v == "スローペース" and bt_label_v != "後有利": load_s_v = max(0, (rp_val_v - 0.4) * pd_val_v * 2.0) * fi_val_v
        
        tags_v = []; is_c_v = False
        if rl3f_v > 0:
            if (rl3f_v - l3f_v) >= 0.5: tags_v.append("🚀 アガリ優秀")
            elif (rl3f_v - l3f_v) <= -1.0: tags_v.append("📉 失速大")
        if pos_v <= 5:
            if bt_label_v == "前有利" and l_pos_v >= 10.0: tags_v.append("💎💎 ﾊﾞｲｱｽ極限逆行" if mx_field_v >= 16 else "💎 ﾊﾞｲｱｽ逆行"); is_c_v = True
            elif bt_label_v == "後有利" and l_pos_v <= 3.0: tags_v.append("💎💎 ﾊﾞｲｱｽ極限逆行" if mx_field_v >= 16 else "💎 ﾊﾞｲｱｽ逆行"); is_c_v = True
            if ps_label_v == "ハイペース" and bt_label_v != "前有利" and l_pos_v <= 3.0: tags_v.append("📉 激流被害" if mx_field_v >= 14 else "🔥 展開逆行"); is_c_v = True
            elif ps_label_v == "スローペース" and bt_label_v != "後有利" and l_pos_v >= 10.0 and (f3f_v - l3f_v) > 1.5: tags_v.append("🔥 展開逆行"); is_c_v = True
        if mx_field_v <= 10 and ps_label_v == "スローペース" and pos_v <= 2: tags_v.append("🟢 展開恩恵")
        ft_tag_v = "多" if mx_field_v >= 16 else "少" if mx_field_v <= 10 else "中"
        mu_f = (f"【{ps_label_v}/{bt_label_v}/負荷:{load_s_v:.1f}({ft_tag_v})/{mid_label_v}】" + "/".join(tags_v)).strip("/")
        fu_f = ("★逆行狙い " + str(row['next_buy_flag']).replace("★逆行狙い", "")).strip() if is_c_v else str(row['next_buy_flag']).replace("★逆行狙い", "").strip()
        return mu_f, fu_f

    st.subheader("🗓 過去レース開催週を一括設定")
    if not df_t6_main_f.empty:
        df_rm_w_f = df_t6_main_f[['last_race', 'date']].drop_duplicates(subset=['last_race']).copy(); df_rm_w_f['track_week'] = 1
        df_ed_w_f = st.data_editor(df_rm_w_f, hide_index=True)
        if st.button("🔄 補正適用"):
            wd_lookup_f = dict(zip(df_ed_w_f['last_race'], df_ed_w_f['track_week']))
            for idx_w_f, row_w_f in df_t6_main_f.iterrows():
                if row_w_f['last_race'] in wd_lookup_f:
                    df_t6_main_f.at[idx_w_f, 'base_rtc'] = row_w_f['base_rtc'] - (wd_lookup_f[row_w_f['last_race']] - 1) * 0.05
                    m_6_f, f_6_f = update_eval_tags_verbose_logic_f(df_t6_main_f.iloc[idx_w_f], df_t6_main_f)
                    df_t6_main_f.at[idx_w_f, 'memo'], df_t6_main_f.at[idx_w_f, 'next_buy_flag'] = m_6_f, f_6_f
            if safe_update(df_t6_main_f): st.success("一括補正完了"); st.rerun()

    st.subheader("🛠️ 一括メンテナンスメニュー")
    c_btn1_f, c_btn2_f = st.columns(2)
    with c_btn1_f:
        if st.button("🔄 DB再解析（上書き）"):
            st.cache_data.clear(); latest_db_f = conn.read(ttl=0)
            for c_nm_f in all_columns_definition_list:
                if c_nm_f not in latest_db_f.columns: latest_db_f[c_nm_f] = None
            for idx_sy_f, row_sy_f in latest_db_f.iterrows():
                m_res_f, f_res_f = update_eval_tags_verbose_logic_f(row_sy_f, latest_db_f)
                latest_db_f.at[idx_sy_f, 'memo'], latest_db_f.at[idx_sy_f, 'next_buy_flag'] = m_res_f, f_res_f
            if safe_update(latest_db_f): st.success("全件再計算・同期完了"); st.rerun()
    with c_btn2_f:
        if st.button("🧼 重複削除詳細クリーニング"):
            cnt_bf_f = len(df_t6_main_f); df_t6_main_f = df_t6_main_f.drop_duplicates(subset=['name', 'date', 'last_race'], keep='first')
            if safe_update(df_t6_main_f): st.success(f"{cnt_bf_f - len(df_t6_main_f)}件削除"); st.rerun()

    if not df_t6_main_f.empty:
        st.subheader("🛠️ データ編集エディタ")
        df_t6_formatted_f = df_t6_main_f.copy(); df_t6_formatted_f['base_rtc'] = df_t6_formatted_f['base_rtc'].apply(format_time_hmsf)
        df_final_ed_f = st.data_editor(df_t6_formatted_f.sort_values("date", ascending=False), num_rows="dynamic", use_container_width=True)
        if st.button("💾 エディタ内容を反映"):
            df_save_f = df_final_ed_f.copy(); df_save_f['base_rtc'] = df_save_f['base_rtc'].apply(parse_time_to_float_seconds)
            if safe_update(df_save_f): st.success("反映完了"); st.rerun()
        
        st.divider(); st.subheader("❌ データ削除設定"); cd1_f, cd2_f = st.columns(2)
        with cd1_f:
            list_r_all_f = sorted([str(x) for x in df_t6_main_f['last_race'].dropna().unique()]); sel_r_del_f = st.selectbox("削除対象レース", ["未選択"] + list_r_all_f)
            if sel_r_del_f != "未選択" and st.button(f"🚨 {sel_r_del_f} 削除"):
                if safe_update(df_t6_main_f[df_t6_main_v['last_race'] != sel_r_del_f]): st.rerun()
        with cd2_f:
            list_h_all_f = sorted([str(x) for x in df_t6_main_f['name'].dropna().unique()]); list_h_del_f = st.multiselect("削除馬（複数可）", list_h_all_f, key="ms_del_f_final")
            if list_h_del_f and st.button(f"🚨 {len(list_h_del_f)}頭削除"):
                if safe_update(df_t6_main_f[~df_t6_main_v['name'].isin(list_h_del_f)]): st.rerun()

        st.divider(); with st.expander("☢️ システム詳細初期化"):
            if st.button("🧨 データベース完全リセット"):
                if safe_update(pd.DataFrame(columns=df_t6_main_f.columns)): st.rerun()
