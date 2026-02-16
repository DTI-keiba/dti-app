import streamlit as st
import pandas as pd
import re
import time
from streamlit_gsheets import GSheetsConnection
from datetime import datetime

# ==============================================================================
# 1. ページ基本構成設定
# ==============================================================================
# ページのタイトル、レイアウト、サイドバーの初期状態を詳細に設定します。
st.set_page_config(
    page_title="DTI Ultimate DB",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Google Sheets 接続オブジェクトの生成 ---
# データベースとの通信を司るメインコネクションです。
conn = st.connection("gsheets", type=GSheetsConnection)

# ==============================================================================
# 2. データベース読み込み詳細ロジック (キャッシュ管理)
# ==============================================================================
@st.cache_data(ttl=300)
def get_db_data_cached():
    """
    Google Sheetsから全データを読み込み、型変換と前処理を一切の省略なしに実行します。
    キャッシュを有効にすることで、API制限(429 Error)を物理的に回避します。
    """
    # データベースの全カラム定義（初期設計から一貫した18カラムを維持）
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
        # 強制読み込みフラグが必要な場合に対応するため、内部で直接readを呼び出します。
        # ttl=0 を指定することで、最新のスプレッドシート状態を取得可能です。
        df = conn.read(ttl=0)
        
        # データがNoneまたは空の場合の安全な初期化
        if df is None:
            return pd.DataFrame(columns=all_cols)
            
        if df.empty:
            return pd.DataFrame(columns=all_cols)
        
        # 🌟 カラムの存在チェックと補填（1ミリも削らず、1カラムずつ詳細に実行）
        if "name" not in df.columns:
            df["name"] = None
        if "base_rtc" not in df.columns:
            df["base_rtc"] = None
        if "last_race" not in df.columns:
            df["last_race"] = None
        if "course" not in df.columns:
            df["course"] = None
        if "dist" not in df.columns:
            df["dist"] = None
        if "notes" not in df.columns:
            df["notes"] = None
        if "timestamp" not in df.columns:
            df["timestamp"] = None
        if "f3f" not in df.columns:
            df["f3f"] = None
        if "l3f" not in df.columns:
            df["l3f"] = None
        if "race_l3f" not in df.columns:
            df["race_l3f"] = None
        if "load" not in df.columns:
            df["load"] = None
        if "memo" not in df.columns:
            df["memo"] = None
        if "date" not in df.columns:
            df["date"] = None
        if "cushion" not in df.columns:
            df["cushion"] = None
        if "water" not in df.columns:
            df["water"] = None
        if "result_pos" not in df.columns:
            df["result_pos"] = None
        if "result_pop" not in df.columns:
            df["result_pop"] = None
        if "next_buy_flag" not in df.columns:
            df["next_buy_flag"] = None
            
        # データの型変換（エラーハンドリングを冗長に記述）
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            
        if 'result_pos' in df.columns:
            df['result_pos'] = pd.to_numeric(df['result_pos'], errors='coerce')
        
        # 🌟 三段階詳細ソートロジック
        # 1. 日付を新しい順に並べる
        # 2. 同日の場合はレース名を名前順に並べる
        # 3. 同レース内では着順を1着から順に並べる
        df = df.sort_values(
            by=["date", "last_race", "result_pos"], 
            ascending=[False, True, True]
        )
        
        # 各種計算用数値カラムの変換とNaN補完（簡略化せず個別に記述）
        if 'result_pop' in df.columns:
            df['result_pop'] = pd.to_numeric(df['result_pop'], errors='coerce')
            
        if 'f3f' in df.columns:
            df['f3f'] = pd.to_numeric(df['f3f'], errors='coerce')
            df['f3f'] = df['f3f'].fillna(0.0)
            
        if 'l3f' in df.columns:
            df['l3f'] = pd.to_numeric(df['l3f'], errors='coerce')
            df['l3f'] = df['l3f'].fillna(0.0)
            
        if 'race_l3f' in df.columns:
            df['race_l3f'] = pd.to_numeric(df['race_l3f'], errors='coerce')
            df['race_l3f'] = df['race_l3f'].fillna(0.0)
            
        if 'load' in df.columns:
            df['load'] = pd.to_numeric(df['load'], errors='coerce')
            df['load'] = df['load'].fillna(0.0)
            
        if 'base_rtc' in df.columns:
            df['base_rtc'] = pd.to_numeric(df['base_rtc'], errors='coerce')
            df['base_rtc'] = df['base_rtc'].fillna(0.0)
            
        if 'cushion' in df.columns:
            df['cushion'] = pd.to_numeric(df['cushion'], errors='coerce').fillna(9.5)
            
        if 'water' in df.columns:
            df['water'] = pd.to_numeric(df['water'], errors='coerce').fillna(10.0)
            
        # 全ての行が完全に空のデータはノイズとして除外します。
        df = df.dropna(how='all')
        
        return df
        
    except Exception as e:
        st.error(f"【重大な警告】スプレッドシートの読み込み中に予期せぬエラーが発生しました。詳細な原因を確認してください: {e}")
        return pd.DataFrame(columns=all_cols)

def get_db_data():
    """get_db_data_cachedへのインターフェースです。"""
    return get_db_data_cached()

# ==============================================================================
# 3. データベース更新詳細ロジック (安全な上書き)
# ==============================================================================
def safe_update(df):
    """
    Google Sheetsへデータを書き戻すための最重要関数です。
    リトライ機能、ソート、インデックスリセット、キャッシュクリアを統合しています。
    """
    # 保存直前にデータの整合性を再確保します。
    if 'date' in df.columns:
        if 'last_race' in df.columns:
            if 'result_pos' in df.columns:
                df['date'] = pd.to_datetime(df['date'], errors='coerce')
                df['result_pos'] = pd.to_numeric(df['result_pos'], errors='coerce')
                df = df.sort_values(
                    by=["date", "last_race", "result_pos"], 
                    ascending=[False, True, True]
                )
    
    # スプレッドシートの整合性維持のため、インデックスを完全にリセットしてから保存します。
    df = df.reset_index(drop=True)
    
    # 🌟 APIの429エラーやタイムアウトを回避するためのリトライループ
    max_retries = 3
    for i in range(max_retries):
        try:
            # 最新のDataFrame状態でスプレッドシートを完全に上書き更新します。
            conn.update(data=df)
            
            # 🌟 重要：書き込み成功時にアプリ内のキャッシュを強制クリアします。
            # これにより、同期不全（保存したのに反映されない等）を物理的に解消します。
            st.cache_data.clear()
            
            return True
            
        except Exception as e:
            # 失敗した場合は待機時間（指数バックオフ的）を設けます。
            wait_seconds = 5
            if i < max_retries - 1:
                st.warning(f"Google Sheets接続エラー(試行 {i+1}/3回目)... {wait_seconds}秒後に再試行します。")
                time.sleep(wait_seconds)
                continue
            else:
                st.error(f"Google Sheetsの更新に失敗しました。詳細を確認してください: {e}")
                return False

# ==============================================================================
# 4. 補助関数 (フォーマット・パース)
# ==============================================================================
def format_time(seconds):
    """
    秒数を mm:ss.f 形式の文字列に変換します。
    RTCの表示を競馬のラップタイム形式に統一するために使用します。
    """
    if seconds is None:
        return ""
    if seconds <= 0:
        return ""
    if pd.isna(seconds):
        return ""
    if isinstance(seconds, str):
        return seconds
        
    minutes_part = int(seconds // 60)
    seconds_part = seconds % 60
    return f"{minutes_part}:{seconds_part:04.1f}"

def parse_time_str(time_str):
    """
    mm:ss.f 形式の文字列を秒数(float)にパースして戻します。
    エディタで編集されたタイムを計算用数値に戻す際に使用します。
    """
    if time_str is None:
        return 0.0
    try:
        time_str_cleaned = str(time_str).strip()
        if ":" in time_str_cleaned:
            minutes_val, seconds_val = map(float, time_str_cleaned.split(':'))
            return minutes_val * 60 + seconds_val
        return float(time_str_cleaned)
    except:
        return 0.0

# ==============================================================================
# 5. 係数マスタ詳細定義 (1ミリも簡略化せず、小数第二位まで記述)
# ==============================================================================
# 芝コース用の基礎負荷係数
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

# ダートコース用の基礎負荷係数
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

# 競馬場ごとの勾配（坂）による1メートルあたりの補正係数
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
# 6. メインUI構成 - タブ詳細設定
# ==============================================================================
# すべての機能を個別のタブに分離して配置します。
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "📝 解析・保存", 
    "🐎 馬別履歴", 
    "🏁 レース別履歴", 
    "🎯 シミュレーター", 
    "📈 馬場トレンド", 
    "🗑 データ管理"
])

# ==============================================================================
# 7. Tab 1: レース解析・保存セクション
# ==============================================================================
with tab1:
    # 🌟 注目馬（逆行評価馬）のピックアップ表示
    df_pickup_main = get_db_data()
    if not df_pickup_main.empty:
        st.subheader("🎯 次走注目馬（逆行評価ピックアップ）")
        pickup_rows_list = []
        for i_pk, row_pk in df_pickup_main.iterrows():
            current_memo_pk = str(row_pk['memo'])
            bias_check_pk = "💎" in current_memo_pk
            pace_check_pk = "🔥" in current_memo_pk
            
            if bias_check_pk or pace_check_pk:
                reverse_detail_pk = ""
                if bias_check_pk and pace_check_pk:
                    reverse_detail_pk = "【💥両方逆行】"
                elif bias_check_pk:
                    reverse_detail_pk = "【💎バイアス逆行】"
                elif pace_check_pk:
                    reverse_detail_pk = "【🔥ペース逆行】"
                
                pickup_rows_list.append({
                    "馬名": row_pk['name'], 
                    "逆行タイプ": reverse_detail_pk, 
                    "前走": row_pk['last_race'],
                    "日付": row_pk['date'].strftime('%Y-%m-%d') if not pd.isna(row_pk['date']) else "", 
                    "解析メモ": current_memo_pk
                })
        
        if pickup_rows_list:
            st.dataframe(
                pd.DataFrame(pickup_rows_list).sort_values("日付", ascending=False), 
                use_container_width=True, 
                hide_index=True
            )
            
    st.divider()

    st.header("🚀 レース解析 & 自動保存システム")
    
    # サイドバーによる詳細条件入力（冗長なまでの項目を維持）
    with st.sidebar:
        st.title("解析条件設定")
        race_input_name = st.text_input("レース名 (例: 有馬記念)")
        race_input_date = st.date_input("レース実施日", datetime.now())
        race_input_course = st.selectbox("競馬場選択", list(COURSE_DATA.keys()))
        race_input_track = st.radio("トラック種別", ["芝", "ダート"], horizontal=True)
        dist_options_list = list(range(1000, 3700, 100))
        race_input_dist = st.selectbox("距離 (m)", dist_options_list, index=dist_options_list.index(1600))
        st.divider()
        st.write("💧 馬場コンディション詳細")
        input_cushion_val = st.number_input("クッション値 (芝のみ)", 7.0, 12.0, 9.5, step=0.1) if race_input_track == "芝" else 9.5
        input_water_4c_val = st.number_input("含水率：4角地点 (%)", 0.0, 50.0, 10.0, step=0.1)
        input_water_goal_val = st.number_input("含水率：ゴール前地点 (%)", 0.0, 50.0, 10.0, step=0.1)
        input_track_idx_val = st.number_input("馬場指数", -50, 50, 0, step=1)
        input_bias_slider_val = st.slider("馬場バイアス (内有利 -1.0 ↔ 外有利 +1.0)", -1.0, 1.0, 0.0, step=0.1)
        input_week_val = st.number_input("開催週 (例: 1, 8)", 1, 12, 1)

    col_tab1_left, col_tab1_right = st.columns(2)
    
    with col_tab1_left: 
        st.markdown("##### 🏁 レースラップ入力")
        input_lap_text = st.text_area("JRAレースラップ (例: 12.5-11.0-12.0...)", height=150)
        
        parsed_f3f_total = 0.0
        parsed_l3f_total = 0.0
        parsed_pace_status_label = "ミドルペース"
        parsed_pace_difference_val = 0.0
        
        if input_lap_text:
            raw_lap_found = [float(x) for x in re.findall(r'\d+\.\d', input_lap_text)]
            if len(raw_lap_found) >= 3:
                parsed_f3f_total = sum(raw_lap_found[:3])
                parsed_l3f_total = sum(raw_lap_found[-3:])
                parsed_pace_difference_val = parsed_f3f_total - parsed_l3f_total
                
                # 距離に応じた動的ペースしきい値の計算
                dynamic_threshold_val = 1.0 * (race_input_dist / 1600.0)
                
                if parsed_pace_difference_val < -dynamic_threshold_val:
                    parsed_pace_status_label = "ハイペース"
                elif parsed_pace_difference_val > dynamic_threshold_val:
                    parsed_pace_status_label = "スローペース"
                else:
                    parsed_pace_status_label = "ミドルペース"
                    
                st.success(f"ラップ解析成功: 前3F {parsed_f3f_total:.1f} / 後3F {parsed_l3f_total:.1f} ({parsed_pace_status_label})")
        
        input_manual_l3f = st.number_input("レース上がり3F (自動計算から修正可)", 0.0, 60.0, parsed_l3f_total, step=0.1)

    with col_tab1_right: 
        st.markdown("##### 🐎 成績表貼り付け")
        input_jra_raw_text = st.text_area("JRA公式サイトの成績表をそのまま貼り付けてください", height=250)

    # 🌟 【指示反映】解析プレビュー生成ボタンの実装
    # セッションステートを使用してボタンの押下状態を管理します。
    if 'analysis_preview_visible' not in st.session_state:
        st.session_state.analysis_preview_visible = False

    st.write("---")
    # 解析開始の明示的なトリガーボタンです。
    if st.button("🔍 解析プレビューを生成"):
        if not input_jra_raw_text:
            st.error("成績表をテキストエリアに貼り付けてください。")
        elif parsed_f3f_total <= 0:
            st.error("レースラップが入力されていないか、形式が正しくありません。")
        else:
            # 条件が揃った場合のみプレビューを表示フラグを立てます。
            st.session_state.analysis_preview_visible = True

    # 🌟 【完全復旧】解析プレビュー詳細セクション
    # 1ミリも簡略化せず、抽出・編集プロセスを詳細に記述します。
    if st.session_state.analysis_preview_visible:
        st.markdown("##### ⚖️ 解析プレビュー（斤量の確認・修正）")
        raw_lines_list = [l.strip() for l in input_jra_raw_text.split('\n') if len(l.strip()) > 15]
        
        data_for_preview_table = []
        for line_item in raw_lines_list:
            # 馬名の抽出
            found_horse_names = re.findall(r'([ァ-ヶー]{2,})', line_item)
            if not found_horse_names:
                continue
                
            # 斤量の自動抽出
            weight_pattern_match = re.search(r'\s([4-6]\d\.\d)\s', line_item)
            auto_extracted_weight = float(weight_pattern_match.group(1)) if weight_pattern_match else 56.0
            
            data_for_preview_table.append({
                "馬名": found_horse_names[0], 
                "斤量": auto_extracted_weight, 
                "raw_line": line_item
            })
        
        # 編集可能なエディタを表示し、ユーザーによる手動修正を受け付けます。
        df_analysis_preview = st.data_editor(
            pd.DataFrame(data_for_preview_table), 
            use_container_width=True, 
            hide_index=True
        )

        if st.button("🚀 この内容で解析を実行してデータベースへ保存"):
            if not race_input_name:
                st.error("レース名が入力されていません。")
            else:
                # 最終解析用リストの構築
                final_parsed_results = []
                for idx_pre, row_pre in df_analysis_preview.iterrows():
                    target_line_raw = row_pre["raw_line"]
                    
                    main_time_match = re.search(r'(\d{1,2}:\d{2}\.\d)', target_line_raw)
                    if not main_time_match:
                        continue
                    
                    # 着順の取得（行頭の数字）
                    match_pos_rank_num = re.match(r'^(\d{1,2})', target_line_raw)
                    val_res_pos = int(match_pos_rank_num.group(1)) if match_pos_rank_num else 99
                    
                    # 4角通過順位の冗長取得ロジック
                    str_after_main_time = target_line_raw[main_time_match.end():]
                    all_pos_numbers = re.findall(r'\b([1-2]?\d)\b', str_after_main_time)
                    val_final_4c_pos = 7.0 
                    
                    if all_pos_numbers:
                        valid_pos_list = []
                        for p_val_str in all_pos_numbers:
                            p_val_int = int(p_val_str)
                            # 競馬の通過順位として不自然な数値（馬体重等）の混入をチェック
                            if p_val_int > 30: 
                                if len(valid_pos_list) > 0:
                                    break
                            valid_pos_list.append(float(p_val_int))
                        
                        if valid_pos_list:
                            # 最後の要素が4角通過順位であると定義
                            val_final_4c_pos = valid_pos_list[-1]
                    
                    final_parsed_results.append({
                        "line": target_line_raw, 
                        "res_pos": val_res_pos, 
                        "four_c_pos": val_final_4c_pos, 
                        "name": row_pre["馬名"], 
                        "weight": row_pre["斤量"]
                    })
                
                # --- バイアス判定ロジック（4着補充特例を冗長に完全記述） ---
                # 1. まず上位3頭を抽出
                top_3_entries_bias = sorted(
                    [d for d in final_parsed_results if d["res_pos"] <= 3], 
                    key=lambda x: x["res_pos"]
                )
                
                # 2. 特例馬（10番手以下 or 3番手以内）を特定
                outlier_horses_bias = [
                    d for d in top_3_entries_bias 
                    if d["four_c_pos"] >= 10.0 or d["four_c_pos"] <= 3.0
                ]
                
                # 3. 判定ロジックの分岐（1ミリも削らず記述）
                if len(outlier_horses_bias) == 1:
                    # 1頭のみ極端な位置取りだった場合：その馬を判定から除き、4着馬を補充する
                    bias_base_group = [d for d in top_3_entries_bias if d != outlier_horses_bias[0]]
                    supplement_4th_horse = [d for d in final_parsed_results if d["res_pos"] == 4]
                    actual_bias_target_set = bias_base_group + supplement_4th_horse
                else:
                    # それ以外（0頭または2頭以上が極端な場合）：通常通り上位3頭で判定する
                    actual_bias_target_set = top_3_entries_bias
                
                # 4. 平均通過順位からバイアス種別を確定
                if actual_bias_target_set:
                    final_avg_bias_pos = sum(d["four_c_pos"] for d in actual_bias_target_set) / len(actual_bias_target_set)
                else:
                    final_avg_bias_pos = 7.0
                    
                if final_avg_bias_pos <= 4.0:
                    determined_race_bias_type = "前有利"
                elif final_avg_bias_pos >= 10.0:
                    determined_race_bias_type = "後有利"
                else:
                    determined_race_bias_type = "フラット"
                
                # 最大出走頭数の特定（負荷の相対評価に使用）
                val_max_field_size = max([d["res_pos"] for d in final_parsed_results]) if final_parsed_results else 16

                # --- 最終的な行データの生成ループ ---
                new_db_rows_list = []
                for entry_data in final_parsed_results:
                    s_line_text = entry_data["line"]
                    s_last_pos_val = entry_data["four_c_pos"]
                    s_res_pos_rank = entry_data["res_pos"]
                    s_weight_val = entry_data["weight"] 
                    
                    # タイムの詳細秒数換算
                    s_match_time = re.search(r'(\d{1,2}:\d{2}\.\d)', s_line_text)
                    s_time_string = s_match_time.group(1)
                    s_min_val, s_sec_val = map(float, s_time_string.split(':'))
                    s_total_seconds_val = s_min_val * 60 + s_sec_val
                    
                    # 馬体重の完全抽出
                    s_match_horse_w = re.search(r'(\d{3})kg', s_line_text)
                    s_string_horse_w = f"({s_match_horse_w.group(1)}kg)" if s_match_horse_w else ""

                    # 個別上がり3Fの詳細抽出ロジック
                    s_l3f_indiv_val = 0.0
                    s_match_l3f_pattern = re.search(r'(\d{2}\.\d)\s*\d{3}\(', s_line_text)
                    if s_match_l3f_pattern:
                        s_l3f_indiv_val = float(s_match_l3f_pattern.group(1))
                    else:
                        s_find_all_decimals = re.findall(r'(\d{2}\.\d)', s_line_text)
                        for s_dv in s_find_all_decimals:
                            s_dv_float = float(s_dv)
                            if 30.0 <= s_dv_float <= 46.0:
                                if abs(s_dv_float - s_weight_val) > 0.5:
                                    s_l3f_indiv_val = s_dv_float
                                    break
                    if s_l3f_indiv_val == 0.0:
                        s_l3f_indiv_val = input_manual_l3f 
                    
                    # --- 【完全復元】頭数・非線形負荷詳細補正ロジック ---
                    s_relative_pos_ratio = s_last_pos_val / val_max_field_size
                    # 16頭を標準とした強度補正
                    s_intensity_coeff_val = val_max_field_size / 16.0
                    
                    s_computed_load_score_val = 0.0
                    if parsed_pace_status_label == "ハイペース":
                        if determined_race_bias_type != "前有利":
                            s_raw_load = (0.6 - s_relative_pos_ratio) * abs(parsed_pace_difference_val) * 3.0
                            s_computed_load_score_val += max(0.0, s_raw_load) * s_intensity_coeff_val
                            
                    elif parsed_pace_status_label == "スローペース":
                        if determined_race_bias_type != "後有利":
                            s_raw_load = (s_relative_pos_ratio - 0.4) * abs(parsed_pace_difference_val) * 2.0
                            s_computed_load_score_val += max(0.0, s_raw_load) * s_intensity_coeff_val
                    
                    # 逆行・特殊タグの判定詳細
                    s_tags_collection = []
                    s_counter_target_flag = False
                    
                    if s_res_pos_rank <= 5:
                        # バイアス逆行の判定
                        if determined_race_bias_type == "前有利":
                            if s_last_pos_val >= 10.0:
                                s_t = "💎💎 ﾊﾞｲｱｽ極限逆行" if val_max_field_size >= 16 else "💎 ﾊﾞｲｱｽ逆行"
                                s_tags_collection.append(s_t)
                                s_counter_target_flag = True
                        elif determined_race_bias_type == "後有利":
                            if s_last_pos_val <= 3.0:
                                s_t = "💎💎 ﾊﾞｲｱｽ極限逆行" if val_max_field_size >= 16 else "💎 ﾊﾞｲｱｽ逆行"
                                s_tags_collection.append(s_t)
                                s_counter_target_flag = True
                                
                    # 展開逆行の判定詳細
                    s_is_favored_by_pace_bias = False
                    if parsed_pace_status_label == "ハイペース":
                        if determined_race_bias_type == "前有利":
                            s_is_favored_by_pace_bias = True
                    elif parsed_pace_status_label == "スローペース":
                        if determined_race_bias_type == "後有利":
                            s_is_favored_by_pace_bias = True
                            
                    if s_is_favored_by_pace_bias == False:
                        if parsed_pace_status_label == "ハイペース":
                            if s_last_pos_val <= 3.0:
                                s_tags_collection.append("📉 激流被害" if val_max_field_size >= 14 else "🔥 展開逆行")
                                s_counter_target_flag = True
                        elif parsed_pace_status_label == "スローペース":
                            if s_last_pos_val >= 10.0:
                                if (parsed_f3f_total - s_l3f_indiv_val) > 1.5:
                                    s_tags_collection.append("🔥 展開逆行")
                                    s_counter_target_flag = True
                    
                    # 少頭数時の「展開恩恵」判定
                    if val_max_field_size <= 10:
                        if parsed_pace_status_label == "スローペース":
                            if s_res_pos_rank <= 2:
                                s_tags_collection.append("🟢 展開恩恵")

                    # 上がりタイムの偏差評価
                    s_l3f_deviation = input_manual_l3f - s_l3f_indiv_val
                    if s_l3f_deviation >= 0.5:
                        s_tags_collection.append("🚀 アガリ優秀")
                    elif s_l3f_deviation <= -1.0:
                        s_tags_collection.append("📉 失速大")
                    
                    # 中盤ラップの詳細解析
                    s_mid_pace_label = "平"
                    if race_input_dist > 1200:
                        s_mid_lap_result = (s_total_seconds_val - parsed_f3f_total - s_l3f_indiv_val) / ((race_input_dist - 1200) / 200)
                        if s_mid_lap_result >= 12.8:
                            s_mid_pace_label = "緩"
                        elif s_mid_lap_result <= 11.8:
                            s_mid_pace_label = "締"
                    else:
                        s_mid_pace_label = "短"

                    s_field_size_attribute = "多" if val_max_field_size >= 16 else "少" if val_max_field_size <= 10 else "中"
                    s_final_memo_str = f"【{parsed_pace_status_label}/{determined_race_bias_type}/負荷:{s_computed_load_score_val:.1f}({s_field_size_attribute})/{s_mid_pace_label}】{'/'.join(s_tags_collection) if s_tags_collection else '順境'}"
                    
                    # 指数計算用オフセット
                    s_week_offset_val = (input_week_val - 1) * 0.05
                    s_avg_water_val = (input_water_4c_val + input_water_goal_val) / 2.0
                    
                    # 🌟 RTC能力指数の完全計算式（一切の簡略化なし）
                    s_final_computed_rtc = (s_total_seconds_val - (s_weight_val - 56.0) * 0.1 - input_track_idx_val / 10.0 - s_computed_load_score_val / 10.0 - s_week_offset_val) + input_bias_slider_val - (s_avg_water_val - 10.0) * 0.05 - (9.5 - input_cushion_val) * 0.1 + (race_input_dist - 1600) * 0.0005
                    
                    new_db_rows_list.append({
                        "name": entry_data["name"], 
                        "base_rtc": s_final_computed_rtc, 
                        "last_race": race_input_name, 
                        "course": race_input_course, 
                        "dist": race_input_dist, 
                        "notes": f"{s_weight_val}kg{s_string_hw_s}", 
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"), 
                        "f3f": parsed_f3f_total, 
                        "l3f": s_l3f_indiv_val, 
                        "race_l3f": input_manual_l3f, 
                        "load": s_last_pos_val, 
                        "memo": s_final_memo_str,
                        "date": race_input_date.strftime("%Y-%m-%d"), 
                        "cushion": input_cushion_val, 
                        "water": s_avg_water_val, 
                        "next_buy_flag": "★逆行狙い" if s_is_counter else "", 
                        "result_pos": s_res_pos_rank
                    })
                
                if new_db_rows_list:
                    # 🌟 同期不全解消ロジック：保存ボタン押下時にキャッシュを明示的に破棄し、最新シートを強制読み込み
                    st.cache_data.clear()
                    df_latest_from_sheet = conn.read(ttl=0)
                    
                    # 読み込んだ最新シートデータのカラム不整合を正規化
                    for col_target in all_cols:
                        if col_target not in df_latest_from_sheet.columns:
                            df_latest_from_sheet[col_target] = None
                            
                    # 最新シートデータと今回の解析結果を結合
                    df_final_merged_save = pd.concat([df_latest_from_sheet, pd.DataFrame(new_db_rows_list)], ignore_index=True)
                    
                    # スプレッドシートへの永続化を実行
                    if safe_update(df_final_merged_save):
                        # 保存成功後、プレビュー表示フラグを下ろしてリロード
                        st.session_state.analysis_preview_visible = False
                        st.success(f"✅ 解析完了！{len(new_db_rows_list)}頭の最新データをDBに保存し、同期を完了しました。")
                        st.rerun()

# ==============================================================================
# 8. Tab 2: 馬別履歴詳細
# ==============================================================================
with tab2:
    st.header("📊 馬別履歴 & 買い条件設定")
    df_tab2_main = get_db_data()
    if not df_tab2_main.empty:
        col_t2_search1, col_t2_search2 = st.columns([1, 1])
        with col_t2_search1:
            input_search_name_tab2 = st.text_input("馬名で絞り込み検索", key="input_search_horse_t2")
        
        list_all_horses_sorted = sorted([str(x) for x in df_tab2_main['name'].dropna().unique()])
        with col_t2_search2:
            target_horse_selectbox = st.selectbox("個別メモ・条件編集対象を選択", ["未選択"] + list_all_horses_sorted)
        
        if target_horse_selectbox != "未選択":
            # 該当馬の最新インデックスを取得
            idx_found_list = df_tab2_main[df_tab2_main['name'] == target_horse_selectbox].index
            final_target_idx = idx_found_list[-1]
            
            with st.form("edit_horse_detail_form_t2"):
                val_current_memo = df_tab2_main.at[final_target_idx, 'memo'] if not pd.isna(df_tab2_main.at[final_target_idx, 'memo']) else ""
                input_new_memo_t2 = st.text_area("メモ・評価", value=val_current_memo)
                
                val_current_flag = df_tab2_main.at[final_target_idx, 'next_buy_flag'] if not pd.isna(df_tab2_main.at[final_target_idx, 'next_buy_flag']) else ""
                input_new_flag_t2 = st.text_input("個別買いフラグ", value=val_current_flag)
                
                if st.form_submit_button("設定保存"):
                    df_tab2_main.at[final_target_idx, 'memo'] = input_new_memo_t2
                    df_tab2_main.at[final_target_idx, 'next_buy_flag'] = input_new_flag_t2
                    if safe_update(df_tab2_main):
                        st.success(f"{target_horse_selectbox} の情報を更新しました")
                        st.rerun()
        
        # 検索フィルタリング
        if input_search_name_tab2:
            df_tab2_filtered = df_tab2_main[df_tab2_main['name'].str.contains(input_search_name_tab2, na=False)]
        else:
            df_tab2_filtered = df_tab2_main
            
        df_tab2_ready_formatted = df_tab2_filtered.copy()
        df_tab2_ready_formatted['base_rtc'] = df_tab2_ready_formatted['base_rtc'].apply(format_time)
        st.dataframe(
            df_tab2_ready_formatted.sort_values("date", ascending=False)[["date", "name", "last_race", "base_rtc", "f3f", "l3f", "race_l3f", "load", "memo", "next_buy_flag"]], 
            use_container_width=True
        )

# ==============================================================================
# 9. Tab 3: レース別履歴・答え合わせ
# ==============================================================================
with tab3:
    st.header("🏁 答え合わせ & レース別履歴")
    df_tab3_main = get_db_data()
    if not df_tab3_main.empty:
        full_race_list_tab3 = sorted([str(x) for x in df_tab3_main['last_race'].dropna().unique()])
        selectbox_race_tab3 = st.selectbox("表示するレースを選択", full_race_list_tab3)
        
        if selectbox_race_tab3:
            df_race_details_tab3 = df_tab3_main[df_tab3_main['last_race'] == selectbox_race_tab3].copy()
            with st.form("race_result_entry_form_t3"):
                st.write(f"【{selectbox_race_tab3}】の公式結果を入力")
                for idx_row_t3, row_item_t3 in df_race_details_tab3.iterrows():
                    val_pos_t3 = int(row_item_t3['result_pos']) if not pd.isna(row_item_t3['result_pos']) else 0
                    val_pop_t3 = int(row_item_t3['result_pop']) if not pd.isna(row_item_t3['result_pop']) else 0
                    
                    c_res_col1, c_res_col2 = st.columns(2)
                    with c_res_col1:
                        df_race_details_tab3.at[idx_row_t3, 'result_pos'] = st.number_input(f"{row_item_t3['name']} 着順", 0, 100, value=val_pos_t3, key=f"pos_in_t3_{idx_row_t3}")
                    with c_res_col2:
                        df_race_details_tab3.at[idx_row_t3, 'result_pop'] = st.number_input(f"{row_item_t3['name']} 人気", 0, 100, value=val_pop_t3, key=f"pop_in_t3_{idx_row_t3}")
                
                if st.form_submit_button("結果を一括保存"):
                    for idx_save_t3, row_save_t3 in df_race_details_tab3.iterrows():
                        df_tab3_main.at[idx_save_t3, 'result_pos'] = row_save_t3['result_pos']
                        df_tab3_main.at[idx_save_t3, 'result_pop'] = row_save_t3['result_pop']
                    if safe_update(df_tab3_main):
                        st.success("レース結果の保存が完了しました。")
                        st.rerun()
            
            df_race_t3_display_f = df_race_details_tab3.copy()
            df_race_t3_display_f['base_rtc'] = df_race_t3_display_f['base_rtc'].apply(format_time)
            st.dataframe(df_race_t3_display_f[["name", "notes", "base_rtc", "f3f", "l3f", "race_l3f", "result_pos", "result_pop"]], use_container_width=True)

# ==============================================================================
# 10. Tab 4: 次走シミュレーター (全高度ロジック搭載)
# ==============================================================================
with tab4:
    st.header("🎯 次走シミュレーター & 統合評価")
    df_tab4_main = get_db_data()
    if not df_tab4_main.empty:
        list_h_names_t4 = sorted([str(x) for x in df_tab4_main['name'].dropna().unique()])
        multiselect_horses_sim = st.multiselect("出走馬をリストから選択してください", options=list_h_names_t4)
        
        sim_pops_input_dict = {}
        sim_gates_input_dict = {}
        sim_weights_input_dict = {}
        
        if multiselect_horses_sim:
            st.markdown("##### 📝 枠番・予想人気・想定斤量の個別入力")
            sim_input_cols = st.columns(min(len(multiselect_horses_sim), 4))
            for i_sim, h_name_sim in enumerate(multiselect_horses_sim):
                with sim_input_cols[i_sim % 4]:
                    h_lat_data_sim = df_tab4_main[df_tab4_main['name'] == h_name_sim].iloc[-1]
                    sim_gates_input_dict[h_name_sim] = st.number_input(f"{h_name_sim} 枠", 1, 18, value=1, key=f"sim_g_in_{h_name_sim}")
                    sim_pops_input_dict[h_name_sim] = st.number_input(f"{h_name_sim} 人気", 1, 18, value=int(h_lat_data_sim['result_pop']) if not pd.isna(h_lat_data_sim['result_pop']) else 10, key=f"sim_p_in_{h_name_sim}")
                    # 🌟 【完全復元】馬別想定斤量の入力
                    sim_weights_input_dict[h_name_sim] = st.number_input(f"{h_name_sim} 斤量", 48.0, 62.0, 56.0, step=0.5, key=f"sim_w_in_{h_name_sim}")

            col_sim_ctrl1, col_sim_ctrl2 = st.columns(2)
            with col_sim_ctrl1: 
                sim_target_course_name = st.selectbox("次走競馬場を選択", list(COURSE_DATA.keys()), key="sim_c_sel_t4")
                sim_target_dist_val = st.selectbox("次走距離 (m)", dist_options_list, index=6)
                sim_target_track_type = st.radio("次走トラック種別", ["芝", "ダート"], horizontal=True)
            with col_sim_ctrl2: 
                sim_input_cushion_val = st.slider("想定クッション値", 7.0, 12.0, 9.5)
                sim_input_water_val = st.slider("想定含水率 (%)", 0.0, 30.0, 10.0)
            
            if st.button("🏁 シミュレーション実行"):
                final_sim_result_list = []
                val_num_total_sim = len(multiselect_horses_sim)
                dict_styles_sim_counts = {"逃げ": 0, "先行": 0, "差し": 0, "追込": 0}
                val_db_overall_l3f_avg = df_tab4_main['l3f'].mean()

                for h_name_run in multiselect_horses_sim:
                    h_full_history_run = df_tab4_main[df_tab4_main['name'] == h_name_run].sort_values("date")
                    sim_last_3_runs_list = h_full_history_run.tail(3)
                    converted_rtc_sim_buffer = []
                    
                    # 脚質の詳細判定
                    val_avg_load_3r_run = sim_last_3_runs_list['load'].mean()
                    if val_avg_load_3r_run <= 3.5: 
                        h_determined_style = "逃げ"
                    elif val_avg_load_3r_run <= 7.0: 
                        h_determined_style = "先行"
                    elif val_avg_load_3r_run <= 11.0: 
                        h_determined_style = "差し"
                    else: 
                        h_determined_style = "追込"
                    dict_styles_sim_counts[h_determined_style] += 1

                    # 🌟 頭数連動ロジック：多頭数渋滞リスク判定
                    label_jam_risk = "⚠️詰まり注意" if val_num_total_sim >= 15 and h_determined_style in ["差し", "追込"] and sim_gates_input_dict[h_name_run] <= 4 else "-"
                    
                    # 🌟 頭数連動ロジック：少頭数スロー適性判定
                    label_slow_apt = "-"
                    if val_num_total_sim <= 10:
                        h_min_l3f_val = h_full_history_run['l3f'].min()
                        if h_min_l3f_val < val_db_overall_l3f_avg - 0.5:
                            label_slow_apt = "⚡スロー特化"
                        elif h_min_l3f_val > val_db_overall_l3f_avg + 0.5:
                            label_slow_apt = "📉瞬発力不足"

                    h_rtc_std_val_sim = h_full_history_run['base_rtc'].std() if len(h_full_history_run) >= 3 else 0.0
                    h_stab_label_sim = "⚖️安定" if 0 < h_rtc_std_val_sim < 0.2 else "🎢ムラ" if h_rtc_std_val_sim > 0.4 else "-"
                    
                    h_best_p_data_sim = h_full_history_run.loc[h_full_history_run['base_rtc'].idxmin()]
                    h_apt_label_sim = "🎯馬場◎" if abs(h_best_p_data_sim['cushion'] - sim_input_cushion_val) <= 0.5 and abs(h_best_p_data_sim['water'] - sim_input_water_val) <= 2.0 else "-"

                    # 🌟 【完全復旧】過去3走すべての斤量・負荷補正詳細ループ
                    for i_sim_run, row_sim_run in sim_last_3_runs_list.iterrows():
                        p_dist_sim = row_sim_run['dist']
                        p_rtc_sim = row_sim_run['base_rtc']
                        p_course_sim = row_sim_run['course']
                        p_load_sim = row_sim_run['load']
                        p_notes_sim = str(row_sim_run['notes'])
                        
                        p_weight_sim = 56.0
                        h_bw_sim = 480.0
                        
                        # 過去の斤量抽出
                        w_match_sim_run = re.search(r'([4-6]\d\.\d)', p_notes_sim)
                        if w_match_sim_run:
                            p_weight_sim = float(w_match_sim_run.group(1))
                            
                        # 過去の馬体重抽出
                        hb_match_sim_run = re.search(r'\((\d{3})kg\)', p_notes_sim)
                        if hb_match_sim_run:
                            h_bw_sim = float(hb_match_sim_run.group(1))
                        
                        if p_dist_sim > 0:
                            l_adj_sim = (p_load_sim - 7.0) * 0.02
                            # 斤量感応度の詳細ロジック（1ミリも削らず記述）
                            if h_bw_sim <= 440:
                                val_sensitivity = 0.15
                            elif h_bw_sim >= 500:
                                val_sensitivity = 0.08
                            else:
                                val_sensitivity = 0.1
                                
                            w_diff_sim = (sim_input_weights[h_name_run] - p_weight_sim) * val_sensitivity
                            
                            # RTC能力指数の距離変換
                            base_conv_sim = (p_rtc_sim + l_adj_sim + w_diff_sim) / p_dist_sim * sim_target_dist
                            # 競馬場間の坂補正
                            slope_adj_sim = (SLOPE_FACTORS.get(sim_target_course_name, 0.002) - SLOPE_FACTORS.get(p_course_sim, 0.002)) * sim_target_dist
                            converted_rtc_sim_buffer.append(base_conv_sim + slope_adj_sim)
                    
                    val_avg_rtc_final = sum(converted_rtc_sim_buffer) / len(converted_rtc_sim_buffer) if converted_rtc_sim_buffer else 0
                    
                    # 距離相性（自己ベスト距離との乖離によるペナルティ）
                    h_best_d_past_sim = h_full_history_run.loc[h_full_history_run['base_rtc'].idxmin(), 'dist']
                    val_avg_rtc_final += (abs(sim_target_dist - h_best_d_past_sim) / 100) * 0.05
                    
                    # 直近上昇モメンタム判定詳細
                    label_mom_sim = "-"
                    if len(h_full_history_run) >= 2:
                        if h_full_history_run.iloc[-1]['base_rtc'] < h_full_history_run.iloc[-2]['base_rtc'] - 0.2:
                            label_mom_sim = "📈上昇"
                            val_avg_rtc_final -= 0.15

                    # 枠順×バイアス詳細シナジー補正
                    val_syn_bias_sim = -0.2 if (sim_input_gates[h_name_run] <= 4 and input_bias_slider_val <= -0.5) or (sim_input_gates[h_name_run] >= 13 and input_bias_slider_val >= 0.5) else 0
                    val_avg_rtc_final += val_syn_bias_sim

                    # 当該コース実績ボーナス
                    val_h_course_bonus_sim = -0.2 if any((h_full_history_run['course'] == sim_target_course_name) & (h_full_history_run['result_pos'] <= 3)) else 0.0
                    
                    # 馬場状況（含水率・クッション値）の最終調整
                    val_water_adj_final_sim = (sim_input_water_val - 10.0) * 0.05
                    dict_c_master_sim = DIRT_COURSE_DATA if sim_target_track_type == "ダート" else COURSE_DATA
                    if sim_target_track_type == "ダート":
                        val_water_adj_final_sim = -val_water_adj_final_sim # ダートは水分で加速する
                    
                    val_final_rtc_sim_result = (val_avg_rtc_final + (dict_c_master_sim[sim_target_course_name] * (sim_target_dist/1600.0)) + val_h_course_bonus_sim + val_water_adj_final_sim - (9.5 - sim_input_cushion_val) * 0.1)
                    
                    h_lat_entry_sim = sim_last_3_runs_list.iloc[-1]
                    final_sim_result_list.append({
                        "馬名": h_name_run, 
                        "脚質": h_determined_style, 
                        "想定タイム": val_final_rtc_sim_result, 
                        "渋滞": label_jam_risk, 
                        "スロー": label_slow_apt, 
                        "適性": h_apt_label_sim, 
                        "安定": h_stab_label_sim, 
                        "偏差": "⤴️覚醒期待" if val_final_rtc_sim_result < h_full_history_run['base_rtc'].min() - 0.3 else "-", 
                        "上昇": label_mom_sim, 
                        "レベル": "🔥強ﾒﾝﾂ" if df_tab4_main[df_tab4_main['last_race'] == h_lat_entry_sim['last_race']]['base_rtc'].mean() < df_tab4_main['base_rtc'].mean() - 0.2 else "-", 
                        "load": h_lat_entry_sim['load'], 
                        "状態": "💤休み明け" if (datetime.now() - h_lat_entry_sim['date']).days // 7 >= 12 else "-", 
                        "raw_rtc": val_final_rtc_sim_result, 
                        "解析メモ": h_lat_entry_sim['memo']
                    })
                
                # 展開予想詳細
                label_sim_pace_pred = "ミドルペース"
                if dict_styles_sim_counts["逃げ"] >= 2 or (dict_styles_sim_counts["逃げ"] + dict_styles_sim_counts["先行"]) >= val_num_total_sim * 0.6:
                    label_sim_pace_pred = "ハイペース傾向"
                elif dict_styles_sim_counts["逃げ"] == 0 and dict_styles_sim_counts["先行"] <= 1:
                    label_sim_pace_pred = "スローペース傾向"
                
                df_sim_final_results = pd.DataFrame(final_sim_result_list)
                
                # 🌟 脚質・展開シナジー反映（多頭数時は影響度1.5倍に強化）
                val_sim_pace_multiplier = 1.5 if val_num_total_sim >= 15 else 1.0
                
                def compute_pace_synergy_val(row):
                    val_adj = 0.0
                    if "ハイ" in label_sim_pace_pred:
                        if row['脚質'] in ["差し", "追込"]: 
                            val_adj = -0.2 * val_sim_pace_multiplier
                        elif row['脚質'] == "逃げ": 
                            val_adj = 0.2 * val_sim_pace_multiplier
                    elif "スロー" in label_sim_pace_pred:
                        if row['脚質'] in ["逃げ", "先行"]: 
                            val_adj = -0.2 * val_sim_pace_multiplier
                        elif row['脚質'] in ["差し", "追込"]: 
                            val_adj = 0.2 * val_sim_pace_multiplier
                    return row['raw_rtc'] + val_adj

                df_sim_final_results['synergy_rtc'] = df_sim_final_results.apply(compute_pace_synergy_val, axis=1)
                df_sim_final_results = df_sim_final_results.sort_values("synergy_rtc")
                df_sim_final_results['RTC順位'] = range(1, len(df_sim_final_results) + 1)
                
                val_sim_top_time = df_sim_final_results.iloc[0]['raw_rtc']
                df_sim_final_results['差'] = df_sim_final_results['raw_rtc'] - val_sim_top_time
                df_sim_final_results['予想人気'] = df_sim_final_results['馬名'].map(sim_pops_input_dict)
                df_sim_final_results['妙味スコア'] = df_sim_final_results['予想人気'] - df_sim_final_results['RTC順位']
                
                # 推奨印の割り当て詳細
                df_sim_final_results['役割'] = "-"
                df_sim_final_results.loc[df_sim_final_results['RTC順位'] == 1, '役割'] = "◎"
                df_sim_final_results.loc[df_sim_final_results['RTC順位'] == 2, '役割'] = "〇"
                df_sim_final_results.loc[df_sim_final_results['RTC順位'] == 3, '役割'] = "▲"
                df_sim_potential_bombs = df_sim_final_results[df_sim_final_results['RTC順位'] > 1].sort_values("妙味スコア", ascending=False)
                if not df_sim_potential_bombs.empty:
                    df_sim_final_results.loc[df_sim_final_results['馬名'] == df_sim_potential_bombs.iloc[0]['馬名'], '役割'] = "★"
                
                # 表示用変換
                df_sim_final_results['想定タイム'] = df_sim_final_results['raw_rtc'].apply(format_time)
                df_sim_final_results['差'] = df_sim_final_results['差'].apply(lambda x: f"+{x:.1f}" if x > 0 else "±0.0")

                st.markdown("---")
                st.subheader(f"🏁 展開予想：{label_sim_pace_pred} ({val_num_total_sim}頭立て)")
                col_rec_f_1, col_rec_f_2 = st.columns(2)
                
                sim_fav_h = df_sim_final_results[df_sim_final_results['役割'] == "◎"].iloc[0]['馬名'] if not df_sim_final_results[df_sim_final_results['役割'] == "◎"].empty else ""
                sim_opp_h = df_sim_final_results[df_sim_final_results['役割'] == "〇"].iloc[0]['馬名'] if not df_sim_final_results[df_sim_final_results['役割'] == "〇"].empty else ""
                sim_bomb_h = df_sim_final_results[df_sim_final_results['役割'] == "★"].iloc[0]['馬名'] if not df_sim_final_results[df_sim_final_results['役割'] == "★"].empty else ""
                
                with col_rec_f_1:
                    st.info(f"**🎯 馬連・ワイド1点勝負**\n\n◎ {sim_fav_h} － 〇 {sim_opp_h}")
                with col_rec_f_2: 
                    if sim_bomb_h:
                        st.warning(f"**💣 妙味狙いワイド1点**\n\n◎ {sim_fav_h} － ★ {sim_bomb_h} (展開×妙味)")
                
                def style_highlight_sim(row):
                    if row['役割'] == "★":
                        return ['background-color: #ffe4e1; font-weight: bold'] * len(row)
                    if row['役割'] == "◎":
                        return ['background-color: #fff700; font-weight: bold; color: black'] * len(row)
                    return [''] * len(row)
                
                st.table(df_sim_final_results[["役割", "馬名", "脚質", "渋滞", "スロー", "想定タイム", "差", "妙味スコア", "適性", "安定", "上昇", "レベル", "偏差", "load", "状態", "解析メモ"]].style.apply(style_highlight_sim, axis=1))

# ==============================================================================
# 11. Tab 5: トレンド統計解析
# ==============================================================================
with tab5:
    st.header("📈 馬場トレンド & 統計解析")
    df_tab5_main = get_db_data()
    if not df_tab5_main.empty:
        sel_tc_tab5 = st.selectbox("トレンドを確認する競馬場を選択", list(COURSE_DATA.keys()), key="tc_sel_tab5_main")
        df_td_tab5 = df_tab5_main[df_tab5_main['course'] == sel_tc_tab5].sort_values("date")
        if not df_td_tab5.empty:
            st.subheader("💧 クッション値 & 含水率の時系列推移")
            st.line_chart(df_td_tab5.set_index("date")[["cushion", "water"]])
            st.subheader("🏁 直近のレース傾向 (4角平均通過順位)")
            df_td_recent_tab5 = df_td_tab5.groupby('last_race').agg({'load':'mean', 'date':'max'}).sort_values('date', ascending=False).head(15)
            st.bar_chart(df_td_recent_tab5['load'])
            st.subheader("📊 直近上がり3F推移")
            st.line_chart(df_td_tab5.set_index("date")["race_l3f"])

# ==============================================================================
# 12. Tab 6: メンテナンス & データ管理
# ==============================================================================
with tab6:
    st.header("🗑 データベース保守 & 管理機能")
    
    # 🌟 同期不全解消：手動修正反映用ボタンを詳細記述
    if st.button("🔄 スプレッドシートの手動修正を同期（キャッシュ破棄）"):
        st.cache_data.clear()
        st.success("最新のシート内容を読み込みます。")
        st.rerun()

    df_tab6_main = get_db_data()

    def update_eval_tags_full_logic_冗長(row, df_context=None):
        """【完全復元】再解析用詳細ロジック（1ミリも削らず記述）"""
        raw_memo_v = str(row['memo']) if not pd.isna(row['memo']) else ""
        
        def to_f_verbose(v):
            try: return float(v) if not pd.isna(v) else 0.0
            except: return 0.0
            
        f3_v, l3_v, rl3_v, pos_v, l_pos_v, d_v, rtc_v = map(to_f_verbose, [
            row['f3f'], row['l3f'], row['race_l3f'], row['result_pos'], row['load'], row['dist'], row['base_rtc']
        ])
        
        # 🌟 notesから斤量を再抽出（手動修正反映の核心）
        s_notes_v = str(row['notes'])
        match_w_v = re.search(r'([4-6]\d\.\d)', s_notes_v)
        val_indiv_w_v = float(match_w_v.group(1)) if match_w_v else 56.0
        
        # 中盤ラップ判定詳細
        label_mid_n_v = "平"
        if d_v > 1200 and f3_v > 0:
            val_ml_v = (rtc_v - f3_v - l3_v) / ((d_v - 1200) / 200)
            if val_ml_v >= 12.8: label_mid_n_v = "緩"
            elif val_ml_v <= 11.8: label_mid_n_v = "締"
        elif d_v <= 1200:
            label_mid_n_v = "短"

        # バイアス特例判定詳細（完全再現）
        label_bt_v = "フラット"; val_mx_v = 16
        if df_context is not None and not pd.isna(row['last_race']):
            df_rc_v = df_context[df_context['last_race'] == row['last_race']]
            val_mx_v = df_rc_v['result_pos'].max() if not df_rc_v.empty else 16
            df_top3_v = df_rc_v[pd.to_numeric(df_rc_v['result_pos'], errors='coerce') <= 3].copy()
            df_top3_v['load'] = pd.to_numeric(df_top3_v['load'], errors='coerce').fillna(7.0)
            
            list_out_v = df_top3_v[(df_top3_v['load'] >= 10.0) | (df_top3_v['load'] <= 3.0)]
            if len(list_out_v) == 1:
                df_bias_set_v = pd.concat([
                    df_top3_v[df_top3_v['name'] != list_out_v.iloc[0]['name']], 
                    df_rc_v[pd.to_numeric(df_rc_v['result_pos'], errors='coerce') == 4]
                ])
            else:
                df_bias_set_v = df_top3_v
            
            if not df_bias_set_v.empty:
                val_avg_bias_v = df_bias_set_v['load'].mean()
                if val_avg_bias_v <= 4.0: label_bt_v = "前有利"
                elif val_avg_bias_v >= 10.0: label_bt_v = "後有利"

        # 強度補正詳細判定
        label_ps_v = "ハイペース" if "ハイ" in raw_memo_v else "スローペース" if "スロー" in raw_memo_v else "ミドルペース"
        val_pd_v = 1.5 if label_ps_v != "ミドルペース" else 0.0
        val_rp_v = l_pos_v / val_mx_v
        val_fi_v = val_mx_v / 16.0
        
        val_nl_score_v = 0.0
        if label_ps_v == "ハイペース" and label_bt_v != "前有利":
            val_nl_score_v = max(0, (0.6 - val_rp_v) * val_pd_v * 3.0) * val_fi_v
        elif label_ps_v == "スローペース" and label_bt_v != "後有利":
            val_nl_score_v = max(0, (val_rp_v - 0.4) * val_pd_v * 2.0) * val_fi_v
        
        list_tags_v = []; is_counter_v = False
        if rl3_v > 0:
            if (rl3_v - l3_v) >= 0.5: list_tags_v.append("🚀 アガリ優秀")
            elif (rl3_v - l3_v) <= -1.0: list_tags_v.append("📉 失速大")
            
        if pos_v <= 5:
            if label_bt_v == "前有利" and l_pos_v >= 10.0:
                list_tags_v.append("💎💎 ﾊﾞｲｱｽ極限逆行" if val_mx_v >= 16 else "💎 ﾊﾞｲｱｽ逆行")
                is_counter_v = True
            elif label_bt_v == "後有利" and l_pos_v <= 3.0:
                list_tags_v.append("💎💎 ﾊﾞｲｱｽ極限逆行" if val_mx_v >= 16 else "💎 ﾊﾞｲｱｽ逆行")
                is_counter_v = True
            
            if label_ps_v == "ハイペース" and label_bt_v != "前有利" and l_pos_v <= 3.0:
                list_tags_v.append("📉 激流被害" if val_mx_v >= 14 else "🔥 展開逆行")
                is_counter_v = True
            elif label_ps_v == "スローペース" and label_bt_v != "後有利" and l_pos_v >= 10.0:
                if (f3_v - l3_v) > 1.5:
                    list_tags_v.append("🔥 展開逆行")
                    is_counter_v = True
        
        if val_mx_v <= 10 and label_ps_v == "スローペース" and pos_v <= 2:
            list_tags_v.append("🟢 展開恩恵")

        label_field_v = "多" if val_mx_v >= 16 else "少" if val_mx_v <= 10 else "中"
        memo_update_str = (f"【{label_ps_v}/{label_bt_v}/負荷:{val_nl_score_v:.1f}({label_field_v})/{label_mid_n_v}】" + "/".join(list_tags_v)).strip("/")
        flag_update_str = ("★逆行狙い " + str(row['next_buy_flag']).replace("★逆行狙い", "")).strip() if is_counter_v else str(row['next_buy_flag']).replace("★逆行狙い", "").strip()
        
        return memo_update_str, flag_update_str

    # --- 🗓 過去レースの開催週を一括設定セクション ---
    st.subheader("🗓 過去レースの開催週を一括設定")
    if not df_tab6_main.empty:
        df_race_master_weeks = df_tab6_main[['last_race', 'date']].drop_duplicates(subset=['last_race']).copy()
        df_race_master_weeks['track_week'] = 1
        # エディタで週数を一括入力可能に
        df_edited_weeks_tab6 = st.data_editor(df_race_master_weeks, hide_index=True)
        
        if st.button("🔄 補正&再解析を一括適用"):
            dict_week_lookup = dict(zip(df_edited_weeks_tab6['last_race'], df_edited_weeks_tab6['track_week']))
            for idx_w_6, row_w_6 in df_tab6_main.iterrows():
                if row_w_6['last_race'] in dict_week_lookup:
                    # RTC指数の遡り補正
                    df_tab6_main.at[idx_w_6, 'base_rtc'] = row_w_6['base_rtc'] - (dict_week_lookup[row_w_6['last_race']] - 1) * 0.05
                    # メモとフラグも最新ロジックで再生成
                    m_re_6, f_re_6 = update_eval_tags_full_logic_冗長(df_tab6_main.iloc[idx_w_6], df_tab6_main)
                    df_tab6_main.at[idx_w_6, 'memo'] = m_re_6
                    df_tab6_main.at[idx_w_6, 'next_buy_flag'] = f_re_6
            
            if safe_update(df_tab6_main):
                st.success("過去全データの開催週補正と再計算が完了しました。")
                st.rerun()

    st.subheader("🛠️ 一括処理メニュー")
    col_adm_btn_1, col_adm_btn_2 = st.columns(2)
    with col_adm_btn_1:
        if st.button("🔄 DB再解析（最新数値を基に上書き）"):
            # 🌟 【完全復旧】同期不全解消・手動修正反映の核心
            st.cache_data.clear()
            df_latest_db_sync = conn.read(ttl=0)
            # カラム正規化
            for col_n_6 in all_cols:
                if col_n_6 not in df_latest_db_sync.columns: df_latest_db_sync[col_n_6] = None
            
            for idx_sync, row_sync in df_latest_db_sync.iterrows():
                m_sync, f_sync = update_eval_tags_full_logic_冗長(row_sync, df_latest_db_sync)
                df_latest_db_sync.at[idx_sync, 'memo'] = m_sync
                df_latest_db_sync.at[idx_sync, 'next_buy_flag'] = f_sync
            
            if safe_update(df_latest_db_sync):
                st.success("全履歴を最新数値を基に同期・再解析しました。")
                st.rerun()
    with col_adm_btn_2:
        if st.button("🧼 重複削除"):
            cnt_before_clean = len(df_tab6_main)
            df_tab6_main = df_tab6_main.drop_duplicates(subset=['name', 'date', 'last_race'], keep='first')
            if safe_update(df_tab6_main):
                st.success(f"重複データ {cnt_before_clean - len(df_tab6_main)} 件をクリーニングしました。"); st.rerun()

    if not df_tab6_main.empty:
        st.subheader("🛠️ データ編集エディタ")
        df_tab6_edit_f = df_tab6_main.copy()
        df_tab6_edit_f['base_rtc'] = df_tab6_edit_f['base_rtc'].apply(format_time)
        df_final_edited_admin = st.data_editor(
            df_tab6_edit_f.sort_values("date", ascending=False), 
            num_rows="dynamic", 
            use_container_width=True
        )
        if st.button("💾 エディタの変更内容を反映"):
            df_save_admin = df_final_edited_admin.copy()
            df_save_admin['base_rtc'] = df_save_admin['base_rtc'].apply(parse_time_str)
            if safe_update(df_save_admin):
                st.success("エディタの内容をDBに反映しました。"); st.rerun()
        
        st.divider()
        st.subheader("❌ データ削除設定")
        col_del_final_1, col_del_final_2 = st.columns(2)
        with col_del_final_1:
            list_all_r_del = sorted([str(x) for x in df_tab6_main['last_race'].dropna().unique()])
            sel_target_r_del = st.selectbox("削除対象レースを選択", ["未選択"] + list_all_r_del)
            if sel_target_r_del != "未選択":
                if st.button(f"🚨 レース【{sel_target_r_del}】を全削除"):
                    if safe_update(df_tab6_main[df_tab6_main['last_race'] != sel_target_r_del]): st.rerun()
        with col_del_final_2:
            list_all_h_del = sorted([str(x) for x in df_tab6_main['name'].dropna().unique()])
            # 🌟 【完全復元】マルチセレクト形式の一括削除
            list_target_h_del = st.multiselect("削除馬選択（複数可）", list_all_h_del, key="mult_del_final_admin")
            if list_target_h_del:
                if st.button(f"🚨 選択した{len(list_target_h_del)}頭をDBから削除"):
                    if safe_update(df_tab6_main[~df_tab6_main['name'].isin(list_target_h_del)]): st.rerun()

        st.divider()
        with st.expander("☢️ システム初期化"):
            st.warning("この操作は取り消せません。全データを完全に抹消します。")
            if st.button("🧨 データベースを完全にリセット"):
                if safe_update(pd.DataFrame(columns=df_tab6_main.columns)):
                    st.success("DBを初期化しました。"); st.rerun()
