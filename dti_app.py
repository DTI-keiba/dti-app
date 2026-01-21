import streamlit as st
import pandas as pd
import re
from streamlit_gsheets import GSheetsConnection
from datetime import datetime

# --- ページ設定 ---
st.set_page_config(page_title="DTI Ultimate DB", layout="wide")

# --- Google Sheets 接続 ---
conn = st.connection("gsheets", type=GSheetsConnection)

def get_db_data():
    try:
        # スプレッドシートから最新データを取得
        return conn.read(ttl="0") 
    except:
        return pd.DataFrame(columns=["name", "base_rtc", "last_race", "course", "dist", "notes", "timestamp"])

def format_time(seconds):
    if seconds is None: return ""
    m = int(seconds // 60)
    s = seconds % 60
    return f"{m}:{s:04.1f}"

# 競馬場データ
COURSE_DATA = {
    "東京": 0.10, "中山": 0.25, "京都": 0.15, "阪神": 0.18, "中京": 0.20,
    "新潟": 0.05, "小倉": 0.30, "福島": 0.28, "札幌": 0.22, "函館": 0.25
}

# --- メイン UI ---
tab1, tab2, tab3 = st.tabs(["📝 レース解析・保存", "📊 馬別履歴データベース", "🎯 次走シミュレーター"])

with tab1:
    st.header("🚀 レース解析 & 自動保存")
    with st.sidebar:
        r_name = st.text_input("レース名")
        c_name = st.selectbox("競馬場", list(COURSE_DATA.keys()))
        t_type = st.radio("種別", ["芝", "ダート"])
        dist = st.number_input("距離 (m)", 800, 4000, 1600)
        cush = st.slider("クッション値", 7.0, 12.0, 9.5) if t_type == "芝" else 9.5
        bias = st.slider("馬場補正 (秒)", -1.0, 1.0, 0.0)

    col1, col2 = st.columns(2)
    with col1: lap_input = st.text_area("ラップタイム入力")
    with col2: raw_input = st.text_area("JRA成績表貼り付け")

    if st.button("🚀 解析してDBへ保存"):
        if raw_input:
            # 簡単なパース処理
            clean_text = re.sub(r'\s+', ' ', raw_input)
            matches = list(re.finditer(r'(\d{1,2}:\d{2}\.\d)', clean_text))
            
            new_rows = []
            for m in matches:
                time_str = m.group(1)
                before = clean_text[max(0, m.start()-100):m.start()]
                
                # 馬名と体重の抽出
                weight_m = re.search(r'(\d{2}\.\d)', before)
                name = "不明"; weight = 56.0
                if weight_m:
                    weight = float(weight_m.group(1))
                    parts = re.findall(r'([ァ-ヶー]{2,})', before[:weight_m.start()])
                    if parts: name = parts[-1]
                
                # RTC計算（簡易版コア）
                m_p, s_p = map(float, time_str.split(':'))
                sec = m_p * 60 + s_p
                rtc = sec + bias - (weight-56)*0.1 # 簡易計算
                
                new_rows.append({
                    "name": name,
                    "base_rtc": rtc,
                    "last_race": r_name,
                    "course": c_name,
                    "dist": dist,
                    "notes": "保存済み",
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M")
                })
            
            if new_rows:
                existing_df = get_db_data()
                updated_df = pd.concat([existing_df, pd.DataFrame(new_rows)], ignore_index=True)
                conn.update(data=updated_df)
                st.success(f"✅ {len(new_rows)}頭のデータをスプレッドシートへ保存しました！")

with tab2:
    st.header("📊 馬別履歴データベース")
    df = get_db_data()
    if not df.empty:
        search = st.text_input("馬名で検索")
        if search:
            df = df[df['name'].str.contains(search)]
        st.dataframe(df.sort_values("timestamp", ascending=False), use_container_width=True)
    else:
        st.info("データがまだありません。")

with tab3:
    st.header("🎯 次走シミュレーター")
    df = get_db_data()
    if not df.empty:
        selected = st.multiselect("出走馬を選択", df['name'].unique())
        if selected:
            target_c = st.selectbox("次走の競馬場", list(COURSE_DATA.keys()))
            if st.button("🏁 シミュレーション実行"):
                results = []
                for h in selected:
                    h_data = df[df['name'] == h].iloc[-1] # 最新データ
                    # 競馬場ごとの補正計算
                    sim_rtc = h_data['base_rtc'] + COURSE_DATA[target_c]
                    results.append({"馬名": h, "想定RTC": format_time(sim_rtc), "raw": sim_rtc})
                
                res_df = pd.DataFrame(results).sort_values("raw")
                st.table(res_df[["馬名", "想定RTC"]])
