import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime
from pymongo import MongoClient  


st.set_page_config(page_title="Hệ Thống Giám Sát Real-time", layout="wide")


# KẾT NỐI MONGODB 

@st.cache_resource 
def init_mongo():
    client = MongoClient("mongodb+srv://22521294_db_user:aHVwCWbKNQYSjrQX@ie212.q11zp3u.mongodb.net/?appName=ie212&tlsAllowInvalidCertificates=true")
    return client["air_quality_db"]

db = init_mongo()
history_col = db["streaming_history"]
forecast_col = db["latest_predictions"]

st.title("🍃 Hệ Thống Giám Sát Không Khí Real-time (Kết nối MongoDB)")
st.markdown("---")


st.sidebar.header("🕹️ BẢNG ĐIỀU KHIỂN")
selected_station = st.sidebar.selectbox("Chọn Trạm Kiểm Đo Dự Báo:", options=[f"Trạm Số {i}" for i in range(1, 7)])
station_idx = int(selected_station.split(" ")[-1])

auto_update = st.sidebar.toggle("🔄 Chế độ tự cập nhật", value=True)
refresh_rate = st.sidebar.slider("Tốc độ làm mới (giây):", min_value=2, max_value=30, value=5)




cursor_actual = history_col.find({"Station_No": station_idx}).sort("_id", -1).limit(24)
list_actual = list(cursor_actual)

if len(list_actual) > 0:
    
    df_station = pd.DataFrame(list_actual).iloc[::-1] 
else:
    df_station = pd.DataFrame()

# 2. Lấy tài liệu dự báo mới nhất
latest_pred_data = forecast_col.find_one({"_id": "global_latest"})


# HIỂN THỊ CHỈ SỐ THỜI GIAN THỰC (METRICS)

if not df_station.empty:
    latest_row = df_station.iloc[-1]
    st.success(f"📌 Dữ liệu thực tế từ MongoDB cập nhật lúc: **{latest_row['date']}** tại **{selected_station}**")
    
    m1, m2, m3, m4 = st.columns(4)
    with m1: st.metric(label="Nồng độ PM2.5", value=f"{latest_row['PM2_5']:.2f} µg/m³")
    with m2: st.metric(label="Nồng độ TSP", value=f"{latest_row['TSP']:.2f} µg/m³")
    with m3: st.metric(label="Nhiệt độ (Temp)", value=f"{int(latest_row['Temperature'])} °C")
    with m4: st.metric(label="Độ ẩm (Humidity)", value=f"{int(latest_row['Humidity'])} %")
else:
    st.warning("⚠️ Đang chờ dữ liệu thực tế đồng bộ từ Kafka vào MongoDB...")

st.markdown("---")


# V VẼ BIỂU ĐỒ (Giữ nguyên logic vẽ đồ thị, chỉ thay đổi nguồn dữ liệu đầu vào)

st.header(f"📈 Xu Hướng Biến Động PM2.5 (Thực Tế Quá Khứ vs Dự Báo Tương Lai)")
station_key = f"station_{station_idx}"
has_predictions = latest_pred_data and "predictions" in latest_pred_data and station_key in latest_pred_data["predictions"]

if not df_station.empty:
    actual_times = df_station['date'].tolist()
    actual_values = df_station['PM2_5'].tolist()
    
    fig = go.Figure()
    
    
    fig.add_trace(go.Scatter(
        x=actual_times, y=actual_values,
        mode='lines+markers', name='Dữ liệu Thực Tế (MongoDB)',
        line=dict(color='#1f77b4', width=3)
    ))
    
    
    if has_predictions:
        pred_list = latest_pred_data["predictions"][station_key]
        current_time_str = latest_pred_data["current_time"]
        
        try:
            base_time = datetime.strptime(current_time_str, "%d-%m-%Y %H:%M")
        except ValueError:
            base_time = datetime.now()
            
        future_labels = [(base_time + pd.Timedelta(hours=h)).strftime("%d-%m-%Y %H:%M") for h in range(1, 25)]
        
       
        connect_time = [actual_times[-1]] + future_labels
        connect_values = [actual_values[-1]] + pred_list
        
        fig.add_trace(go.Scatter(
            x=connect_time, y=connect_values,
            mode='lines+markers', name='Dự Báo (24h Tiếp Theo)',
            line=dict(color='#FF4B4B', width=3, dash='dash'),
            marker=dict(symbol='diamond')
        ))
        st.info(f"🔮 Dự báo nạp từ MongoDB | Mốc tính toán gần nhất: **{current_time_str}**")
        
    fig.update_layout(hovermode="x unified", height=500)
    st.plotly_chart(fig, use_container_width=True)


if auto_update:
    import time
    time.sleep(refresh_rate)
    st.rerun()