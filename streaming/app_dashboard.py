import streamlit as st
import pandas as pd
import json
import os
import time
import plotly.graph_objects as go
from datetime import datetime


st.set_page_config(
    page_title="Hệ Thống Giám Sát & Dự Báo Không Khí Real-time",
    page_icon="🍃",
    layout="wide"
)


HISTORY_FILE = "data/streaming_history.csv"
PREDICT_FILE = "data/latest_predictions.json"

st.title("🍃 Hệ Thống Giám Sát & Dự Báo Chất Lượng Không Khí Real-time")
st.subheader("Đồ án môn học: Phân tích dữ liệu luồng trực tuyến (IE212)")
st.markdown("---")


st.sidebar.header("🕹️ BẢNG ĐIỀU KHIỂN")


selected_station = st.sidebar.selectbox(
    "Chọn Trạm Kiểm Đo Dự Báo:",
    options=[f"Trạm Số {i}" for i in range(1, 7)]
)
station_idx = int(selected_station.split(" ")[-1]) 

st.sidebar.markdown("---")


auto_update = st.sidebar.toggle("🔄 Chế độ tự cập nhật", value=True)
refresh_rate = st.sidebar.slider("Tốc độ làm mới (giây):", min_value=2, max_value=30, value=5)

if auto_update:
    st.sidebar.caption(f"🟢 Đang tự động quét và nạp dữ liệu mới sau mỗi **{refresh_rate} giây**.")
else:
    st.sidebar.caption("🔴 Đã tắt tự động cập nhật. Nhấn nút bên dưới để tải dữ liệu thủ công.")
    if st.sidebar.button("🔄 Tải lại dữ liệu ngay"):
        st.rerun()

st.sidebar.markdown("---")
st.sidebar.info(
    "💡 **Mẹo Demo Đồ Án:**\n"
    "Bật 'Tự cập nhật' khi muốn cho thầy cô thấy dữ liệu từ Kafka đổ về làm biểu đồ tự nhảy.\n"
    "Tắt đi khi cần đứng im màn hình để giải thích các thông số kỹ thuật."
)



if os.path.exists(HISTORY_FILE):
    df_history = pd.read_csv(HISTORY_FILE)
    df_station = df_history[df_history['Station_No'] == station_idx].copy()
else:
    df_station = pd.DataFrame()


latest_pred_data = None
if os.path.exists(PREDICT_FILE):
    try:
        with open(PREDICT_FILE, 'r', encoding='utf-8') as f:
            latest_pred_data = json.load(f)
    except Exception:
        pass


if not df_station.empty:
    latest_row = df_station.iloc[-1]
    st.success(f"📌 Dữ liệu thực tế mới nhất nhận được lúc: **{latest_row['date']}** tại **{selected_station}**")
    
    # Khối hiển thị nhanh chỉ số môi trường hiện tại
    m1, m2, m3, m4 = st.columns(4)
    with m1: st.metric(label="Nồng độ PM2.5", value=f"{latest_row['PM2.5']:.2f} µg/m³")
    with m2: st.metric(label="Nồng độ TSP", value=f"{latest_row['TSP']:.2f} µg/m³")
    with m3: st.metric(label="Nhiệt độ (Temp)", value=f"{int(latest_row['Temperature'])} °C")
    with m4: st.metric(label="Độ ẩm (Humidity)", value=f"{int(latest_row['Humidity'])} %")
        
    m5, m6, m7, m8 = st.columns(4)
    with m5: st.metric(label="Khí O3", value=f"{latest_row['O3']:.2f} ppb")
    with m6: 
        co_val = "N/A" if station_idx == 2 else f"{latest_row['CO']:.2f} ppb"
        st.metric(label="Khí CO", value=co_val)
    with m7: st.metric(label="Khí NO2", value=f"{latest_row['NO2']:.2f} ppb")
    with m8: st.metric(label="Khí SO2", value=f"{latest_row['SO2']:.2f} ppb")
else:
    st.warning(f"⚠️ Đang chờ `producer.py` kích hoạt để hứng dữ liệu lịch sử đầu vào...")

st.markdown("---")


st.header(f"📈 Xu Hướng Biến Động PM2.5 (Thực Tế Quá Khứ vs Dự Báo Tương Lai)")

station_key = f"station_{station_idx}"


has_predictions = latest_pred_data and "predictions" in latest_pred_data and station_key in latest_pred_data["predictions"]

if not df_station.empty:
   
    df_recent_actual = df_station.tail(24)
    actual_times = df_recent_actual['date'].tolist()
    actual_values = df_recent_actual['PM2.5'].tolist()
    
    fig = go.Figure()
    
   
    fig.add_trace(go.Scatter(
        x=actual_times,
        y=actual_values,
        mode='lines+markers',
        name='Dữ liệu Thực Tế (Quá khứ)',
        line=dict(color='#1f77b4', width=3), 
        marker=dict(size=5)
    ))
    
    
    if has_predictions:
        pred_list = latest_pred_data["predictions"][station_key]
        current_time_str = latest_pred_data["current_time"]
        
        try:
            base_time = datetime.strptime(current_time_str, "%d-%m-%Y %H:%M")
        except ValueError:
            base_time = datetime.now()
            
        future_labels = []
        for h in range(1, 25):
            future_time = base_time + pd.Timedelta(hours=h)
            future_labels.append(future_time.strftime("%d-%m-%Y %H:%M"))
            
        
        connect_time = [actual_times[-1]] + future_labels
        connect_values = [actual_values[-1]] + pred_list
        
        fig.add_trace(go.Scatter(
            x=connect_time,
            y=connect_values,
            mode='lines+markers',
            name='Dự Báo AI (24h Tiếp Theo)',
            line=dict(color='#FF4B4B', width=3, dash='dash'), 
            marker=dict(size=6, symbol='diamond')
        ))
        
        st.info(f"🔮 Mô hình AI vừa chạy dự báo dựa trên dữ liệu mốc: **{current_time_str}**")
    else:
        st.warning(f"⏳ Trạm này đang tích lũy dữ liệu nền. Chưa có file dự báo `{PREDICT_FILE}` hoặc chưa đủ 48h.")

  
    fig.update_layout(
        xaxis_title="Trục thời gian liên tục (Real-time Timeline)",
        yaxis_title="Nồng độ PM2.5 (µg/m³)",
        hovermode="x unified",
        margin=dict(l=40, r=40, t=20, b=40),
        height=500,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    
    if has_predictions:
        with st.expander("🔍 Xem bảng so sánh dữ liệu số chi tiết"):
            col_left, col_right = st.columns(2)
            with col_left:
                st.write("**📊 5 giờ thực tế gần nhất:**")
                st.dataframe(df_recent_actual[['date', 'PM2.5']].tail(5), use_container_width=True)
            with col_right:
                st.write("**🔮 5 giờ dự báo tương lai đầu tiên:**")
                df_future_show = pd.DataFrame({
                    "Thời Gian Tương Lai": future_labels[:5],
                    "PM2.5 Dự Đoán (µg/m³)": [round(v, 2) for v in pred_list[:5]]
                })
                st.dataframe(df_future_show, use_container_width=True)
else:
    st.info("💡 Đang chờ luồng dữ liệu khởi tạo để vẽ biểu đồ tổng hợp...")


if auto_update:
    time.sleep(refresh_rate)
    st.rerun()