
import time
import json
import pandas as pd
from kafka import KafkaProducer

# ==========================================
# CẤU HÌNH CÁC THÔNG SỐ HỆ THỐNG
# ==========================================
# Cứ 3 giây gửi dữ liệu của 1 mốc giờ
# 1. Khởi tạo Kafka Producer
# 2. Đọc file dữ liệu stream đã được làm sạch và đồng bộ
# 3. Gửi tin nhắn bọc gói lên Kafka
# 4. Chờ 3 giây trước khi giả lập giờ tiếp theo

KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'air_quality'
CSV_FILE_PATH = 'D:\VisualStudio\IE212\DoAn\streaming\data\simulation_stream.csv'
DELAY_TIME = 1/2  

def json_serializer(data):
    """ Hàm mã hóa dữ liệu dict thành chuỗi bytes JSON để gửi lên Kafka """
    return json.dumps(data).encode('utf-8')

def run_producer():
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=json_serializer
        )
        print(f"[*] Kết nối thành công tới Kafka Broker: {KAFKA_BROKER}")
    except Exception as e:
        print(f"[!] LỖI: Không thể kết nối tới Kafka Broker. Hãy chắc chắn Kafka đang chạy!")
        print(f"Chi tiết lỗi: {e}")
        return

    
    try:
        print(f"[*] Đang tải dữ liệu stream từ: {CSV_FILE_PATH}...")
        df = pd.read_csv(CSV_FILE_PATH)
    except FileNotFoundError:
        print(f"[!] LỖI: Không tìm thấy file {CSV_FILE_PATH}. Hãy chạy file tạo dữ liệu sạch trước!")
        return

    grouped = df.groupby('date', sort=False)
    
    total_hours = len(grouped)
    print(f"[*] Tìm thấy {total_hours} mốc giờ hợp lệ để tiến hành giả lập streaming.")
    print(f"[!] Bắt đầu truyền dữ liệu (Tốc độ: 1 mốc giờ / {DELAY_TIME} giây)...")
    print("[*] Ấn Ctrl + C nếu muốn DỪNG luồng stream.\n")
    print("-" * 60)

    count = 0
    for date_str, group_df in grouped:
        count += 1
        
        
        message = {
            "date": str(date_str)
        }
        
        
        for _, row in group_df.iterrows():
            station_id = int(row['Station_No'])
            station_key = f"station_{station_id}"
            
            
            message[station_key] = {
                "TSP": float(row['TSP']),
                "PM2.5": float(row['PM2.5']),
                "O3": float(row['O3']),
                "CO": float(row['CO']),  
                "NO2": float(row['NO2']),
                "SO2": float(row['SO2']),
                "Temperature": float(row['Temperature']),
                "Humidity": float(row['Humidity'])
            }
            
        
        try:
            producer.send(TOPIC_NAME, value=message)
            print(f"[--> SEND][{count}/{total_hours}] Gửi thành công data mốc giờ: {date_str}")
        except Exception as e:
            print(f"[!] Lỗi khi gửi dữ liệu tại mốc {date_str}: {e}")
            
        # 4. Chờ 3 giây trước khi giả lập giờ tiếp theo
        time.sleep(DELAY_TIME)

    print("-" * 60)
    print("[+] Hoàn thành truyền toàn bộ tập dữ liệu stream!")

if __name__ == '__main__':
    run_producer()