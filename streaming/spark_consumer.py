
import os
import json
import joblib
import numpy as np
import pandas as pd
import torch
import torch.nn as nn
import torch.nn.functional as F

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# ============================================================================
# 1. ĐỊNH NGHĨA KIẾN TRÚC MẠNG AI (BẮT BUỘC CÓ ĐỂ PYTORCH LOAD MODEL)
# 2. CẤU HÌNH ĐƯỜNG DẪN HỆ THỐNG
# 3. HÀM XỬ LÝ MICRO-BATCH DỮ LIỆU REAL-TIME (FOREACH BATCH)
# 4. KHỞI TẠO SPARK STRUCTURED STREAMING LUỒNG CHÍNH
# ============================================================================
class LocalAttention(nn.Module):
    def __init__(self, hidden_size, window_size=3):
        super().__init__()
        self.window_size = window_size
        self.W_q = nn.Linear(hidden_size, hidden_size, bias=False)
        self.W_k = nn.Linear(hidden_size, hidden_size, bias=False)
        self.W_v = nn.Linear(hidden_size, hidden_size, bias=False)
        self.scale = hidden_size ** 0.5
        self.out_proj = nn.Linear(hidden_size, hidden_size)

    def forward(self, x):
        B, T, H = x.size()
        w = self.window_size
        Q = self.W_q(x)
        K = self.W_k(x)
        V = self.W_v(x)

        pad = w // 2
        K_pad = F.pad(K.permute(0, 2, 1), (pad, pad), mode='replicate').permute(0, 2, 1)
        V_pad = F.pad(V.permute(0, 2, 1), (pad, pad), mode='replicate').permute(0, 2, 1)

        out = []
        for t in range(T):
            k_local = K_pad[:, t : t + w, :]
            v_local = V_pad[:, t : t + w, :]
            q_t     = Q[:, t, :].unsqueeze(1)
            score = torch.bmm(q_t, k_local.transpose(1, 2)) / self.scale
            attn  = F.softmax(score, dim=-1)
            context = torch.bmm(attn, v_local)
            out.append(context)

        out = torch.cat(out, dim=1)
        return self.out_proj(out)

class MultiScaleBlock(nn.Module):
    def __init__(self, hidden_size, window_sizes=(3, 6, 12)):
        super().__init__()
        self.attentions = nn.ModuleList([LocalAttention(hidden_size, ws) for ws in window_sizes])
        self.fusion = nn.Linear(hidden_size * len(window_sizes), hidden_size)
        self.norm   = nn.LayerNorm(hidden_size)

    def forward(self, x):
        outs = [attn(x) for attn in self.attentions]
        concat = torch.cat(outs, dim=-1)
        fused  = self.fusion(concat)
        return self.norm(fused + x)

class BiLSTM_MLAM(nn.Module):
    def __init__(self, input_size, hidden_size=64, output_size=24, num_layers=2, window_sizes=(3, 6, 12), dropout=0.2):
        super().__init__()
        self.bilstm  = nn.LSTM(
            input_size=input_size,
            hidden_size=hidden_size,
            num_layers=num_layers,
            batch_first=True,
            bidirectional=True,
            dropout=dropout,
        )
        bilstm_output_dim = hidden_size * 2
        self.mlam = MultiScaleBlock(bilstm_output_dim, window_sizes)
        self.dropout = nn.Dropout(dropout)
        self.fc      = nn.Linear(bilstm_output_dim, output_size)

    def forward(self, x):
        bilstm_out, _ = self.bilstm(x)
        mlam_out = self.mlam(bilstm_out)
        feat_vector = mlam_out[:, -1, :]
        feat_vector = self.dropout(feat_vector)
        output = self.fc(feat_vector)
        return output

# ============================================================================
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "air_quality"
MODEL_DIR = r"D:\VisualStudio\IE212\DoAn\streaming\models"
HISTORY_FILE = "data/streaming_history.csv"
PREDICT_FILE = "data/latest_predictions.json"


BASE_FEATURES = ['PM2.5', 'TSP', 'O3', 'CO', 'NO2', 'SO2', 'Temperature', 'Humidity']

MODELS = {}
SCALERS = {}

def load_ai_resources():
    """ Hàm nạp toàn bộ 6 mô hình và 12 file bộ mã hóa Scaler lên bộ nhớ RAM """
    global MODELS, SCALERS
    if len(MODELS) == 6:  
        return

    print("\n[*] Đang tiến hành nạp các file Model (.pt) và Scaler (.pkl) của 6 trạm...")
    for i in range(1, 7):
        
        input_size = 7 if i == 2 else 8
        
       
        model = BiLSTM_MLAM(input_size=input_size, hidden_size=64, output_size=24)
        model_path = os.path.join(MODEL_DIR, f"model_48_24_station{i}.pt")
        
        
        if os.path.exists(model_path):
            try:
                
                checkpoint = torch.load(model_path, map_location='cpu', weights_only=False)
                
               
                if isinstance(checkpoint, dict):
                    if 'model_state_dict' in checkpoint:
                        model.load_state_dict(checkpoint['model_state_dict'])
                    elif 'state_dict' in checkpoint:
                        model.load_state_dict(checkpoint['state_dict'])
                    else:
                        model.load_state_dict(checkpoint)
                else:
                    model = checkpoint
                
              
                model.eval()
                MODELS[i] = model
                
            except Exception as e:
                print(f"[!] Lỗi nghiêm trọng không thể giải mã tệp model trạm {i}: {e}")
        else:
            print(f"[!] CẢNH BÁO: Không tìm thấy file model tại {model_path}")

       
        scaler_x_path = os.path.join(MODEL_DIR, f"scaler_X_station_{i}.pkl")
        scaler_y_path = os.path.join(MODEL_DIR, f"scaler_y_station_{i}.pkl")
        
        if os.path.exists(scaler_x_path) and os.path.exists(scaler_y_path):
            SCALERS[i] = {
                "X": joblib.load(scaler_x_path),
                "y": joblib.load(scaler_y_path)
            }
        else:
            print(f"[!] CẢNH BÁO: Thiếu file Scaler của trạm {i}!")
            
    print("[+] Tải toàn bộ tài nguyên AI thành công!\n")

# ============================================================================

def process_batch(batch_df, batch_id):
    if batch_df.count() == 0:
        return

    
    load_ai_resources()

   
    records = batch_df.collect()
    new_rows = []

    for row in records:
        date_str = row['date']
        for i in range(1, 7):
            station_key = f"station_{i}"
            if row[station_key] is not None:
                st_data = row[station_key]
                new_rows.append({
                    "date": date_str,
                    "Station_No": i,
                    "PM2.5": float(st_data["PM2.5"]),
                    "TSP": float(st_data["TSP"]),
                    "O3": float(st_data["O3"]),
                    "CO": float(st_data["CO"]),
                    "NO2": float(st_data["NO2"]),
                    "SO2": float(st_data["SO2"]),
                    "Temperature": float(st_data["Temperature"]),
                    "Humidity": float(st_data["Humidity"])
                })

    if not new_rows:
        return

    
    new_batch_df = pd.DataFrame(new_rows)
    os.makedirs("data", exist_ok=True)
    
    if os.path.exists(HISTORY_FILE):
        new_batch_df.to_csv(HISTORY_FILE, mode='a', header=False, index=False)
    else:
        new_batch_df.to_csv(HISTORY_FILE, mode='w', header=True, index=False)

   
    full_history = pd.read_csv(HISTORY_FILE)
    
    
    latest_predictions = {
        "current_time": new_rows[-1]["date"],
        "predictions": {}
    }


    for i in range(1, 7):
        st_history = full_history[full_history['Station_No'] == i].copy()
        
      
        if len(st_history) < 48:
            print(f"[-] Trạm {i} chưa tích lũy đủ 48h dữ liệu để dự báo (Hiện tại: {len(st_history)}/48h)")
            continue

       
        input_seq = st_history.tail(48).copy()
        
        
        features_list = list(BASE_FEATURES)
        if i == 2:
            features_list.remove('CO')

        X_raw = input_seq[features_list].values  

       
        scaler_X = SCALERS[i]["X"]
        scaler_y = SCALERS[i]["y"]

        X_scaled = scaler_X.transform(X_raw)
        X_tensor = torch.tensor(X_scaled, dtype=torch.float32).unsqueeze(0) 
        with torch.no_grad():
            y_pred_scaled = MODELS[i](X_tensor) 

     
        y_pred_real = scaler_y.inverse_transform(y_pred_scaled.numpy())[0]

        
        latest_predictions["predictions"][f"station_{i}"] = [float(val) for val in y_pred_real]

  
    if latest_predictions["predictions"]:
        with open(PREDICT_FILE, "w", encoding="utf-8") as f:
            json.dump(latest_predictions, f, ensure_ascii=False, indent=4)
        print(f"[==> FORECAST] Đã tính toán xong và cập nhật dự báo 24 giờ tiếp theo cho mốc: {latest_predictions['current_time']}")


# ============================================================================

if __name__ == '__main__':
    print("[*] Đang khởi động hệ thống Spark Structured Streaming...")
    

    spark = SparkSession.builder \
        .appName("AirQualityStreamingConsumer") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")

   
    station_fields = StructType([
        StructField("TSP", DoubleType(), True),
        StructField("PM2.5", DoubleType(), True),
        StructField("O3", DoubleType(), True),
        StructField("CO", DoubleType(), True),
        StructField("NO2", DoubleType(), True),
        StructField("SO2", DoubleType(), True),
        StructField("Temperature", DoubleType(), True),
        StructField("Humidity", DoubleType(), True)
    ])

    kafka_json_schema = StructType([
        StructField("date", StringType(), True),
        StructField("station_1", station_fields, True),
        StructField("station_2", station_fields, True),
        StructField("station_3", station_fields, True),
        StructField("station_4", station_fields, True),
        StructField("station_5", station_fields, True),
        StructField("station_6", station_fields, True)
    ])

    
    raw_kafka_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", TOPIC_NAME) \
        .load()

   
    parsed_stream = raw_kafka_stream \
        .selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), kafka_json_schema).alias("data")) \
        .select("data.*")

    
    query = parsed_stream.writeStream \
        .foreachBatch(process_batch) \
        .start()

    print("[+] Luồng Spark Streaming lắng nghe Kafka đã kích hoạt thành công!")
    print("[*] Đang chờ luồng dữ liệu đổ về từ file producer.py...")
    query.awaitTermination()