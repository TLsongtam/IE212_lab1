# Air Quality Forecasting using BiLSTM-MLAM

> **Note:** This README was generated with the assistance of AI based on the project source code and project report, then reviewed and edited by the author.

A Big Data course project for forecasting **PM2.5 concentration** in Ho Chi Minh City using the **BiLSTM-MLAM** deep learning model. The project also provides a real-time prediction pipeline built with **Apache Kafka**, **Apache Spark Streaming**, **MongoDB**, and **Streamlit**.

---

## Pipeline

<p align="center">
  <img src="images/Pipeline.png" alt="Project Pipeline" width="900">
</p>

---

## Project Structure

```text
DoAn/
├── Data/                     # Dataset and sliding-window data
├── images/                   # Figures and pipeline images
├── models/                   # Trained models
├── streaming/                # Real-time prediction system
│   ├── data/
│   ├── models/
│   ├── producer.py
│   ├── spark_consumer.py
│   ├── spark_consumer_mongodb.py
│   ├── app_dashboard.py
│   └── app_dashboard_mongodb.py
├── data_preprocess.ipynb      # Data preprocessing
├── producer.py
├── spark_consumer.py
├── spark_consumer_mongodb.py
├── app_dashboard.py
├── app_dashboard_mongodb.py
└── README.md
```

---

## Installation

### Clone repository

```bash
git clone <repository-url>
cd DoAn
```

### Install dependencies

```bash
pip install -r requirements.txt
```

### Run Streaming Pipeline

Start Kafka and MongoDB, then run:

```bash
python producer.py
```

```bash
python spark_consumer.py
```

Finally, launch the dashboard:

```bash
streamlit run app_dashboard.py
```

---

## Results

- Forecast PM2.5 concentration for **6 monitoring stations** in Ho Chi Minh City.
- Compare **BiLSTM** and **BiLSTM-MLAM** using **48→24** and **72→24** sliding-window settings.
- Evaluate model performance with:
  - RMSE
  - MAE
  - MAPE
- Support **real-time prediction** through Kafka + Spark Streaming with visualization on Streamlit.
