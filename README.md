# 💰 Real-Time Liquidity Forecasting for Small Businesses

Forecast your company’s future cash position in real time using Plaid data, Google Cloud, and Looker.

---

## 📌 Project Summary
Small businesses struggle with visibility into future liquidity. This project builds a real-time pipeline that ingests transaction data via Plaid, processes it with GCP tools, and visualizes projected balances using Looker dashboards.

---

## 🧠 Key Features
- **Plaid API Integration**: Simulated bank data ingestion
- **GCP Pipeline**: Pub/Sub → Cloud Functions → BigQuery
- **Modeling Layer**: Modular DBT transformations (see [DBT repo](https://github.com/abhaypadmanabhan/dataengineering1))
- **Visualization**: Looker dashboard for burn rate, cash runway, and liquidity insights

---

## ⚙️ Tech Stack
- **APIs & Ingestion:** Plaid, Pub/Sub, Cloud Functions
- **Storage & Transformation:** BigQuery, [DBT → See Repo](https://github.com/abhaypadmanabhan/dataengineering1)
- **Visualization:** Looker
- **Other Tools:** Git, GitHub, Vertex AI (future scope)

---

## 📈 KPIs Tracked
- Projected Liquidity Runway
- Daily Cash Position
- Receivables & Payables Forecast
- Net Burn Rate

---

## 🎯 Business Impact
> Enables small businesses and treasury teams to identify liquidity risks 30+ days in advance, improving financial decision-making.

---

## 🧪 Demo & Setup
> *This repo contains the ingestion and pipeline code. For DBT model setup, see [this repo](https://github.com/abhaypadmanabhan/dataengineering1).*

---

## 🎥 Project Walkthrough
Loom video coming soon...
