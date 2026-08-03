# Real-Time Log Monitor using Apache Spark

A real-time log monitoring project that demonstrates how streaming log data can be generated, processed, and monitored using **Apache Spark Streaming** and a web-based interface. The project focuses on real-time data ingestion, stream processing, and visualization of application logs to support faster monitoring and debugging.

---

## 📌 Project Objective

The objective of this project is to build a real-time log monitoring pipeline capable of:

* Generating/reading application logs
* Continuously processing incoming log data
* Detecting and analyzing log events in real time
* Streaming processed information to a web interface
* Providing immediate visibility into application behavior

---

## 🛠️ Technology Stack

| Technology          | Purpose                                    |
| ------------------- | ------------------------------------------ |
| **Python**          | Log generation and backend processing      |
| **Apache Spark**    | Real-time stream processing                |
| **Spark Streaming** | Continuous processing of incoming log data |
| **JavaScript**      | Front-end interaction and live updates     |
| **HTML/CSS**        | Web-based monitoring interface             |
| **Node.js**         | Backend/web server functionality           |
| **GitHub**          | Version control and documentation          |

---

## 🔄 System Architecture

```text
                Log Generator
                     │
                     ▼
                Log File
                     │
                     ▼
          Apache Spark Streaming
                     │
                     ▼
            Stream Processing
                     │
                     ▼
              Backend Server
                     │
                     ▼
             Web Interface
                     │
                     ▼
          Real-Time Log Monitor
```

The system continuously generates or receives log events, processes the incoming stream using Apache Spark, and exposes the resulting information through a web interface.

---

## 📂 Project Structure

```text
real-time-log-monitor/
│
├── app.js
├── log_generator.py
├── spark_stream.py
├── application.log
├── package.json
└── README.md
```

> Update this structure if additional files are added to the project.

---

## ⚙️ Core Components

### 1. Log Generator

The log generator produces application log events that simulate continuously changing system activity.

The generated logs can be used to demonstrate events such as:

* Informational messages
* Warnings
* Errors
* Application activity

---

### 2. Spark Streaming

Apache Spark is used to process the incoming log stream.

The streaming component demonstrates:

* Continuous data ingestion
* Stream processing
* Real-time transformation of log data
* Processing of continuously arriving events

This forms the core data-processing layer of the project.

---

### 3. Backend Server

The backend provides the communication layer between the processed log data and the front-end monitoring interface.

It enables the browser to receive updated log information without requiring manual refreshes.

---

### 4. Web Interface

The web interface provides a live view of incoming log events.

The purpose of the interface is to provide quick visibility into:

* Incoming log activity
* Application events
* Warnings and errors
* Real-time system behavior

---

## 🔍 Key Concepts Demonstrated

This project demonstrates practical understanding of:

* Real-time data streaming
* Apache Spark Streaming
* Log ingestion
* Stream processing
* Backend/frontend integration
* Event monitoring
* Real-time data visualization
* Distributed data-processing concepts

---

## 🚀 How the Pipeline Works

```text
1. Generate application logs
             ↓
2. Write logs to the log source
             ↓
3. Spark Streaming monitors incoming data
             ↓
4. Spark processes the incoming stream
             ↓
5. Processed events are exposed by backend
             ↓
6. Browser displays the events in real time
```

---

## 🎯 Use Cases

A real-time log monitoring system can be useful for:

* Application monitoring
* Debugging
* Error detection
* System observability
* Infrastructure monitoring
* Operational support

The project demonstrates concepts relevant to **platform services, technical support, data engineering, and system monitoring** workflows.

---

## 📈 Potential Improvements

Future versions could include:

* Log-level filtering (`INFO`, `WARNING`, `ERROR`)
* Search functionality
* Error/event counters
* Timestamp-based filtering
* Log aggregation
* Real-time charts
* Alert generation for critical errors
* Persistent storage for historical logs
* Kafka integration as a streaming source
* Dockerized deployment
* Cloud-based deployment

---

## 📚 Learning Objectives

This project was developed to strengthen practical knowledge of:

* Apache Spark
* Real-time stream processing
* Python programming
* Backend/frontend communication
* Log monitoring
* Data streaming architectures
* Event-driven systems

---

## ⚠️ Project Scope

This project is primarily intended as a **learning and demonstration project** for real-time streaming and monitoring concepts. It is not intended to replace production-grade observability platforms such as enterprise log management or monitoring systems.

---

## 👤 Author

**ShivkarEXE**

Computer Science & Engineering Student
