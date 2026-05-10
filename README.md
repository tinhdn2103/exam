# Ad Performance Aggregator

A high-performance CLI application designed to process and aggregate large advertising datasets (~1GB). It calculates key performance indicators (CTR, CPA) and identifies top-performing campaigns.

## 🛠️ Setup Instructions

### Prerequisites
- **Python**: 3.9 or higher
- **Docker** (Optional, for containerized execution)

### Installation
1. Clone the repository to your local machine.
2. Navigate to the project directory:
   ```bash
   cd fv-sec-001-software-engineer-challenge
   ```
3. Install the required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## 💻 How to Run the Program

### Running via Python CLI
Standard execution using the Python interpreter:
```bash
python aggregator.py --input ad_data.csv/ad_data.csv --output results/
```
*Note: If your input file is in a different location, update the `--input` path accordingly.*

### Running via Docker
1. **Build the image**:
   ```bash
   docker build -t ad-aggregator .
   ```
2. **Run the container** (using CMD syntax):
   ```cmd
   docker run --rm ^
     -v "%cd%\ad_data.csv:/data" ^
     -v "%cd%\results:/app/results" ^
     ad-aggregator --input /data/ad_data.csv --output /app/results
   ```

## 📚 Libraries Used

- **Pandas (>= 2.0.0)**: Used for high-performance, vectorized data manipulation and CSV chunk processing.
- **NumPy (>= 1.24.0)**: Used for efficient numerical operations and handling missing data (NaN).
- **Pytest**: Framework used for unit testing and verifying processing logic.
- **Argparse**: Built-in module for handling command-line arguments.
- **Tracemalloc**: Built-in module used to monitor and report peak memory usage.

## ⚡ Performance Results (1GB File)

The following metrics were measured while processing a 1GB CSV file containing over **26 million records** (approx. 26,843,544 rows):

| Metric | Result |
|--------|--------|
| **Processing Time** | **6.36 seconds** (Multiprocessing) |
| **Peak Memory (Main Process)** | **0.60 MB** |
| **Throughput** | **~4.2 million rows/sec** |

*Measurements taken on an 8-core system processing a 1GB CSV file (26.8M rows). Multiprocessing significantly reduced processing time from ~38s to ~6s.*

---
*Developed as part of the Software Engineer Challenge.*
