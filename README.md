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

- **Psutil**: Used to monitor accurate peak memory usage across multiple processes.
- **Pytest**: Framework used for unit testing and verifying processing logic.
- **Argparse**: Built-in module for handling command-line arguments.
- **Tracemalloc**: Built-in module used to monitor Python-specific memory allocations.
- **CSV (Built-in)**: Used for high-performance, memory-efficient streaming data processing.

## ⚡ Performance Results (1GB File)

The following metrics were measured while processing a 1GB CSV file containing over **26 million records** (approx. 26,843,544 rows):

| Metric | Result |
|--------|--------|
| **Processing Time** | **~12.84 seconds** (Multiprocessing) |
| **Peak Memory (Total RSS)** | **~195 MB** |
| **Python Overhead** | **0.40 MB** |
| **Throughput** | **~2.1 million rows/sec** |

*Measurements taken on an 8-core system processing a 1GB CSV file. The application uses a pure streaming approach to keep memory usage minimal while utilizing multiple processes for speed.*

---
*Developed as part of the Software Engineer Challenge.*
