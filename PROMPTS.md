# PROMPTS.md

---

### Prompt 1: Initial Project Setup & Requirements Analysis
"I need to build a console application (CLI) in Python that processes a 1GB CSV file named `ad_data.csv`. 

The CSV schema is:
- `campaign_id` (string)
- `date` (YYYY-MM-DD)
- `impressions` (integer)
- `clicks` (integer)
- `spend` (float)
- `conversions` (integer)

The goal is to aggregate data by `campaign_id` and calculate:
1. `total_impressions`
2. `total_clicks`
3. `total_spend`
4. `total_conversions`
5. `CTR` = total_clicks / total_impressions
6. `CPA` = total_spend / total_conversions (if conversions > 0, otherwise null)

Since the file is 1GB, I need a memory-efficient approach (streaming or chunking). Can you help me set up the project structure and suggest the best approach for processing this large file in Python?"

---

### Prompt 2: Core Processing Logic (Memory Efficient)
"Let's implement the core aggregation logic using Python's `csv` module with a streaming approach to keep memory usage low. 

I want to:
- Iterate through the CSV rows.
- Use a dictionary to store aggregated values for each `campaign_id`.
- Ensure that I'm not loading the whole file into memory.
- Handle potential `ZeroDivisionError` for CTR and CPA calculations.

Please provide a function `process_csv(input_path)` that returns the aggregated data."

---

### Prompt 3: Sorting and Generating Results
"Now that we have the aggregated data, I need to generate two result lists:
A. Top 10 campaigns with the highest CTR.
B. Top 10 campaigns with the lowest CPA (excluding campaigns with 0 conversions).

The output should be saved as `top10_ctr.csv` and `top10_cpa.csv` with the following columns:
`campaign_id`, `total_impressions`, `total_clicks`, `total_spend`, `total_conversions`, `CTR`, `CPA`.

Please write the logic to sort the data and save these two files."

---

### Prompt 4: CLI Interface and Error Handling
"I want to make this a proper CLI application. 
- Use `argparse` to handle command-line arguments: `--input` (input CSV path) and `--output` (directory to save results).
- Add error handling for:
    - Missing input file.
    - Malformed CSV rows.
    - Permission issues when writing output.
- Add some logging or print statements to show progress (e.g., 'Processing started...', 'Completed in X seconds')."

---

### Prompt 5: Testing and Refinement
"Finally, I need to ensure the code is robust. 
- Please write unit tests using `pytest`. 
- Create a small mock CSV for testing.
- Test edge cases like:
    - Empty input file.
    - File with 0 conversions for some campaigns.
    - File with 0 impressions.
---

### Prompt 6: Dockerization
"I want to containerize this application to ensure it runs consistently across different environments. 
- Please create a `Dockerfile` for this Python application. 
- Use a lightweight base image (like `python:3.9-slim`).
- Include instructions on how to:
    1. Build the Docker image.
    2. Run the container, passing the input CSV file from the host machine and capturing the output results.
- Ensure the `README.md` is updated with these Docker instructions."
