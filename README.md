
# **Apache Beam Data Processing Pipeline**

This project implements an **Apache Beam** pipeline for processing CSV transaction data. The pipeline reads data from a source (such as Google Cloud Storage), processes it, and outputs the results in a structured format (JSON). The pipeline supports transformations like filtering, aggregation, and conversion.

## **Table of Contents**

- [Project Overview](#project-overview)
- [Getting Started](#getting-started)
- [Pipeline Overview](#pipeline-overview)
- [Dependencies](#dependencies)
- [Testing](#testing)
- [Running the Pipeline](#running-the-pipeline)
- [License](#license)

---

## **Project Overview**

This project processes transaction data using **Apache Beam**, a unified model for defining both batch and streaming data-parallel processing pipelines. The pipeline performs the following tasks:

1. **Parse CSV**: Reads and parses CSV lines into dictionaries.
2. **Filter Transactions**: Filters out transactions based on custom rules (e.g., amount less than a threshold or date before a certain year).
3. **Aggregate by Date**: Groups transactions by date and calculates the sum of amounts for each date.
4. **Convert to JSON**: Outputs the final results as JSON strings.

## **Getting Started**

### **Prerequisites**

1. **Apache Beam**: This project uses the Apache Beam SDK for Python. You will need to install it first to Run this project.
2. **Python 3.7+**: Ensure that Python 3.7 or higher is installed.
3. **Google Cloud SDK**: If you're working with Google Cloud Storage (GCS), you’ll need to authenticate using **Application Default Credentials**.

### **Clone the Repository**

Clone the repository to your local machine:

```bash
git https://github.com/Muhammad234871/apache-beam-pipeline.git
cd apache-beam-pipeline
```

### **Create a Virtual Environment**

It is recommended to use a **virtual environment** to manage dependencies.

```bash
python3 -m venv beam_env
source beam_env/bin/activate  # On Windows use `beam_env\Scriptsctivate`
```

### **Install Dependencies**

Install the required Python dependencies listed in the `requirements.txt` file.

```bash
pip install -r requirements.txt
```

---

## **Pipeline Overview**

The **Beam Pipeline** consists of the following transformations:

1. **Parse CSV**:
   - Parses CSV input lines into dictionaries, mapping fields like `date`, `origin`, `destination`, and `transaction_amount`.

2. **Extract Transaction**:
   - Filters transactions based on conditions, such as `transaction_amount > 20` and `date >= 2010`.

3. **Group and Sum Transactions**:
   - Groups transactions by **date** and sums the `transaction_amount` for each date.

4. **Output Results as JSON**:
   - Converts the final results to JSON format for output.

Here’s a simplified flow of the pipeline:

```
Create Input -> Parse CSV -> Extract Transaction -> Group by Date -> Sum Transactions -> Convert to JSON -> Write to Output
```

---

## **Dependencies**

This project relies on the following dependencies:

- **Apache Beam**: For building and running data pipelines.
- **Google Cloud SDK** (optional): If you're using GCS for input/output.
- **pytest**: For unit testing the pipeline.
- **json**: For handling JSON serialization.

Install the required dependencies with:

```bash
pip install apache-beam[interactive] pytest google-cloud-storage
```

---

## **Testing**

To ensure the pipeline behaves as expected, unit tests have been implemented. The tests cover different parts of the pipeline, ensuring that the CSV parsing, filtering, and aggregation work correctly.

### **Run Unit Tests**

You can run the tests using **pytest**:

```bash
pytest -q  # Runs the tests in quiet mode
```

#### **Test Pipeline**:
- A custom **`ProcessTransactions`** composite transform has been tested using **Apache Beam's `TestPipeline`**.
- The tests include assertions to check that the transformed data matches the expected results.

---

## **Running the Pipeline**

### **Run the Pipeline Locally**

To run the pipeline locally, use the **DirectRunner**, which executes the pipeline on your local machine.

```bash
python3 -m beamapp.main --runner DirectRunner --output output/results.jsonl.gz
```

This command will process the data from the input CSV, apply the transformations, and write the results to the **`output/results.jsonl.gz`** file.

### **Run the Pipeline on Google Cloud Dataflow (Optional)**

If you'd like to run the pipeline on **Google Cloud Dataflow**, use the **DataflowRunner**.

```bash
python3 -m beamapp.main --runner DataflowRunner     --project <YOUR_PROJECT_ID>     --staging_location gs://<YOUR_BUCKET>/staging/     --temp_location gs://<YOUR_BUCKET>/temp/     --region <YOUR_REGION>     --output gs://<YOUR_BUCKET>/results.jsonl.gz
```

Replace `<YOUR_PROJECT_ID>` and `<YOUR_BUCKET>` with your Google Cloud project and storage bucket names.

---

## **License**

This project is licensed under the **MIT License** - see the [LICENSE](LICENSE) file for details.

---

### **Additional Information**:
- **Logging**: Logs are generated throughout the pipeline for debugging and monitoring purposes.
- **Performance**: The pipeline supports scaling with **Google Cloud Dataflow** for larger datasets.
