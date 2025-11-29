# Machine Learning section — walkthrough and fixes

I'll go block-by-block, then provide corrected code.

Original ML imports

import pandas as pd
from sklearn.utils import resample
from sklearn.utils import shuffle
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import confusion_matrix, classification_report

import warnings
import boto3
from sagemaker import get_execution_role

warnings.filterwarnings('ignore')

Notes

Fine imports. shuffle from sklearn.utils unused.

sagemaker.get_execution_role() only works if running inside a SageMaker notebook or if SDK can determine role — otherwise it raises.

Hiding warnings is convenient in notebook, but in production you may want to see them.



---

Data Loading

Original:

import numpy as np
import pandas as pd
from sklearn import datasets
import sagemaker
from sagemaker import get_execution_role
role = get_execution_role()

bucket_name = "loan-data202511291100"
folder_name = "loan_cleaned_data"
data_key = "loan_cleaned.csv"

data_location = f"s3://{bucket_name}/{folder_name}/{data_key}"

data = pd.read_csv(data_location)
data.head()

Issues & notes

pd.read_csv("s3://...") works if you have s3fs installed and AWS credentials configured (environment variables, AWS CLI, or IAM role). If not installed, pandas raises error. For robust behavior use s3fs or boto3 to download locally.

role = get_execution_role() will fail outside SageMaker. Use conditional or pass role as config.

No error handling for missing file.

No schema inference — Pandas will infer dtypes (can be slow for large files).


Better:

Use s3fs or boto3 to stream or download file.

For large files, prefer reading in chunks: pd.read_csv(..., chunksize=100000).


Example:

import s3fs
s3 = s3fs.S3FileSystem()  # requires s3fs installed and credentials
with s3.open(data_location, 'rb') as f:
    data = pd.read_csv(f)


---

Feature Engineering

Original:

data = pd.get_dummies(data, columns=["purpose"], dtype=int)
data.head()

Notes

get_dummies ok. If purpose has many categories, this can explode features — consider OneHotEncoder with sparse matrix or target encoding.

If dataset has ordering or cardinality issues, consider frequency thresholding.



---

Imbalanced data handling

Original:

print(data["not_fully_paid"].value_counts())

df_majority = data[data["not_fully_paid"]==0]
df_minority = data[data["not_fully_paid"]==1]

# Handle the imbalanced data using resample method and oversample the minority class.
df_minority_upsampled = resample(df_minority, replace=True, n_samples= df_majority.shape[0], random_state=42)

# Concatenate the upsampled data records with the majority class records and shuffle the resultant dataframe
df_balanced = pd.concat([df_majority, df_minority_unsampled])
print(df_balanced['not_fully_paid].value_counts())

Many bugs

df_minority_unsampled — undefined variable. Should be df_minority_upsampled.

print(df_balanced['not_fully_paid].value_counts()) — quoting error: 'not_fully_paid] missing closing quote.

Oversampling naive; better to use SMOTE (for numeric features) or use class_weight='balanced' in classifier.

After concat should call shuffle or sample(frac=1, random_state=...) to mix rows.


Corrected snippet

print(data["not_fully_paid"].value_counts())

df_majority = data[data["not_fully_paid"] == 0]
df_minority = data[data["not_fully_paid"] == 1]

df_minority_upsampled = resample(df_minority, replace=True, n_samples=df_majority.shape[0], random_state=42)

df_balanced = pd.concat([df_majority, df_minority_upsampled])
df_balanced = df_balanced.sample(frac=1, random_state=42).reset_index(drop=True)

print(df_balanced['not_fully_paid'].value_counts())

Better alternatives

imblearn.over_sampling.SMOTE() if features are numeric and you want synthetic examples.

Use RandomForestClassifier(class_weight='balanced') to avoid resampling.

Cross-validate with stratified folds.



---

Model training & evaluation

Original:

X = df_balanced.drop("not_fully_paid", axis=1)
y = df_balaced["not_fully_paid"]

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.4, random_state=42)

rf = RandomForestClassifier(random_state=42)
rf.fit(X_train, y_train)

y_pred = rf.predict(X_test)

print(classification_report(y_test, y_pred))

Issues

df_balaced typo; should be df_balanced.

Not using stratify=y in train_test_split — recommended for class distributions.

No scaling for numeric features — RandomForest doesn't need scaling but other models might.

No hyperparameter tuning or cross-validation — important for production quality.

Use classification_report and also ROC AUC and confusion matrix.


Corrected

X = df_balanced.drop("not_fully_paid", axis=1)
y = df_balanced["not_fully_paid"]

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.4, random_state=42, stratify=y)

rf = RandomForestClassifier(random_state=42, n_estimators=200, class_weight="balanced")
rf.fit(X_train, y_train)

y_pred = rf.predict(X_test)
print(classification_report(y_test, y_pred))

Improvements

Use GridSearchCV or RandomizedSearchCV with StratifiedKFold.

Track metrics: precision/recall for minority class, ROC-AUC, PR-AUC.



---

Saving model to S3

Original:

import boto3
import joblib
import tempfile

with tempfile.NamedTemporaryFile as tmp:
    joblib.dump(rf, tmp.name)
    tmp.flush()

    s3 = boto3.client('s3')
    s3.upload_file(tmp.name, bucket_name, "model.pkl")
    print("Model saved to s3 bucket", f"s3://{bucket_name}/model.pkl")

Issues & fixes

tempfile.NamedTemporaryFile context manager may delete file on close; on Windows that prevents re-open. Use delete=False to be safe.

with tempfile.NamedTemporaryFile() as tmp: returns an object whose .name is path — joblib.dump will write to it — but then file closed. Safer to call joblib.dump(rf, tmp.name) then upload.

boto3.client('s3') requires AWS credentials. On EC2/SageMaker, better to use instance role.

Alternative: joblib.dump(rf, 'model.pkl'); s3.upload_file('model.pkl', bucket, key) — but ensure cleanup.


Robust snippet

import joblib
import tempfile
import boto3
import os

s3 = boto3.client("s3")
with tempfile.NamedTemporaryFile(suffix=".pkl", delete=False) as tmp:
    tmp_path = tmp.name
try:
    joblib.dump(rf, tmp_path, compress=3)
    s3.upload_file(tmp_path, bucket_name, "models/model.pkl")
    print(f"Model saved to s3://{bucket_name}/models/model.pkl")
finally:
    os.remove(tmp_path)

Alternative: save model to S3 via sagemaker model APIs or boto3.put_object with buffer.


---

Full corrected, improved ML pipeline (compact)

import pandas as pd
import s3fs           # ensure installed in environment
from sklearn.model_selection import train_test_split, RandomizedSearchCV, StratifiedKFold
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, roc_auc_score
from imblearn.over_sampling import SMOTE
import joblib
import boto3

# Load
s3_path = f"s3://{bucket_name}/{folder_name}/{data_key}"
data = pd.read_csv(s3_path)  # requires s3fs and AWS creds

# Feature engineering
data = pd.get_dummies(data, columns=["purpose"], dtype=int)

# split features/labels
X = data.drop("not_fully_paid", axis=1)
y = data["not_fully_paid"]

# train-test split (stratify)
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.4, random_state=42, stratify=y)

# Option A: SMOTE on training set
sm = SMOTE(random_state=42)
X_train_res, y_train_res = sm.fit_resample(X_train, y_train)

# Train
rf = RandomForestClassifier(n_estimators=200, random_state=42, class_weight="balanced")
rf.fit(X_train_res, y_train_res)

# Evaluate
y_pred = rf.predict(X_test)
print(classification_report(y_test, y_pred))
print("ROC AUC:", roc_auc_score(y_test, rf.predict_proba(X_test)[:,1]))

# Save model to s3
import tempfile, os
s3 = boto3.client("s3")
tmp = tempfile.NamedTemporaryFile(suffix=".pkl", delete=False)
tmp.close()
joblib.dump(rf, tmp.name, compress=3)
s3.upload_file(tmp.name, bucket_name, "models/rf_model.pkl")
os.remove(tmp.name)

Notes:

Use SMOTE only if features are numeric and meaningful to interpolate.

Consider Pipeline (sklearn Pipeline) to manage transforms and model together.

Persist pipeline (preprocessing + model) so you can deploy easily.



---

4 — Operational & security recommendations

Security

Never hardcode credentials (Redshift, S3, DB passwords). Use:

IAM roles attached to EC2/EMR/SageMaker.

AWS Secrets Manager to store DB credentials and retrieve at runtime.

Environment variables with restricted access.


For Redshift: prefer COPY from S3 with IAM role instead of JDBC mass inserts.


Performance

Use Parquet for intermediate storage (columnar, faster).

Avoid coalesce(1) on large datasets.

Avoid .count() on huge DataFrames when possible.

Use partitioning (e.g., by year/month) when writing to S3.


Observability

Use structured logging. Integrate with CloudWatch.

Emit metrics like row counts, default rates, job duration.

Add DAG-level tracking (Airflow, Step Functions) and alerting.


Testing

Unit test transformation functions with small sample DataFrames (pytest + local SparkSession).

Add data quality checks (e.g., row counts, null percentages).


Deployment

Package ETL as a script or PySpark job. Use Airflow / Step Functions to schedule.

For ML, save full pipeline and version models (MLflow or SageMaker model registry).


Code hygiene

Use type hints, docstrings.

Replace print with Python logging.

Add exception handling and retries for transient network failures.



---

5 — Quick checklist / cheat-sheet (common bugs I fixed)

s3_inputs_path vs s3_input_path — fix variable name.

input_file vs input_df — use parameter consistently.

.drop() misuse — use dropna(), dropDuplicates() or .drop('col').

Filtering with "null" string — use isNotNull() and string equality checks after trimming.

Division safe: avoid divide-by-zero; use when(...).otherwise(...).

agg usage: use F.sum(...).alias("x"), then compute derived column with .withColumn.

round usage: F.round(col, 2) not round((col, 2)).

Typos in ML section: df_balaced, df_minority_unsampled, 'not_fully_paid] — fix.

Saving model: use delete=False temp file and remove after upload.