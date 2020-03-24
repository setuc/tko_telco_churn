from utils.cmlapi import CMLApi
from IPython.display import Javascript, HTML
import time
import os


# "https://ml-44322529-cb3.eng-ml-l.vnu8-sqze.cloudera.site"
HOST = os.getenv("CDSW_API_URL").split(
    ":")[0] + "://" + os.getenv("CDSW_DOMAIN")
USERNAME = os.getenv("CDSW_PROJECT_URL").split("/")[6]  # "jfletch"
API_KEY = os.getenv("CDSW_API_KEY")    # erdfUKIlsd..
PROJECT_NAME = os.getenv("CDSW_PROJECT")   # "refractor"


# Instantiate API Wrapper
cml = CMLApi(HOST, USERNAME, API_KEY, PROJECT_NAME)

# Get User Details
user_details = cml.get_user({})
user_obj = {"id": user_details["id"], "username": "vdibia",
            "name": user_details["name"],
            "type": user_details["type"],
            "html_url": user_details["html_url"],
            "url": user_details["url"]
            }

# Get Project Details
project_details = cml.get_project({})
project_id = project_details["id"]


# Create Job
create_jobs_params = {"name": "Train Model " + str(int(time.time())),
                      "type": "manual",
                      "script": "3_train_models.py",
                      "timezone": "America/Los_Angeles",
                      "environment": {},
                      "kernel": "python3",
                      "cpu": 1,
                      "memory": 2,
                      "nvidia_gpu": 0,
                      "include_logs": True,
                      "notifications": [
    {"user_id": user_obj["id"],
     "user":  user_obj,
     "success": False, "failure": False, "timeout": False, "stopped": False
     }
],
    "recipients": {},
    "attachments": [],
    "include_logs": True,
    "report_attachments": [],
    "success_recipients": [],
    "failure_recipients": [],
    "timeout_recipients": [],
    "stopped_recipients": []
}

new_job = cml.create_job(create_jobs_params)
new_job_id = new_job["id"]
print("Created new job with jobid", new_job_id)

##
# Start a job
job_env_params = {}
start_job_params = {"environment": job_env_params}
job_id = new_job_id
job_status = cml.start_job(job_id, start_job_params)
print("Job started")

# Stop a job
#job_dict = cml.start_job(job_id, start_job_params)
#cml.stop_job(job_id, start_job_params)

# Get Default Engine Details
default_engine_details = cml.default_engine({})
default_engine_image_id = default_engine_details["id"]

# Create Model
example_model_input = {"StreamingTV": "No", "MonthlyCharges": 70.35, "PhoneService": "No", "PaperlessBilling": "No", "Partner": "No", "OnlineBackup": "No", "gender": "Female", "Contract": "Month-to-month", "TotalCharges": 1397.475,
                       "StreamingMovies": "No", "DeviceProtection": "No", "PaymentMethod": "Bank transfer (automatic)", "tenure": 29, "Dependents": "No", "OnlineSecurity": "No", "MultipleLines": "No", "InternetService": "DSL", "SeniorCitizen": "No", "TechSupport": "No"}

create_model_params = {
    "projectId": project_id,
    "name": "Model Explainer " + str(int(time.time())),
    "description": "Explain a given model prediction",
    "visibility": "private",
    "targetFilePath": "4_model_serve_explainer.py",
    "targetFunctionName": "explain",
    "engineImageId": default_engine_image_id,
    "kernel": "python3",
    "examples": [
        {
            "request": example_model_input,
            "response": {}
        }],
    "cpuMillicores": 1000,
    "memoryMb": 2048,
    "nvidiaGPUs": 0,
    "replicationPolicy": {"type": "fixed", "numReplicas": 1},
    "environment": {}}

new_model_details = cml.create_model(create_model_params)
access_key = new_model_details["accessKey"]  # todo check for bad response
print("New model created with access key", access_key)

# Create Application
create_application_params = {
    "name": "Explainer App",
    "subdomain": str(int(time.time())),
    "description": "Explainer web application",
    "type": "manual",
    "script": "5_application.py", "environment": {},
    "kernel": "python3", "cpu": 1, "memory": 2,
    "nvidia_gpu": 0
}

new_application_details = cml.create_application(create_application_params)
application_url = new_application_details["url"]

print("Application may need a few minutes to finish deploying. Open link below in about a minute ..")
print("Aplication created, deploying at ", application_url)
HTML("<a href='{}'>Open Application UI</a>".format(application_url))
