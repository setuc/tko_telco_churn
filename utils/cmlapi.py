import requests
import json
import logging


class CMLApi:

    def __init__(self, host, username, api_key, project_name, log_level=logging.INFO):
        self.host = host
        self.username = username
        self.api_key = api_key
        self.project_name = project_name
        logging.basicConfig(level=log_level)

        logging.debug("Api Initiated")

    def get_user(self, params):
        get_user_endpoint = "/".join([self.host, "api/v1/users",
                                      self.username])
        res = requests.get(
            get_user_endpoint,
            headers={"Content-Type": "application/json"},
            auth=(self.api_key, ""),
            data=json.dumps(params)
        )

        response = res.json()
        if (res.status_code != 200):
            logging.error(response["message"])
            logging.error(response)
        else:
            logging.debug("User details retrieved")

        return response

    def get_project(self, params):
        get_project_endpoint = "/".join([self.host, "api/v1/projects",
                                         self.username, self.project_name])
        res = requests.get(
            get_project_endpoint,
            headers={"Content-Type": "application/json"},
            auth=(self.api_key, ""),
            data=json.dumps(params)
        )

        response = res.json()
        if (res.status_code != 200):
            logging.error(response["message"])
            logging.error(response)
        else:
            logging.debug("Project details retrieved")

        return response

    def get_jobs(self, params):
        create_job_endpoint = "/".join([self.host, "api/v1/projects",
                                        self.username, self.project_name, "jobs"])
        res = requests.get(
            create_job_endpoint,
            headers={"Content-Type": "application/json"},
            auth=(self.api_key, ""),
            data=json.dumps(params)
        )

        response = res.json()
        if (res.status_code != 201):
            logging.error(response["message"])
            logging.error(response)
        else:
            logging.debug("List of jobs retrieved")

        return response

    def create_job(self, params):
        create_job_endpoint = "/".join([self.host, "api/v1/projects",
                                        self.username, self.project_name, "jobs"])
        res = requests.post(
            create_job_endpoint,
            headers={"Content-Type": "application/json"},
            auth=(self.api_key, ""),
            data=json.dumps(params)
        )

        response = res.json()
        if (res.status_code != 201):
            logging.error(response["message"])
            logging.error(response)
        else:
            logging.debug("Job created")
        return response

    def start_job(self, job_id, params):
        start_job_endpoint = "/".join([self.host, "api/v1/projects",
                                       self.username, self.project_name, "jobs", str(job_id), "start"])
        res = requests.post(
            start_job_endpoint,
            headers={"Content-Type": "application/json"},
            auth=(self.api_key, ""),
            data=json.dumps(params)
        )

        response = res.json()
        if (res.status_code != 200):
            logging.error(response["message"])
            logging.error(response)
        else:
            logging.debug(">> Job started")

        return response

    def stop_job(self, job_id, params):
        stop_job_endpoint = "/".join([self.host, "api/v1/projects",
                                      self.username, self.project_name, "jobs", str(job_id), "stop"])
        res = requests.post(
            stop_job_endpoint,
            headers={"Content-Type": "application/json"},
            auth=(self.api_key, ""),
            data=json.dumps(params)
        )

        response = res.json()
        logging.error(response)
        if (res.status_code != 200):
            logging.error(response["message"])
            logging.error(response)
        else:
            logging.debug(">> Job stopped")

        return response

    def create_model(self, params):
        create_model_endpoint = "/".join([self.host,
                                          "api/altus-ds-1", "models", "create-model"])
        res = requests.post(
            create_model_endpoint,
            headers={"Content-Type": "application/json"},
            auth=(self.api_key, ""),
            data=json.dumps(params)
        )

        response = res.json()
        if (res.status_code != 200):
            logging.error(response["message"])
            logging.error(response)
        else:
            logging.debug(">> Model created")

        return response

    def create_application(self, params):
        create_application_endpoint = "/".join([self.host, "api/v1/projects",
                                                self.username, self.project_name, "applications"])
        res = requests.post(
            create_application_endpoint,
            headers={"Content-Type": "application/json"},
            auth=(self.api_key, ""),
            data=json.dumps(params)
        )

        response = res.json()
        if (res.status_code != 201):
            logging.error(response["message"])
            logging.error(response)
        else:
            logging.debug(">> Application created")

        return response
