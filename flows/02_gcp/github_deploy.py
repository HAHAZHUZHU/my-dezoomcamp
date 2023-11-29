from prefect.deployments import Deployment
from homework2_q4 import etl_web_to_gcs
from prefect.filesystems import GitHub

github_block = GitHub.load("dezoom")

# create a deployment
github_block.get_directory("flows") # specify a subfolder of repo
github_block.save("dev")

