from setuptools import find_packages, setup
from typing import List

REQUIREMENT_FILE_NAME = "requirements.txt"
HYPHON_E_DOT = "-e ."

def get_requirement()->List[str]:
    requirement_list = []
    with open("requirements.txt") as req_obj:
        requirement_list = req_obj.readlines()
    requirement_list = [requirement_name.replace("\n","") for requirement_name in requirement_list]

    if HYPHON_E_DOT in requirement_list:
        requirement_list.remove(HYPHON_E_DOT)
    return requirement_list

setup(
    name = "Amazon_Listing_Project",
    version= "0.0.1",
    author= "Ricky Mehra",
    author_email ='Rickymehra299@gmail.com',
    packages=find_packages(),
    install_requires = get_requirement())