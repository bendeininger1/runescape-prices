from setuptools import setup, find_packages

setup(
    name="rs_project",
    version="0.0.4",
    description="This contains the code in the ./src directory of the project",
    author="Ben Deininger",
    packages=find_packages(where="./src"),
    package_dir={"":"./src"},
    install_requires=["setuptools"],
    entry_points={
        "packages":[
            "main=rs_project.main:main"
        ]
    }
)
