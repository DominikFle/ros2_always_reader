import os
import subprocess
import sys
from setuptools import find_packages, setup  # noqa: D100

package_name = "ros2_always_reader"

def install_requirements():
    pass
    requirements_path = os.path.join(os.path.dirname(__file__), 'requirements.txt')

    if os.path.exists(requirements_path):
        result = subprocess.run(
            [ sys.executable, '-m','pip', 'install', '-r', requirements_path ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

        if result.returncode != 0:
            print()
            print(f"Error during recursive pip install: {result.stderr.decode()}")
            print()
            exit(result.returncode)
        else:
            pass

    else:
        pass

if os.getenv('PIP_INSTALL') == '1':
    install_requirements()
setup(
    name=package_name,
    version="0.0.0",
    packages=find_packages(exclude=["test"]),
    data_files=[
        ("share/ament_index/resource_index/packages", ["resource/" + package_name]),
        ("share/" + package_name, ["package.xml"]),
    ],
    install_requires=["setuptools","numpy"],
    zip_safe=True,
    maintainer="Dominik Fletschinger",
    maintainer_email="",
    description="Reader-only utilities for ROS2 bags with direct deserialization",
    license="TODO: License declaration",
    tests_require=["pytest"],
    entry_points={
        "console_scripts": [

        ],
    },
)
