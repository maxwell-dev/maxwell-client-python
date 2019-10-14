import setuptools
import subprocess

cmd = ['pip', 'install', '-r', 'requirements.txt']
p = subprocess.Popen(cmd, stdout=subprocess.PIPE)
for line in p.stdout:
    print(line)
p.wait()

with open("README.md", "r") as f:
    long_description = f.read()

setuptools.setup(
    name="maxwell_client",
    version="0.1.0",
    author="Xu Chaoqian",
    author_email="chaoranxu@gmail.com",
    description="The maxwell client implementation for python.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/maxwell-dev/maxwell-client-python",
    packages=setuptools.find_packages(),
    classifiers=(
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    )
)
