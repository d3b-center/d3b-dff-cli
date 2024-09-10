import os
from setuptools import setup, find_packages
from d3b_dff_cli.version import __version__

root_dir = os.path.dirname(os.path.abspath(__file__))
req_file = os.path.join(root_dir, "requirements.txt")
with open(req_file) as f:
    requirements = f.read().splitlines()

setup(
    name='d3b-dff-cli',
    version=__version__,
    packages=find_packages(),
    include_package_data=True,
    entry_points={
        'console_scripts': [
            'd3b=d3b_dff_cli.cli:main',
        ],
    },
    install_requires=requirements,
    python_requires='>=3.8',
    author='Xiaoyan Huang',
    author_email='huangx@chop.edu',
    description='D3B CLI.',
    license='Apache License 2.0',
    classifiers=[
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3.8',
        "Operating System :: OS Independent",
    ],
    project_urls={
        'Source': 'https://github.com/d3b-center/d3b-dff-cli',
    },
)
