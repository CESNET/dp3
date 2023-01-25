import setuptools

# with open("README.md", "r") as fh:
#    long_description = fh.read()
long_description = "TODO"

setuptools.setup(
    name="dp3",
    version="0.1.0",
    author="Vaclav Bartos",
    author_email="bartos@cesnet.cz",
    description="Dynamic Profile Processing Platform",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/CESNET/dp3",
    packages=setuptools.find_packages(),
    install_requires=[
        "AMQPStorm~=2.7.2",
        "apscheduler~=3.6.3",
        "event-count-logger>=1.1",
        "Flask>=2.1.0",
        "numpy>=1.23.0",
        "psycopg2-binary~=2.9.5",
        "pydantic~=1.10",
        "pymongo~=4.3.3",
        "pyyaml>=5.3.1,<5.5.0",
        "requests~=2.28.1",
        "SQLAlchemy~=1.3.17",
    ],
    # TODO include other files as well
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Development Status :: 3 - Alpha",
    ],
    python_requires='~=3.9',
    scripts=[
        "bin/worker",
        "bin/updater",
        "bin/update_all",
        "api/receiver.py"
    ]
)
