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
        "numpy",
        "flask",
        "amqpstorm~=2.7.2",
        "apscheduler~=3.6.3",
        "certifi~=2020.4.5.1",
        "chardet~=3.0.4",
        "idna~=2.9",
        "pamqp~=2.3.0",
        "psycopg2-binary~=2.8.5",
        "pytz~=2020.1",
        "pyyaml>=5.3.1,<5.5.0",
        "requests~=2.28.1",
        "six~=1.15.0",
        "sqlalchemy~=1.3.17",
        "tzlocal~=2.1",
        "urllib3>=1.25.9,<1.27.0",
        "python-dateutil",
        "event_count_logger",
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
