import setuptools

#with open("README.md", "r") as fh:
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
    # TODO include other files as well
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Development Status :: 3 - Alpha",
    ],
    python_requires='>=3.6',
)
