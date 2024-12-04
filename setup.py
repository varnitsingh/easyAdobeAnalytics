from setuptools import setup, find_packages

setup(
    name="easyAdobeAnalytics",
    version="1.0.1",
    author="Varnit Singh",
    author_email="",
    description="This is an attempt at a usable python library to query report data from Adobe Analytics 1.4 API.",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/varnitsingh/easyAdobeAnalytics",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.10",
    install_requires=[
        "pandas>=2.2.3",
        "requests>=2.32.3",
    ],
    project_urls={
        "Homepage": "https://github.com/varnitsingh/easyAdobeAnalytics",
        "Issues": "https://github.com/varnitsingh/easyAdobeAnalytics/issues",
    },
)
