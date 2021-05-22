from setuptools import setup

with open("README.md") as f:
    readme = f.read()

setup(
    name="asqlite",
    author="Rapptz",
    url="https://github.com/Rapptz/asqlite/",
    project_urls={
        "Issue Tracker": "https://github.com/Rapptz/asqlite/issues/",
    },
    version="1.0.0",
    packages=["asqlite"],
    license="MIT",
    description="A simple and easy to use async wrapper for sqlite3.",
    long_description=readme,
    long_description_content_type="text/markdown",
    include_data_package=True,
    python_requires=">=3.5.3",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Intended Audience :: Developers",
        "Natural Language :: English",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ]
)
