""" pykvstores setup module
"""

from setuptools import setup

with open("README.md", "r") as readme:
    long_description = readme.read()

setup(
    name="pykvstores",
    version="v0.1.0",
    license="MIT",
    author="Sanjay Kumar Srikakulam",
    maintainer="Sanjay Kumar Srikakulam",
    url="https://github.com/sanjaysrikakulam/pykvstores",
    description="Python Key-Value (kv) stores using different backends",
    long_description=long_description,
    long_description_content_type="text/markdown",
    keywords="key-value stores sharedmemory plasma store",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: Information Technology",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.8",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Operating System :: POSIX :: Linux",
        "Natural Language :: English",
    ],
    include_package_data=True,
    python_requires=">=3.8",
    package_dir={"": "."},
    packages=["pykvstores"],
    install_requires=[
        "lmdb==1.3.0",
        "pyarrow==7.0.0",
        "ray==1.11.0",
        "msgspec==0.5.0",
        "zstd==1.5.1.0",
        "psutil==5.9.0",
        "humanfriendly==10.0",
    ],
    zip_safe=False,
)
