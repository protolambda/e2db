from setuptools import setup, find_packages

with open("README.md", "rt", encoding="utf8") as f:
    readme = f.read()

setup(
    name="e2db",
    description="E2DB - index Eth2 data",
    version="0.0.1",
    long_description=readme,
    long_description_content_type="text/markdown",
    author="protolambda",
    author_email="proto+pip@protolambda.com",
    url="https://github.com/protolambda/e2db",
    python_requires=">=3.8, <4",
    license="MIT",
    packages=find_packages(),
    tests_require=[],
    extras_require={
        "testing": ["pytest"],
        "linting": ["flake8"],
    },
    install_requires=[
        "eth2>=0.0.5",
        "remerkleable>=0.1.16",
        "eth2spec==0.12.1",
        "eth2fastspec==0.0.3",
        "py_ecc==4.0.0",
        "trio>=0.15.0,<0.16.0",
        "httpx>=0.12.1,<0.13.0",
        "eth_typing>=2.2.1",
        "eth-utils>=1.8.4",
        "web3>=5.7.0",
        "sqlalchemy>=1.3.16",
        "lru-dict==1.1.6",
        "sqlalchemy_mate>=0.0.10",
    ],
    include_package_data=True,
    keywords=["eth2", "ethereum", "serenity", "beacon", "database", "index", "sql"],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Natural Language :: English",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: Implementation :: PyPy",
        "Operating System :: OS Independent",
    ],
)
