from setuptools import setup

with open("clstr.py", "r") as f:
    for line in f:
        if line.startswith("__version__"):
            exec(line)

setup(
    name="clstr",
    version=__version__,
    py_modules=["clstr"],

    # Metadata for PyPi
    url="https://github.com/mossblaser/clstr",
    author="Jonathan Heathcote",
    author_email="mail@jhnet.co.uk",
    description="A quick'n'dirty cluster manager for quick'n'dirty clusters.",
    license="GPLv2",

    # Requirements
    install_requires=[],

    # Scripts
    entry_points={
        "console_scripts": [
            "clstr = clstr:main",
        ],
    }
)
