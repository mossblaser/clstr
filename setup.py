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
    description="A quick'n'dirty cluster manager.",
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
