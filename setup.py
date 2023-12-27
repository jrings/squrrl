from setuptools import find_packages, setup

setup(
    name="squrrl",
    version="0.1",
    packages=find_packages(),
    author="Joerg Rings",
    author_email="mail@rings.de",
    description="A serendipitous book recommender",
    url="https://github.com/jrings/squrrl",
    package_dir={"": "src"},
)
