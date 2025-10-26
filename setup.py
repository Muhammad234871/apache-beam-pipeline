from setuptools import setup, find_packages

setup(
    name="beamapp",
    version="1.0.0",
    description="Apache Beam package for Virgin Media O2",
    python_requires='>=3.12.0',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'apache-beam[gcp]>=2.68.0',
        'pytest>=8.4.2',
    ]
)