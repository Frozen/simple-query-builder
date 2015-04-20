from setuptools import setup, find_packages

setup(
    name='simple-query-builder',
    version='0.0.1',
    description='Query builder',
    # long_description=open("README.rst").read(),
    author='Potapov Konstantin',
    # author_email='@gmail.com',
    license="MIT",
    url='https://github.com/Frozen/simple-query-builder',
    packages=find_packages(exclude=["tests.*", "tests"]),
    classifiers=[
        "Development Status :: Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
    ]
)