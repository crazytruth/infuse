from setuptools import setup, find_packages

import infuse

def readme():
    with open('README.rst') as f:
        return f.read()

setup(
    name='infuse',
    version=infuse.__version__,
    description='circuit breaker implementation for async storage',
    long_description=readme(),
    classifiers=[
    'Development Status :: 3 - Alpha',
    'License :: OSI Approved :: MIT License',
    'Programming Language :: Python :: 3.6',
    ],
    keywords='circuit breaker async asyncio ',
    url='https://github.com/MyMusicTaste/infuse',
    author='crazytruth',
    author_email='kwangjinkim@gmail.com',
    license='MIT',
    packages=find_packages(exclude=['contrib', 'docs', 'tests*']),
    include_package_data=True,
    zip_safe=False
)
