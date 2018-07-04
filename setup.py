from setuptools import setup, find_packages

version = '0.1.2'

def readme():
    with open('README.rst') as f:
        return f.read()


def pytest_command():
    from commands.pytest import PyTestCommand
    return PyTestCommand


test_requires = ['pytest',
                 'mock',
                 # 'fakeredis',
                 'pytest-asyncio',
                 'pytest-redis',
                 'pytest-cov',
                 'pytest-sugar',
                 'pytest-sanic']

setup(
    name='infuse',
    version=version,
    description='async circuit breaker implementation for async storages',
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
    license='BSD',
    packages=find_packages(exclude=['contrib', 'docs', 'tests*']),
    setup_requires=["zest.releaser[recommended]", "setuptools"],
    install_requires=["aioredis>=1.1.0"],
    include_package_data=True,
    zip_safe=False,
    tests_require=test_requires,
    test_suite='tests',
    extras_require={
        "testing": test_requires,
        "dev": ["zest.releaser[recommended]", "flake8"]
    },
    cmdclass={'test': pytest_command()}
)
