from setuptools import setup

setup(
    name='RabbitMQWorkerPlugin',
    version='1.0',
    packages=['RabbitMQWorkerPlugin'],
    install_requires=
    [
        'Coronado',
        'pika==0.12.0',
        'WorkerPlugin'
    ],
    author='Mukul Majmudar',
    author_email='mukul@curecompanion.com',
    description='RabbitMQ-based worker plugin for Coronado')
