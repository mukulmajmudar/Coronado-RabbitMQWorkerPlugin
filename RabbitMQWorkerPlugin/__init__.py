import time
import logging

import tornado.concurrent
import WorkerPlugin
import RabbitMQPlugin

from .SimpleClient import SimpleClient

logger = logging.getLogger(__name__)

# pylint: disable=abstract-method
class Config(WorkerPlugin.Config, RabbitMQPlugin.Config):

    def __init__(self, keys=None): 
        if keys is None:
            keys = []
        super().__init__(
        [
            'rmqWorkerRequestQName',
            'rmqWorkerResponseQName'
        ] + keys)


    def _getRmqWorkerRequestQName(self):
        return 'rmqWorkerRequestQ'

    def _getRmqWorkerResponseQName(self):
        return 'rmqWorkerResponseQ'


class AppPlugin(WorkerPlugin.AppPlugin):

    def getId(self):
        return 'rabbitmqWorkerPlugin'

    def getProducerClass(self):
        return Producer

    def getConsumerClass(self):
        return Consumer


class Producer(WorkerPlugin.Producer):

    def __init__(self, context):
        # Call parent
        super().__init__(context['ioloop'])

        self._requestQueueName = context['rmqWorkerRequestQName']
        self._responseQueueName = context['rmqWorkerResponseQName']
        self._shutdownDelay = context['workerShutdownDelay']

        # Create a client
        self._client = SimpleClient(
                host=context['rmqHost'],
                port=context['rmqPort'],
                messageHandler=self.onMessage,
                ioloop=context['ioloop'])


    def start(self):
        self._client.setup(
                self._requestQueueName, self._responseQueueName)

        # Start consuming from the response queue
        return self._client.startConsuming(self._responseQueueName)


    def destroy(self):
        future = tornado.concurrent.Future()
        def onStopped(stopFuture):
            try:
                stopFuture.result()
            finally:
                def disconnect():
                    disconnFuture = self._client.disconnect()
                    def onDisconnected(disconnFuture):
                        try:
                            disconnFuture.result()
                        finally:
                            future.set_result(None)

                    self._ioloop.add_future(disconnFuture, onDisconnected)

                # Disconnect after a delay
                logger.info('Producer will be stopped in %d seconds',
                        self._shutdownDelay)
                self._ioloop.add_timeout(time.time() + self._shutdownDelay,
                        disconnect)

        self._ioloop.add_future(self._client.stopConsuming(), onStopped)

        return future


    # pylint: disable=too-many-arguments
    def _request(self, requestId, tag, body, contentType, contentEncoding):
        '''
        Publish a request to a worker.
        '''

        return self._client.publish(
                queueName=self._requestQueueName,
                messageType=tag,
                body=body,
                contentType=contentType,
                contentEncoding=contentEncoding,
                correlationId=requestId,
                persistent=True,
                replyTo=self._responseQueueName)


    def onMessage(self, properties, body):
        # Pass to parent
        self._onResponse(properties.correlation_id, body,
                properties.content_type, properties.content_encoding)


    _requestQueueName = None
    _responseQueueName = None
    _client = None
    _shutdownDelay = None


class Consumer(WorkerPlugin.Consumer):

    def __init__(self, workHandlers, context):
        # Call parent
        super().__init__(workHandlers, context['ioloop'])

        self._requestQueueName = context['rmqWorkerRequestQName']
        self._shutdownDelay = context['workerShutdownDelay']

        # Create a client
        self._client = SimpleClient(
                host=context['rmqHost'],
                port=context['rmqPort'],
                messageHandler=self.onMessage,
                ioloop=context['ioloop'])


    def start(self):
        self._client.setup(self._requestQueueName)

        # Start consuming from the request queue
        return self._client.startConsuming(self._requestQueueName)


    def destroy(self):
        future = tornado.concurrent.Future()
        def onStopped(stopFuture):
            try:
                stopFuture.result()
            finally:
                def disconnect():
                    disconnFuture = self._client.disconnect()
                    def onDisconnected(disconnFuture):
                        try:
                            disconnFuture.result()
                        finally:
                            future.set_result(None)

                    self._ioloop.add_future(disconnFuture, onDisconnected)

                # Disconnect after a delay
                logger.info('Consumer will be stopped in %d seconds',
                        self._shutdownDelay)
                self._ioloop.add_timeout(time.time() + self._shutdownDelay,
                        disconnect)

        self._ioloop.add_future(self._client.stopConsuming(), onStopped)

        return future


    # pylint: disable=too-many-arguments
    def respond(self, requestId, replyTo, body, contentType, contentEncoding):
        '''
        Publish a message to the response queue.
        '''
        return self._client.publish(
                queueName=replyTo,
                messageType=None,
                body=body,
                contentType=contentType,
                contentEncoding=contentEncoding,
                correlationId=requestId,
                persistent=True)


    def onMessage(self, properties, body):
        # Pass to parent
        self._onRequest(
                requestId=properties.correlation_id,
                tag=properties.type,
                body=body,
                contentType=properties.content_type,
                contentEncoding=properties.content_encoding,
                replyTo=properties.reply_to)


    _requestQueueName = None
    _client = None
    _shutdownDelay = None
