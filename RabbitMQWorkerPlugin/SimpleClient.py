from contextlib import closing
import logging
import functools

import pika
from pika.adapters import TornadoConnection
from pika.spec import BasicProperties
import tornado.concurrent
from tornado.ioloop import IOLoop

# Logger for this module
logger = logging.getLogger(__name__)

def when(*args, **kwargs):
    '''
    Deprecated.

    A JQuery-like "when" function to gather futures and deal with
    maybe-futures. Copied from previous version of Coronado.
    '''
    # If ioloop not given, use the current one
    ioloop = kwargs.get('ioloop', IOLoop.current())

    makeTuple = kwargs.get('makeTuple', False)

    future = tornado.concurrent.Future()
    numDone = [0]
    numFutures = len(args)
    result = [None] * numFutures

    def onFutureDone(index, f):
        result[index] = f
        numDone[0] += 1
        if numDone[0] == numFutures:
            if numFutures > 1 or makeTuple:
                future.set_result(tuple(result))
            else:
                tornado.concurrent.chain_future(f, future)

    index = 0
    for maybeFuture in args:
        if isinstance(maybeFuture, tornado.concurrent.Future):
            ioloop.add_future(maybeFuture,
                    functools.partial(onFutureDone, index))
        elif isinstance(maybeFuture, Exception):
            # Make a future with the exception set to the argument
            f = tornado.concurrent.Future()
            f.set_exception(maybeFuture)
            onFutureDone(index, f)
        else:
            # Make a future with the result set to the argument
            f = tornado.concurrent.Future()
            f.set_result(maybeFuture)
            onFutureDone(index, f)
        index += 1

    if numFutures == 0:
        future.set_result(tuple())

    return future

def transform(future, callback, ioloop=None):
    '''
    Deprecated.

    A future transformer. Similar to JQuery's "deferred.then()". Copied
    from previous version of Coronado.
    '''
    if ioloop is None:
        ioloop = IOLoop.current()
    newFuture = tornado.concurrent.Future()

    def onFutureDone(future):
        try:
            transformedValue = callback(future)
        except Exception as e:  # pylint: disable=broad-except
            transformedValue = e
        nextFuture = when(transformedValue)
        tornado.concurrent.chain_future(nextFuture, newFuture)

    ioloop.add_future(future, onFutureDone)
    return newFuture

class SimpleClient(object):
    '''
    A simplified RabbitMQ client.

    This client always uses the default exchange and queue-name-based bindings,
    useful for simple cases where RabbitMQ's complete messaging model is not
    required (e.g. work queue implementation).
    '''

    def __init__(self, host, port, messageHandler=None, ioloop=None):
        self._host = host
        self._port = port
        self._messageHandler = messageHandler
        self._ioloop = ioloop is not None and ioloop or IOLoop.current()
        self._connected = False


    def setup(self, *queueNames):
        # Use a blocking connection to declare the app's queues
        params = pika.ConnectionParameters(host=self._host, port=self._port)
        with closing(pika.BlockingConnection(params)) as connection:
            with closing(connection.channel()) as channel:
                # Declare durable queues; we will use the
                # default exchange for simple tag-based routing
                for queueName in queueNames:
                    logger.info('Declaring RabbitMQ queue %s', queueName)
                    channel.queue_declare(queue=queueName, durable=True)
                    logger.info('Declared RabbitMQ queue %s', queueName)


    # pylint: disable=too-many-arguments
    def declare(self, queueName, passive=False, durable=False,
            exclusive=False, auto_delete=False, nowait=False,
            arguments=None):
        declareFuture = tornado.concurrent.Future()

        def onQueueDeclared(methodFrame):   # pylint: disable=unused-argument
            logger.info('Declared RabbitMQ queue %s', queueName)
            declareFuture.set_result(None)

        # If already connected to RabbitMQ server, declare immediately
        if self._connected:
            logger.info('Declaring RabbitMQ queue %s', queueName)
            self._channel.queue_declare(onQueueDeclared, queueName,
                    passive=passive, durable=durable, exclusive=exclusive,
                    auto_delete=auto_delete, nowait=nowait, arguments=arguments)
            return

        #
        # Not connected, so connect and then declare
        #

        def onConnected(connectFuture):
            try:
                # Trap connection exceptions if any
                connectFuture.result()
            except Exception as e:  # pylint: disable=broad-except
                declareFuture.set_exception(e)
            else:
                assert self._connected

                logger.info('Declaring RabbitMQ queue %s', queueName)
                self._channel.queue_declare(onQueueDeclared, queueName,
                        passive=passive, durable=durable, exclusive=exclusive,
                        auto_delete=auto_delete, nowait=nowait,
                        arguments=arguments)

        self._ioloop.add_future(self.connect(), onConnected)
        return declareFuture


    def deleteQueues(self, queueNames):
        # Use a blocking connection to delete the app's queues
        params = pika.ConnectionParameters(host=self._host, port=self._port)
        with closing(pika.BlockingConnection(params)) as connection:
            with closing(connection.channel()) as channel:
                # Delete durable queues; we will use the
                # default exchange for simple tag-based routing
                for queueName in queueNames:
                    logger.info('Deleting RabbitMQ queue %s', queueName)
                    channel.queue_delete(queue=queueName)
                    logger.info('Deleted RabbitMQ queue %s', queueName)


    def connect(self):
        logger.info('Connecting to RabbitMQ server')

        if self._connectFuture is not None:
            return self._connectFuture

        # Make connection
        self._connectFuture = tornado.concurrent.Future()
        params = pika.ConnectionParameters(host=self._host, port=self._port)
        self._connection = TornadoConnection(params,
                on_open_callback=self._onConnected,
                on_open_error_callback=self._onConnectError,
                on_close_callback=self._onConnectionClosed,
                custom_ioloop=self._ioloop)

        return self._connectFuture


    def disconnect(self):
        if not self._connected:
            return
        self._disconnectFuture = tornado.concurrent.Future()
        self._connection.close()
        return self._disconnectFuture


    def publish(self, queueName, messageType, body, contentType,
            contentEncoding, correlationId, persistent, replyTo=None):
        # If already connected to RabbitMQ server, publish immediately
        if self._connected:
            self._publish(
                    queueName=queueName,
                    messageType=messageType,
                    body=body,
                    contentType=contentType,
                    contentEncoding=contentEncoding,
                    correlationId=correlationId,
                    persistent=persistent,
                    replyTo=replyTo)
            return

        #
        # Not connected, so connect and then publish
        #

        queueFuture = tornado.concurrent.Future()

        def onConnected(connectFuture):
            try:
                # Trap connection exceptions, if any
                connectFuture.result()
            except Exception as e:  # pylint: disable=broad-except
                queueFuture.set_exception(e)
            else:
                assert self._connected

                # Connected, so publish
                self._publish(
                        queueName=queueName,
                        messageType=messageType,
                        body=body,
                        contentType=contentType,
                        contentEncoding=contentEncoding,
                        correlationId=correlationId,
                        persistent=persistent,
                        replyTo=replyTo)

                # Resolve the future
                queueFuture.set_result(None)

        self._ioloop.add_future(self.connect(), onConnected)

        return queueFuture


    def startConsuming(self, queueName):
        # Connect, then start consuming
        def onConnected(connectFuture):
            connectFuture.result()
            assert self._connected

            # Add on-cancel callback
            def onCancel(frame):    # pylint: disable=unused-argument
                self._channel.close()
            self._channel.add_on_cancel_callback(onCancel)

            # Start consuming
            self._consumerTag = self._channel.basic_consume(
                    self._onMessage, queueName)
            logger.info('Started consuming from queue %s', queueName)

        return transform(self.connect(), onConnected, ioloop=self._ioloop)


    def stopConsuming(self):
        logger.info('Stopping RabbitMQ consumer')
        stopFuture = tornado.concurrent.Future()
        def onCanceled(unused): # pylint: disable=unused-argument
            logger.info('Canceled RabbitMQ consumer')
            stopFuture.set_result(None)
        self._channel.basic_cancel(onCanceled, self._consumerTag)
        return stopFuture


    def _publish(self, queueName, messageType, body, contentType,
            contentEncoding, correlationId, persistent, replyTo):

        # Define properties
        properties = BasicProperties(
                content_type=contentType,
                content_encoding=contentEncoding,
                type=messageType,
                delivery_mode=persistent and 2 or None,
                correlation_id=correlationId,
                reply_to=replyTo)

        # Publish to RabbitMQ server
        self._channel.basic_publish(exchange='',
                routing_key=queueName, body=body,
                properties=properties)


    def _onConnected(self, connection):
        # Open a channel in the connection
        self._channel = connection.channel(self._onChannel)


    def _onConnectError(self):
        self._connectFuture.set_exception(ConnectionError())
        self._connectFuture = None


    # pylint: disable=unused-argument
    def _onConnectionClosed(self, connection, replyCode, replyText):
        logger.info('RabbitMQ server connection closed')
        self._connected = False
        if self._disconnectFuture is not None:
            self._disconnectFuture.set_result(None)
            self._disconnectFuture = None


    def _onChannel(self, channel):
        # Add channel-close callback
        channel.add_on_close_callback(self._onChannelClosed)
        self._connected = True
        logger.info('Connected to RabbitMQ server')
        self._connectFuture.set_result(None)
        self._connectFuture = None


    # pylint: disable=unused-argument
    def _onChannelClosed(self, channel, replyCode, replyText):
        self._connected = False
        logger.info('RabbitMQ channel closed')
        self._connection.close()


    # pylint: disable=unused-argument
    def _onMessage(self, channel, basicDeliver, properties, body):
        logger.info('Message received (may be partial): %s', body[0:50])
        logger.debug('Message body (may be partial): %s', body[0:1000])
        self._messageHandler(properties, body)

        # Acknowledge message
        self._channel.basic_ack(basicDeliver.delivery_tag)


    _host = None
    _port = None
    _messageHandler = None
    _ioloop = None
    _connected = None
    _connectFuture = None
    _connection = None
    _channel = None
    _consumerTag = None
    _disconnectFuture = None
