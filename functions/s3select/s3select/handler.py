import json

from .exceptions import UnknownFrameTypeError
from .parser import FrameParser


class ResponseHandler(object):
    def __init__(self, frame_parser=None):
        """Handle responding to an s3 streamed response.

        Example::

            class PrintingResponseHandler(ResponseHandler):
                def handle_records(self, record_data):
                    print(record_data.decode('utf-8')

                def handle_stats(self, stats_data):
                    print(stats_data)

                def handle_exceptions(self, exception_data):
                    print(exception_data)

            response = client.select_object(
                Bucket='my-bucket', Key='my-key.csv', SelectRequest={
                    'ObjectType': 'CSV',
                    'SqlQuery': 'SELECT * FROM S3Object',
                    'ObjectSerialization': {}
                }
            )

            handler = PrintingResponseHandler()
            handler.handle_response(response['Body'])

        :param frame_parser: an object that accepts a stream of data and
            yields raw parsed frames.
        """
        if frame_parser is None:
            frame_parser = FrameParser()
        self._parser = frame_parser

    def handle_response(self, stream):
        """Parse and handle a response stream.

        :param stream: The streaming response body from the call to
            select_object.
        """
        try:
            for frame in self._parser.yield_frames(stream):
                self._handle_frame(frame)
        finally:
            stream.close()

    def handle_records(self, record_data):
        """Handle record data.

        Each record is guaranteed to be be within a single chunk, but each
        chunk may have multiple records.

        :type record_data: bytes
        :param record_data: The raw bytes of the record(s) in this chunk.
        """
        raise NotImplementedError('handle_record()')

    def handle_exceptions(self, exception_data):
        """Handle exception data.

        This will be in the following format::

        {
            "Exceptions": [{
                "Code": "<ExceptionName>",
                "Offset": "<Record Offset>",
                "Message": "<Message>"
            }],
            "Counts": [{
                "Total": <int>,
                "<Record Exception Name 1>": <int>,
                "<Record Exception Name 2>": <int>,
                ...
            }]
        }

        :type exception_data: dict
        :param exception_data: A dictionary containing part of the exception
            data.
        """
        raise NotImplementedError('handle_exception()')

    def handle_stats(self, stats_data):
        """Handle query statistics.

        This will be in the following format::

        {
            "<Stat Name 1>": <int>,
            "<Stat Name 2>": <int>
        }

        :type stats_data: dict
        :param stats_data: A dictionary containing various request statistics.
        """
        raise NotImplementedError('handle_stats()')

    def _handle_frame(self, frame):
        handler_name = '_handle_%s_frame' % frame.header.frame_type
        handler = getattr(
            self, handler_name, self._handle_unknown_frame
        )
        handler(frame)

    def _handle_unknown_frame(self, frame):
        raise UnknownFrameTypeError(frame.header.frame_type)

    def _handle_record_frame(self, frame):
        self.handle_records(frame.payload.data)

    def _handle_exception_frame(self, frame):
        data = json.loads(frame.payload.data.decode('utf-8'))
        self.handle_exceptions(data)

    def _handle_stats_frame(self, frame):
        data = json.loads(frame.payload.data.decode('utf-8'))
        self.handle_stats(data)

    def _handle_continuation_frame(self, frame):
        pass

    def _handle_end_frame(self, frame):
        pass
