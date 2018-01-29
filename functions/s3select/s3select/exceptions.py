class UnknownFrameTypeError(Exception):
    def __init__(self, frame_type):
        """Indicate that an unknown frame type was encountered.

        :type frame_type: int
        :param frame_type: The parsed frame type.
        """
        message = 'Unknown frame type encountered: %s' % frame_type
        super(UnknownFrameTypeError, self).__init__(message)


class ParserError(Exception):
    pass
