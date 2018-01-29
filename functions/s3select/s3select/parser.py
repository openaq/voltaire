from struct import unpack
from collections import namedtuple
from binascii import crc32

from .exceptions import ParserError

FrameHeader = namedtuple('FrameHeader', [
    'version', 'frame_type', 'payload_length', 'checksum'])
FramePayload = namedtuple('FramePayload', ['data', 'offset', 'checksum'])
Frame = namedtuple('Frame', ['header', 'payload'])

BYTE_FORMAT = '!I'
FRAME_TYPE_MAP = {
    8388609: 'record',
    8388610: 'exception',
    8388611: 'stats',
    8388612: 'continuation',
    8388613: 'end'
}


class FrameParser(object):
    def yield_frames(self, stream):
        while True:
            frame = self._parse_frame(stream)
            if frame is None:
                break
            yield frame
            if frame.header.frame_type == 'end':
                break

    def _parse_frame(self, stream):
        parsed_header = self._parse_header(stream)
        if not parsed_header:
            return None
        parsed_payload = self._parse_payload(stream, parsed_header)
        return Frame(parsed_header, parsed_payload)

    def _parse_header(self, stream):
        header = stream.read(12)
        if not header:
            return None

        # When grabbing a single byte, python will auto convert it to an int.
        version = header[0]

        # Pad the type with an empty byte because unpack requires at least
        # four bytes, but the type section is only three bytes.
        frame_type = self._unpack(b'\x00' + header[1:4])
        frame_type = FRAME_TYPE_MAP.get(frame_type, frame_type)

        payload_length = self._unpack(header[4:8])
        header_checksum = self._unpack(header[8:12])
        self._validate_checksum(header[:8], header_checksum)
        return FrameHeader(
            version, frame_type, payload_length, header_checksum
        )

    def _validate_checksum(self, data, checksum):
        computed_checksum = crc32(data) & 0xFFFFFFFF
        if checksum != computed_checksum:
            raise ParserError(
                'Checksum mismatch: expected %s but received %s' % (
                    computed_checksum, checksum
                )
            )

    def _parse_payload(self, stream, header):
        if header.payload_length == 0:
            return FramePayload(None, None, None)

        payload = stream.read(header.payload_length)
        payload_checksum = self._unpack(stream.read(4))
        self._validate_checksum(payload, payload_checksum)

        offset = None
        if header.frame_type in ['record', 'stats', 'continuation']:
            offset = payload[:8]
            payload = payload[8:]

        if not payload:
            payload = None

        return FramePayload(payload, offset, payload_checksum)

    def _unpack(self, binary_data):
        return unpack(BYTE_FORMAT, binary_data)[0]
