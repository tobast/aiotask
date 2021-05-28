import asyncio
import logging
import pickle
import uuid
from .task_manager import TaskManager
from . import constants

logger = logging.getLogger(__name__)


class TlvValue:
    """A TLV, to be read from or transmitted to the network.

    A TLV is of the form

    +--------+----------------+----------------+
    |Type(1B)| Size (2B)      | Payload...     |
    +--------+----------------+----------------+

    Where Type and Size are interpreted as unsigned, big-endian integer values, Type is
    a valid value for the enumeration `constants.TlvType`, and Size is the length in
    bytes of Payload."""

    class BadTlv(Exception):
        """ Raised when trying to read a badly formed TLV from a buffer """

    class NotEnoughData(Exception):
        """ Raised when trying to read a TLV from a buffer that lacks data """

    def __init__(self, typ: constants.TlvType, value: bytes):
        self.typ = typ
        self.value = value

    @property
    def weight(self):
        """ Weight of this TLV in total bytes to be transmitted """
        return 3 + len(self.value)

    def to_bytes(self):
        """ Export this TLV to Python bytes """
        assert 0 <= self.typ < (1 << 7)
        assert len(self.value) < (1 << 15)
        return (
            bytes([self.typ])
            + len(self.value).to_bytes(2, "big", signed=False)
            + self.value
        )

    def split_uuid(self):
        """ Split the value into UUID and remainder of the payload """
        if len(self.value) < 16:
            raise self.BadTlv("This TLV is too short to contain an UUID")
        tlv_uuid = uuid.UUID(bytes=self.value[:16])
        return tlv_uuid, self.value[16:]

    @classmethod
    def read_from_buffer(cls, buffer):
        """Read a TLV from buffer. Returns a tuple `(new_buffer, tlv)` where `tlv` is
        a `TlvValue`"""
        if len(buffer) < 3:
            raise cls.NotEnoughData("Less than 3 bytes to read.")
        try:
            typ = constants.TlvType(int(buffer[0]))
        except ValueError as exn:
            raise cls.BadTlv("Bad TLV type {}".format(int(buffer[0]))) from exn
        length = int.from_bytes(buffer[1:3], "big", signed=False)
        if len(buffer) - 3 < length:
            raise cls.NotEnoughData(
                "Buffer of size {} too small for a payload of size {}".format(
                    len(buffer), length
                )
            )
        payload = buffer[3 : 3 + length]

        return (buffer[3 + length :], cls(typ, payload))


class BaseProtocol(asyncio.Protocol):
    @property
    def _debug_id(self):
        return id(self) % 100

    def __init__(self, event_loop, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._event_loop = event_loop
        self._pending_data = b""

    def connection_made(self, transport):
        super().connection_made(transport)
        logger.debug(
            "[%d] Connected to %s", self._debug_id, transport.get_extra_info("peername")
        )
        self.transport = transport

    def connection_lost(self, exn):
        super().connection_lost(exn)
        logger.debug("[%d] Connection lost: %s", self._debug_id, exn)

    def data_received(self, data):
        super().data_received(data)
        logger.debug("[%d] Received data (%d)", self._debug_id, len(data))
        self._pending_data += data
        while self._pending_data:
            try:
                self._pending_data, tlv = TlvValue.read_from_buffer(self._pending_data)
                logger.debug("Received TLV %s of %d bytes", tlv.typ, len(tlv.value))
                self.tlv_received(tlv)
            except TlvValue.NotEnoughData:
                break  # Leave the remaining data for the next `data_received`
            except TlvValue.BadTlv as exn:
                logger.warning(
                    "Received badly-formed TLV: %s. Discarding received data.", exn
                )
                self._pending_data = b""
                break

    def tlv_received(self, tlv):
        """ Called when a valid TLV is received. To be overridden. """

    def emit_tlv(self, tlv):
        """ Called to emit a TLV to the other end. """
        self.transport.write(tlv.to_bytes())

    def eof_received(self):
        logger.debug("[%d] EOF received, closing connection.", self._debug_id)
        return super().eof_received()
