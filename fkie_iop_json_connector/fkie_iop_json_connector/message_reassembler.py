import time
import logging

from fkie_iop_json_connector.logger import MyLogger
from fkie_iop_json_connector.message import Message


class MessageReassembler:
    def __init__(self, *, timeout=5.0, loglevel='info'):
        self.logger = MyLogger('MessageReassembler', loglevel=loglevel)
        self._streams = {}
        self._timeout = timeout

    def _make_partial_key(self, msg):
        return (msg.src_id.value, msg.dst_id.value)

    def _make_full_key(self, msg, start_seq):
        return (msg.src_id.value, msg.dst_id.value, start_seq)

    def process(self, messages):
        result = []
        now = time.time()

        self._cleanup(now)

        for msg in messages:
            if msg.data_flags == Message.DF_SINGLE:
                result.append(msg)
                continue

            if msg.data_flags == Message.DF_FIRST:
                key = self._make_full_key(msg, msg.seqnr)

                # vorhandenen Stream mit gleichem key verwerfen
                if key in self._streams:
                    self.logger.warning(f"Drop existing stream (new FIRST): {key}")

                self._streams[key] = {
                    "start_seq": msg.seqnr,
                    "fragments": {msg.seqnr: msg.payload},
                    "first_seq": msg.seqnr,
                    "last_seq": None,
                    "created": now,
                    "last_update": now,
                    "meta": msg
                }
                continue

            # MIDDLE oder LAST
            partial_key = self._make_partial_key(msg)
            stream = self._find_stream(partial_key, msg.seqnr)

            if stream is None:
                self.logger.warning(f"Orphan fragment dropped seq={msg.seqnr}")
                continue

            stream["fragments"][msg.seqnr] = msg.payload
            stream["last_update"] = now

            if msg.data_flags == Message.DF_LAST:
                stream["last_seq"] = msg.seqnr

            if self._is_complete(stream):
                assembled = self._assemble(stream)
                result.append(assembled)

                key = (assembled.src_id.value,
                       assembled.dst_id.value,
                       stream["start_seq"])
                del self._streams[key]

        return result

    def _find_stream(self, partial_key, seqnr):
        src, dst = partial_key

        for key, stream in self._streams.items():
            if key[0] != src or key[1] != dst:
                continue

            if seqnr >= stream["start_seq"]:
                return stream

        return None

    def _is_complete(self, stream):
        if stream["last_seq"] is None:
            return False

        expected = range(stream["first_seq"], stream["last_seq"] + 1)
        return all(seq in stream["fragments"] for seq in expected)

    def _assemble(self, stream):
        msg = stream["meta"]
        # reset payload!
        msg.payload = b''

        for seq in sorted(stream["fragments"].keys()):
            msg.appendPayload(stream["fragments"][seq])

        msg.data_flags = Message.DF_SINGLE
        return msg

    def _cleanup(self, now):
        to_delete = []

        for key, stream in self._streams.items():
            if now - stream["last_update"] > self._timeout:
                self.logger.warning(f"Stream timeout dropped: {key}")
                to_delete.append(key)

        for key in to_delete:
            del self._streams[key]

    # def _create_base_message(self, first_msg):
    #     new_msg = Message(msg_id=first_msg.msg_id, version=first_msg.version)

    #     new_msg.src_id = first_msg.src_id
    #     new_msg.dst_id = first_msg.dst_id
    #     new_msg.priority = first_msg.priority
    #     new_msg.bcast = first_msg.bcast
    #     new_msg.acknak = first_msg.acknak
    #     new_msg.cmd_code = first_msg.cmd_code
    #     new_msg.message_type = first_msg.message_type

    #     new_msg.payload = b''
    #     new_msg.data_flags = Message.DF_SINGLE
    #     new_msg.seqnr = first_msg.seqnr

    #     return new_msg